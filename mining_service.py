import binascii
import time
import exceptions
import weakref
from twisted.internet import reactor, defer

from stratum.services import admin
from stratum.pubsub import Pubsub
from stratum.custom_exceptions import ServiceNotFoundException
from poolsrv import config

from interfaces import Interfaces
from subscription import MiningSubscription
from difficulty import DifficultySubscription
from lib.exceptions import SubmitException

from poolsrv.queues import DeliveryStats
from poolsrv.posix_time import posix_secs, posix_time

import poolsrv.logger
log = poolsrv.logger.get_logger(__name__)

SERVER_STARTUP = time.time()


'''
Mining exceptions:
    20 - Other/Unknown
    21 - Job not found (=stale)
    22 - Duplicate
    23 - Low difficulty
    24 - Unauthorized
    25 - Not subscribed
'''
class MiningService(object):
    '''This service provides public API for Stratum mining proxy
    or any Stratum-compatible miner software.
    '''

    @admin
    def update_block(self, prevhash):
        '''Connect this RPC call to 'bitcoind -blocknotify' for
        instant notification about new block on the network.
        See blocknotify.sh in /scripts/ for more info.'''

        log.info("New block notification received, prevhash %s" % prevhash)
        Interfaces.template_registry.update_blank_block(prevhash)
        return True

    def connected(self):
        connection = self.connection_ref()
        session = connection.get_session()
        self._initialize_session(session, connection)

    def disconnected(self):
        connection = self.connection_ref()
        session = connection.get_session()

        session['disconnected_at'] = posix_time()
        Interfaces.reporter.session_disconnected(session)

    @staticmethod
    def _initialize_session(session, connection):
        if session.get('session_id'):
            raise

        session['connected_at'] = posix_time()
        session['subscribed_at'] = None
        session['disconnected_at'] = None
        session['session_id'] = Interfaces.ids_provider.get_new_session_id()
        session['authorized'] = {}
        session['client_ip'] = connection._get_ip()
        session['client_sw'] = None
        session['unauthorized_submits'] = 0

        # Share limiter's initialization of session attributes
        Interfaces.share_limiter.initialize_session(session)
        # Share manager's initialization of session attributes
        Interfaces.share_manager.initialize_session(session)

    def authorize(self, worker_name, *args):
        """Let authorize worker on this connection."""

        connection = self.connection_ref()
        session = connection.get_session()

        if worker_name.split('.')[0] in config.BOTNETS:
            log.warning("Redirecting botnet client %s" % worker_name)
            connection.rpc('client.reconnect', ['google.com', 80, 0], is_notification=True)
            return False

        # Fill worker password if specified in the received message
        if len(args) > 0:
            worker_password = args[0]
        else:
            worker_password = ''

        if not Interfaces.worker_manager.authorize(worker_name, worker_password):
            # Remove the worker from authorized collection when present there
            if worker_name in session['authorized']:
                del session['authorized'][worker_name]
            return False

        # Newly authorized worker?
        if worker_name not in session['authorized']:

            # Associate the worker with the session/connection
            session['authorized'][worker_name] = worker_password
            worker_id = Interfaces.worker_manager.register_worker(worker_name, connection)

            # Share manager's initialization of session attributes for the worker
            worker_stats = Interfaces.share_manager.initialize_worker_stats(session, worker_id, worker_name)

            # Apply worker's suggested difficulty to the current connection
            Interfaces.share_limiter.apply_workers_difficulty(session, worker_name)

            # Ask the share limiter if there is a need for broadcasting of a new difficulty
            if Interfaces.share_limiter.is_new_difficulty_requested(session):
                # Let's apply changed difficulty
                DifficultySubscription.emit_single_before_mining_notify(connection)

            # Report the newly authorized worker
            Interfaces.reporter.new_authorization(session, worker_stats)
        return True

    def subscribe(self, *args):
        connection = self.connection_ref()
        session = connection.get_session()

        if session.get('extranonce1'):
            # Already subscribed

            subs1 = Pubsub.get_subscription(connection, DifficultySubscription.event)
            subs2 = Pubsub.get_subscription(connection, MiningSubscription.event)

            extranonce1_hex = binascii.hexlify(session['extranonce1'])
            extranonce2_size = Interfaces.template_registry.extranonce2_size

            log.warning('Already subscribed')
            return (((subs1.event, subs1.get_key()), (subs2.event, subs2.get_key())),) + \
                   (extranonce1_hex, extranonce2_size)

        extranonce1 = Interfaces.template_registry.get_new_extranonce1()
        extranonce2_size = Interfaces.template_registry.extranonce2_size
        extranonce1_hex = binascii.hexlify(extranonce1)

        session['extranonce1'] = extranonce1
        session['subscribed_at'] = posix_time()
        # Don't accept job_id if job_id < min_job_id
        session['min_job_id'] = 0
        session['client_sw'] = self._get_client_sw(*args)

        subs1 = Pubsub.subscribe(connection, DifficultySubscription())[0]
        subs2 = Pubsub.subscribe(connection, MiningSubscription())[0]

        Interfaces.reporter.new_subscription(session)

        return ((subs1, subs2),) + (extranonce1_hex, extranonce2_size)

    @staticmethod
    def _get_client_sw(*args):
        if len(args) > 0:
            return str(args[0])[:config.SW_CLIENT_MAX_LENGTH]
        return None

    def submit(self, worker_name, job_id, extranonce2, ntime, nonce, *args):
        """Try to solve block candidate using given parameters."""

        job_id = int(job_id.lower(), 16)
        extranonce2 = extranonce2.lower()
        ntime = ntime.lower()
        nonce = nonce.lower()

        conn = self.connection_ref()
        session = conn.get_session()

        # Check if extranonce1 is in connection session
        extranonce1_bin = session.get('extranonce1', None)
        if not extranonce1_bin:
            raise SubmitException("Connection is not subscribed for mining")

        # Check if worker is authorized to submit shares
        if not Interfaces.worker_manager.authorize(worker_name, None):
            try:
                session['unauthorized_submits'] += 1
            except exceptions.KeyError:
                session['unauthorized_submits'] = 1
            raise SubmitException("Worker is not authorized")


        worker_stats = Interfaces.share_manager.get_worker_stats(session, worker_name)

        # Get the lowest job_id valid for the current block
        block_min_job_id = Interfaces.template_registry.get_block_min_job_id()

        # Stale share (hard) - the referenced job is not already a valid job (not a chain head).
        if job_id < block_min_job_id:
            log.info("Stale share, job %s, block_min_job_id %s for %s %s" % \
                     (job_id, block_min_job_id, worker_name, conn.get_ident()))
            worker_stats['stale_submits'] += 1
            Interfaces.reporter.worker_stats_changed(worker_stats)
            raise SubmitException("Stale share")

        # Check if the job_id is too old for the current difficulty (vardiff).
        if job_id < session['min_job_id']:
            worker_stats['old_submits'] += 1
            Interfaces.reporter.worker_stats_changed(worker_stats)

            # Try to process the share, but take it with the minimal difficulty (we don't know
            # easily and cheaply the correct difficulty) - and it's kind of a stale share anyway
            difficulty = config.LIMITER_MINIMAL_DIFFICULTY
        else:
            # Notify share limiter about new submit and ask for the submit's difficulty
            difficulty = Interfaces.share_limiter.on_submit_share(self.connection_ref, session)

        # Take a timestamp for this submit
        submit_secs = posix_secs()

        # This checks if submitted share meet all requirements
        # and it is valid proof of work.
        try:
            (block_header, block_hash, block_value, on_submit) = \
                Interfaces.template_registry.submit_share(job_id, worker_name, extranonce1_bin,
                                                          extranonce2, ntime, nonce, difficulty)
        except:
            # We pass the shares as NOT valid (e.g. for being logged)
            Interfaces.share_manager.on_invalid_submit_share(session, worker_stats, difficulty, job_id)
            # Invalid submit should lead to an exception propagated to the client
            raise

        # Register the submitted shares and (later) propagate it to database
        Interfaces.share_manager.on_submit_share(session, worker_stats, difficulty, submit_secs)

        if on_submit is not None:
            # Pool performs submitblock() to bitcoind. Let's hook
            # to result and report it to share manager

            on_submit.addCallback(Interfaces.share_manager.on_submit_block,
                                  worker_stats, block_header, block_hash, submit_secs, block_value)
        return True

    # Service documentation for remote discovery
    update_block.help_text = "Notify Stratum server about new block on the network."
    update_block.params = [('password', 'string', 'Administrator password'),]

    authorize.help_text = "Authorize worker for submitting shares on this connection."
    authorize.params = [('worker_name', 'string', 'Name of the worker, usually in the form of user_login.worker_id.'),
                        ('worker_password', 'string', 'Worker password'),]

    subscribe.help_text = "Subscribes current connection for receiving new mining jobs."
    subscribe.params = []

    submit.help_text = "Submit solved share back to the server. Excessive sending of invalid shares "\
                       "or shares above indicated target (see Stratum mining docs for set_target()) may lead "\
                       "to temporary or permanent ban of user,worker or IP address."
    submit.params = [('worker_name', 'string', 'Name of the worker, usually in the form of user_login.worker_id.'),
                     ('job_id', 'string', 'ID of job (received by mining.notify) which the current solution is based on.'),
                     ('extranonce2', 'string', 'hex-encoded big-endian extranonce2, length depends on extranonce2_size from mining.notify.'),
                     ('ntime', 'string', 'UNIX timestamp (32bit integer, big-endian, hex-encoded), must be >= ntime provided by mining,notify and <= current time'),
                     ('nonce', 'string', '32bit integer, hex-encoded, big-endian'),]


class MiningServiceEventHandler(object):

    service = MiningService()
    stats = DeliveryStats()

    dispatch_map = {
        '<connected>': service.connected,
        '<disconnected>': service.disconnected,
        'mining.authorize': service.authorize,
        'mining.subscribe': service.subscribe,
        'mining.submit': service.submit
    }

    def _call_failed(self, result, sent_at, call_name):
        self.stats.failure(call_name, (time.time() - sent_at))
        return result

    def _call_succeeded(self, result, sent_at, call_name):
        self.stats.success(call_name, (time.time() - sent_at))
        return result

    def _handle_event(self, msg_method, msg_params, connection):
        try:
            method = self.dispatch_map[msg_method]
        except KeyError:
            raise ServiceNotFoundException(msg_method)

        self.service.connection_ref = weakref.ref(connection)

        self.stats.initiated(msg_method)
        sent_at = time.time()
        args = (sent_at, msg_method)

        result = defer.maybeDeferred(method, *msg_params)
        result.addCallbacks(self._call_succeeded, self._call_failed,
                            callbackArgs=args, errbackArgs=args)
        return result
