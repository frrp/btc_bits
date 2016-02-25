from stratum.pubsub import Pubsub, Subscription
from poolsrv import config
from poolsrv.posix_time import posix_time
from mining.interfaces import Interfaces

from twisted.internet import defer
import poolsrv.logger
log = poolsrv.logger.get_logger()


class MiningSubscription(Subscription):
    '''This subscription object implements
    logic for broadcasting new jobs to the clients.'''

    event = 'mining.notify'

    before_broadcast = defer.Deferred()


    def process(self, *args, **kwargs):
        ''' By default emission is disabled until when explicitly enabled'''
        return None


    def standard_process(self, *args, **kwargs):
        # Do nothing special, just return the args since they are already prepared for sending
        return args


    def enable_emission(self):
        # Re-plan processing invocation directly to standard_process for the next
        # emission
        self.process = self.standard_process


    @classmethod
    def on_template(cls, is_new_block):
        '''This is called when TemplateRegistry registers
           new block which we have to broadcast clients.'''

        start = posix_time()

        clean_jobs = is_new_block
        (job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, _) = \
                        Interfaces.template_registry.get_last_template_broadcast_args()

        if not is_new_block:
            try:
                cls.before_broadcast.callback(True)
                cls.before_broadcast = defer.Deferred()
            except:
                log.exception("before_broadcast callback failed!")

        # Push new job to subscribed clients
        cls.emit("%x"%job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs)

        cnt = Pubsub.get_subscription_count(cls.event)
        log.info("BROADCASTED to %d connections in %.03f sec" % (cnt, (posix_time() - start)))


    def after_subscribe(self, *args):
        '''
        Sends initial difficulty and a new job to newly subscribed client.
        It enables mining.notify emission just before sending the first job to the client,
        so that any concurrently broadcasted job is not sent.
        '''
        session = self.get_session()
        connection = self.connection_ref()

        # The first message received by the client after subscription is
        # difficulty setup. Therefore it is active from the first job.
        Interfaces.share_limiter.send_difficulty_update(session, connection)

        try:
            (job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, _) = \
                        Interfaces.template_registry.get_last_template_broadcast_args()
        except Exception:
            log.error("Template not ready yet")
            return

        # Don't allow to reuse any previous jobs, prevents 'postpone-better-hash' cheating
        session['min_job_id'] = job_id

        # Enable emission of mining.notify event so that we can call emit_single afterwards
        self.enable_emission()

        # Force client to remove previous jobs if any (e.g. from previous connection) ..
        clean_jobs = True
        # .. and send first job to the client
        self.emit_single("%x"%job_id, prevhash, coinb1, coinb2, merkle_branch,
                         version, nbits, ntime, clean_jobs)

        # Notify the share limiter that new job was sent so that the newly set difficulty is active.
        Interfaces.share_limiter.on_new_job_sent(session)
