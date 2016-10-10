
from stratum.pubsub import Pubsub, Subscription
from stratum.custom_exceptions import PubsubException
from mining.interfaces import Interfaces
from mining.subscription import MiningSubscription

import poolsrv.logger
log = poolsrv.logger.get_logger()

class DifficultySubscription(Subscription):
    event = 'mining.set_difficulty'

    def __init__(self):
        Subscription.__init__(self)
        self.emission_planned = False

    def recalculate_difficulty(self, worker):
        '''This is called when some worker changed its suggested difficulty'''
        session = self.get_session()

        Interfaces.share_limiter.apply_workers_difficulty(session, worker)

        if Interfaces.share_limiter.is_new_difficulty_requested(session):
            # Emit new difficulty before sending new job
            self.plan_single_emission()


    def process(self, *args, **kwargs):
        session = self.get_session()
        connection = self.connection_ref()

        # Reset flag for planned emission, we're just processing it
        self.emission_planned = False

        # Is there something to be sent here?
        if not Interfaces.share_limiter.is_new_difficulty_requested(session):
            log.warning("Difficulty change is not requested for %s" %
                        (connection.get_ident(),))
            return

        # Send difficulty update to the client - the planned new difficulty is sent
        Interfaces.share_limiter.send_difficulty_update(session, connection)

        # Prepare job facts
        broadcast_args = Interfaces.template_registry.get_last_template_broadcast_args()

        (job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, _) = broadcast_args

        # Push new job to the connection with clean_jobs = True!
        connection.rpc('mining.notify', ("%x"%job_id, prevhash, coinb1, coinb2,
                merkle_branch, version, nbits, ntime, True), is_notification=True)

        # Don't accept any previous job_id anymore - by setting the lower limit
        # for acceptable job_id
        session['min_job_id'] = job_id

        # Notify the share limiter that the last sent difficulty is now active
        Interfaces.share_limiter.on_new_job_sent(session)

        return None


    def plan_single_emission(self):
        '''
        Plans emission of a difficulty update, but it doesn't do it when
        some previous emission has been requested and not processed yet.
        '''
        if not self.emission_planned:
            # Remember the planned emission for future checks
            self.emission_planned = True
            MiningSubscription.before_broadcast.addCallback(self.emit_single)


    @classmethod
    def emit_single_before_mining_notify(self, connection):
        try:
            # Get the related subscription if there is some already
            diff_subs = Pubsub.get_subscription(connection, DifficultySubscription.event)

            # Plan the emission
            diff_subs.plan_single_emission()

        except PubsubException:
            # Connection is not subscribed for mining yet
            pass
