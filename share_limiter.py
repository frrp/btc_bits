import weakref
from stratum.connection_registry import ConnectionRegistry
from poolsrv import config
from poolsrv.posix_time import posix_time
from mining.interfaces import Interfaces
from twisted.internet import reactor
from scipy.stats import poisson
import poolsrv.logger
log = poolsrv.logger.get_logger()


class ShareLimiter(object):
    # See config.py for details about configuration options for the share limiter.
    # Computed dynamically by apply_shape_settings() operation

    _MIN_SUBMITS_IN_TIME = []
    _MAX_SUBMITS_IN_TIME = []
    # Length of a collection period so that when zero submission occurred
    # in this period then we know for sure we need to decrease the difficulty
    # (derived from LIMITER_MIN_SUBMITS_PERCENTILE)
    _NO_SHARE_RECALCULATION_PERIOD = None
    _HALF_COLLECTION_PERIOD = None
    _PERIOD_WITH_COMPUTED_LIMITS = None
    _FINE_TUNE_UPPER_RATE = None
    _FINE_TUNE_LOWER_RATE = None

    def __init__(self, plan_emission_callback, on_startup):

        self._no_submit_conn_refs = weakref.WeakKeyDictionary()

        self.plan_emission = plan_emission_callback
        self.apply_shape_settings()

        reactor.callLater(self._NO_SHARE_RECALCULATION_PERIOD, #@UndefinedVariable
                          self._process_no_submits)

        self._startup_time = None
        # Register handler for startup event
        on_startup.addCallback(self._server_started)

    def _server_started(self, *events):
        self._startup_time = posix_time()
        log.info("Server started")

    def apply_shape_settings(self):

        self._PERIOD_WITH_COMPUTED_LIMITS = int(config.LIMITER_FULL_COLLECTION_PERIOD_S * 1.2)

        self._MIN_SUBMITS_IN_TIME = \
            [poisson.ppf(config.LIMITER_MIN_SUBMITS_PERCENTILE,
                         i * config.LIMITER_TARGET_SUBMISSION_RATE * config.LIMITER_MIN_OK_SUBMITS_RATIO) \
                for i in range(self._PERIOD_WITH_COMPUTED_LIMITS + 1)]

        self._MAX_SUBMITS_IN_TIME = \
            [poisson.ppf(config.LIMITER_MAX_SUBMITS_PERCENTILE,
                         i * config.LIMITER_TARGET_SUBMISSION_RATE * config.LIMITER_MAX_OK_SUBMITS_RATIO) \
                for i in range(self._PERIOD_WITH_COMPUTED_LIMITS + 1)]

        # Find the first non-zero expected value of minimal submission count
        for i in range(1, config.LIMITER_FULL_COLLECTION_PERIOD_S):
            # At least one submit is expected in i-th second
            if self._MIN_SUBMITS_IN_TIME[i] >= 1:
                self._NO_SHARE_RECALCULATION_PERIOD = i
                break
        # If we didn't find the right spot, full collection period is used
        # (should never happen)
        else:
            self._NO_SHARE_RECALCULATION_PERIOD = config.LIMITER_FULL_COLLECTION_PERIOD_S
            log.error("Non-zero minimal submissions not found")

        # Recalculate collection period
        self._HALF_COLLECTION_PERIOD = int(config.LIMITER_FULL_COLLECTION_PERIOD_S / 2)
        self._FINE_TUNE_UPPER_RATE = config.LIMITER_TARGET_SUBMISSION_RATE * \
                config.LIMITER_FINE_TUNE_UPPER_RATIO
        self._FINE_TUNE_LOWER_RATE = config.LIMITER_TARGET_SUBMISSION_RATE * \
                config.LIMITER_FINE_TUNE_LOWER_RATIO

    def initialize_session(self, session):
        if not session.get('SL_difficulty'):

            now = posix_time()
            # Initial difficulty for new connections - it depends on the server's state.
            # When the server is accepting connections shorter then a configured time
            # then special default difficulty is applied to protect the server from
            # extreme submission rate from miners with not properly set difficulty.
            if self._startup_time and \
                    now > self._startup_time + config.LIMITER_STARTUP_PERIOD_S:
                session['SL_difficulty'] = config.LIMITER_DEFAULT_DIFFICULTY
            else:
                session['SL_difficulty'] = config.LIMITER_DEFAULT_DIFFICULTY_STARTUP
            session['SL_difficulty_set_at'] = now

            session['SL_requested_difficulty'] = None
            session['SL_last_sent_difficulty'] = None

            # Statistical information
            session['SL_changes_up'] = 0
            session['SL_changes_down'] = 0

            self._reset_submission_collection(session)

    @staticmethod
    def _reset_submission_collection(session):
        now = posix_time()

        # Number of submits collected during the current full collection period
        session['SL_submits'] = 0
        session['SL_first_part_submits'] = 0

        session['SL_start_time'] = now
        session['SL_first_part_end_time'] = now

        session['SL_last_recalculation_time'] = now
        session['SL_submission_rate'] = None

    @staticmethod
    def is_new_difficulty_requested(session):
        return session['SL_requested_difficulty'] != None

    @staticmethod
    def apply_workers_difficulty(session, worker_name):
        suggested = Interfaces.worker_manager.get_worker_difficulty(worker_name)
        current = max(session.get('SL_requested_difficulty'), session['SL_difficulty'])
        if suggested > current:
            log.info("Applied suggested difficulty %d for %s:%d (%s)" % \
                     (suggested, session['client_ip'], session['session_id'], worker_name))
            new_requested = max(suggested, session['SL_requested_difficulty'])
            session['SL_requested_difficulty'] = new_requested

    @staticmethod
    def send_difficulty_update(session, connection):
        # Take the most recent difficulty available, requested difficulty can be float
        difficulty = int(session['SL_requested_difficulty'] or session['SL_difficulty'])

        # Send the new difficulty to the miner(s), even when it is the same as the previous
        connection.rpc('mining.set_difficulty', [difficulty,], is_notification=True)

        # Remember the last sent difficulty
        session['SL_last_sent_difficulty'] = difficulty

    def on_new_job_sent(self, session):
        """
        From now on a new difficulty is valid if some has been sent. So the last
        sent difficulty is now the valid one.
        """

        # Take the last sent difficulty to the connection
        sent = session['SL_last_sent_difficulty']
        if not sent:
            log.error("Difficulty not sent before a job!!")
            # Expect minimal difficulty for such case
            sent = config.LIMITER_MINIMAL_DIFFICULTY

        current = session['SL_difficulty']
        request = session['SL_requested_difficulty']
        # The requested_difficulty is being forgotten and accepted as a new value of difficulty
        if request and int(request) == int(sent):
            session['SL_requested_difficulty'] = None

        # By sending the job, the actual difficulty has changed to the last sent
        if sent != current:
            self._change_difficulty(session, sent)
            log.info("Difficulty changed [%d -> %d] for %s:%d (%s)" % \
                 (current, sent, session['client_ip'], session['session_id'],
                  ",".join(session['authorized'].keys())))

    def _change_difficulty(self, session, new_difficulty):
        org = session['SL_difficulty']

        if new_difficulty > org:
            session['SL_changes_up'] += 1
        if new_difficulty < org:
            session['SL_changes_down'] += 1

        session['SL_difficulty'] = new_difficulty
        session['SL_difficulty_set_at'] = posix_time()

        # Report the changed difficulty
        Interfaces.reporter.difficulty_changed(session)

        # We need to restart submission collection to not mix shares with different
        # difficulty
        self._reset_submission_collection(session)

    def on_submit_share(self, connection_ref, session):
        now = posix_time()

        # Remember the submit
        session['SL_submits'] += 1

        # If we reached planned recalculation time
        if now >= session['SL_last_recalculation_time'] + config.LIMITER_RECALCULATION_PERIOD_S:
            connection = connection_ref()
            if connection:
                self._recalculate(connection, session)
                # Remove the current connection from collection of conns. without submit
                try:
                    del self._no_submit_conn_refs[connection]
                except:
                    pass

        # Return difficulty of the just submitted shares
        return session['SL_difficulty']

    def _recalculate(self, connection, session):
        now = posix_time()
        result = False

        # Short period features
        submits = session['SL_submits']
        part_submits = session['SL_first_part_submits']
        secs = now - session['SL_start_time']
        part_secs = now - session['SL_first_part_end_time']

        submission_rate = float(submits) / secs
        secs = int(secs + 0.5)

        session['SL_last_recalculation_time'] = now
        session['SL_submission_rate'] = submission_rate

        # We completed half of the full collection period so we should restart it
        if part_secs >= self._HALF_COLLECTION_PERIOD:
            part_submits = submits - part_submits
            session['SL_submits'] =  part_submits
            session['SL_first_part_submits'] = part_submits
            session['SL_start_time'] = session['SL_first_part_end_time']
            session['SL_first_part_end_time'] = now

        # A protection against index out of bounds (when a submit didn't arrived
        # for quite long time and we're over the pre-computed period)
        if secs > self._PERIOD_WITH_COMPUTED_LIMITS:
            # Late submit means that the current submission rate is really low
            # and it is most probable that the current difficulty is already 1
            # and session workers are too weak. We then linearly approximate
            # submit count.
            submits = submits * self._PERIOD_WITH_COMPUTED_LIMITS / float(secs)
            secs = self._PERIOD_WITH_COMPUTED_LIMITS

        # Number of submits exceeds an expected and probable count
        if submits > self._MAX_SUBMITS_IN_TIME[secs]:
            result = (self._request_difficulty_by_rate(submission_rate,
                                                       connection, session,
                                                       config.LIMITER_CHANGE_RATIO_UP)
                      or result)

        # Number is lower then an expected and probable count
        if submits < self._MIN_SUBMITS_IN_TIME[secs] \
                and session['SL_difficulty'] > config.LIMITER_MINIMAL_DIFFICULTY:
            result = (self._request_difficulty_by_rate(submission_rate,
                                                       connection, session,
                                                       config.LIMITER_CHANGE_RATIO_DOWN)
                      or result)

        # If the current difficulty is stable long time enough, try to fine tune the difficulty
        if (now - session['SL_difficulty_set_at']) >= config.LIMITER_FINE_TUNE_AFTER_S \
            and (submission_rate >= self._FINE_TUNE_UPPER_RATE \
                 or submission_rate <= self._FINE_TUNE_LOWER_RATE):

            # Get moving average for short time hash rate (roughly minutes) and compute
            # estimated submission rate over the period (related to the current difficulty)
            submission_rate = float(Interfaces.share_manager.get_session_short_wsma(session)) / \
                                    session['SL_difficulty']
            if submission_rate >= self._FINE_TUNE_UPPER_RATE or submission_rate <= self._FINE_TUNE_LOWER_RATE:
                result = (self._request_difficulty_by_rate(submission_rate,
                                                           connection, session, 1)
                          or result)

        # There is not a reason for changing difficulty from submissions and we are
        # far enough form the subscription. In such case we want to cancel the change request.
        # We block the cancellation right after subscription because of applied suggested
        # difficulty and slow workers startup (there can be significant delay.
        if session['SL_requested_difficulty'] and not result \
                and (now - session['subscribed_at']) >= config.LIMITER_BLOCK_REQUEST_CANCEL_PERIOD_S:

            session['SL_requested_difficulty'] = None
            log.info("request cancelled for %s:%d (%s)" % \
                     (session['client_ip'], session['session_id'],
                      ",".join(session['authorized'].keys())))

        return result

    def _request_difficulty_by_rate(self, rate, connection, session, change_ratio):

        current_difficulty = session['SL_difficulty']

        # Calculates the ideal non-integer difficulty
        ideal_difficulty = current_difficulty * rate / config.LIMITER_TARGET_SUBMISSION_RATE

        # Compute the request by given a change ratio from the current difficulty and ideal difficulty
        new_request = current_difficulty + ((ideal_difficulty - current_difficulty) * change_ratio)

        # We always truncate non-integer difficulty to lower values since it is significant
        # only for lower difficulties and there we prefer more accurate calculations
        # based on more submission then higher variance caused by higher difficulty.
        # On the other side, we do not permit higher difficulty the configured maximum.
        normalized = int(max(config.LIMITER_MINIMAL_DIFFICULTY,
                             min(config.LIMITER_MAXIMAL_DIFFICULTY,
                                 new_request)))

        # Get current request value
        current_request = session['SL_requested_difficulty'] or current_difficulty

        # Request new difficulty only when it is not already set or requested
        if normalized != current_request:
            log.info("request: %d [%d -> %d] (%.02f) rate %.02f for %s:%d (%s)" % \
                     (current_difficulty,current_request, normalized, ideal_difficulty,
                      rate, session['client_ip'], session['session_id'],
                      ",".join(session['authorized'].keys())))

            session['SL_requested_difficulty'] = normalized
            self.plan_emission(connection)

        # Return if the difficulty should change or not (regardless already requested change)
        return (normalized != current_difficulty)

    def _process_no_submits(self):
        # Plan the next execution
        reactor.callLater(self._NO_SHARE_RECALCULATION_PERIOD, #@UndefinedVariable
                          self._process_no_submits)

        # Statistics
        start = posix_time()
        total, changes = 0, 0

        for connection_ref in self._no_submit_conn_refs.iterkeyrefs():
            try:
                connection = connection_ref()
                session = connection.get_session()
                total += 1
            except:
                # Not connected
                continue

            # Skip not initialized sessions
            if not session.get('session_id'):
                continue

            # Decrease hash rate for all authorized workers
            Interfaces.share_manager.update_stats_by_no_submit(session)

            # Shortcut for the slowest miners, recalculation is not necessary
            if session['SL_difficulty'] == 1 and not session['SL_requested_difficulty']:
                continue

            # Difficulty recalculation should almost always lead to new difficulty here
            if self._recalculate(connection, session):
                changes += 1

        # Log results only when at least one connection has finished its short period
        if changes > 0:
            log.info("No submits processed in %.03fs, %d / %d (chng, total)" % \
                     (posix_time() - start, changes, total))

        # Get new shallow copy of all connections
        self._no_submit_conn_refs = ConnectionRegistry.get_shallow_copy()
