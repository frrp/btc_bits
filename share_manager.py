import datetime
import math
import time
import traceback

from twisted.internet import defer, reactor
from poolsrv import config
from poolsrv.posix_time import posix_time, posix_secs

from lib.bitcoin_rpc import BitcoinRPC
from mining.interfaces import Interfaces
from iface.share_backends import DbShareBackend, SinkShareBackend

import dbquery

import poolsrv.logger
log = poolsrv.logger.get_logger()


class PushWatchdog(object):
    def __init__(self, on_push_shares):
        self._on_push_shares = on_push_shares
        reactor.callLater(3, self.run)

    def run(self):
        try:
            self._on_push_shares()
        finally:
            reactor.callLater(3, self.run)


class ShareManager(object):
    def __init__(self):
        # Fire deferred when manager is ready
        self.on_load = defer.Deferred()

        self._last_worker_session_id = 0

        self._watchdog = PushWatchdog(self.push_shares)

        self._bitcoin_rpc = BitcoinRPC(config.BITCOIN_TRUSTED_HOST,
                                       config.BITCOIN_TRUSTED_PORT,
                                       config.BITCOIN_TRUSTED_USER,
                                       config.BITCOIN_TRUSTED_PASSWORD)

        self._db_share_backend = DbShareBackend()
        self._sink_share_backend = SinkShareBackend()

        self.load_block_info()

    def get_db_share_backend(self):
        return self._db_share_backend

    def on_network_block(self, prevhash):
        """Prints when there's new block coming from the network (possibly new round)"""
        # Refresh info about current round (another instance mined block?)
        self.load_block_info()

    @defer.inlineCallbacks
    def load_block_info(self, _=None):
        # Load current block_id from db
        (block_id, date_started) = (yield dbquery.get_block_info())

        block_started_secs = int(time.mktime(date_started.utctimetuple()))
        self._db_share_backend.set_block_info(block_id, block_started_secs)

        # We're ready!
        if not self.on_load.called:
            self.on_load.callback(True)

    @staticmethod
    def initialize_session(session):
        if not session.get('SM_worker_stats'):
            session['SM_worker_stats'] = {}

    def initialize_worker_stats(self, session, worker_id, worker_name):
        now = posix_secs()

        self._last_worker_session_id += 1

        stats = {
            'session_id': session['session_id'],
            'worker_id': worker_id,
            'worker_name': worker_name,
            'worker_session_id': self._last_worker_session_id,
            'authorized_at': now,
            'stale_submits': 0,
            'old_submits': 0,
            'invalid_submits': 0,
            'valid_submits': 0,
            'valid_shares': 0,
            'last_valid_share': None,
            'wsma_prev_rate_short': None,
            'wsma_prev_rate_long': None,
            'wsma_start_time_short': now,
            'wsma_start_time_long': now,
            'wsma_shares_short': 0,
            'wsma_shares_long': 0,
            'wsma_rate_short': 0.,
            'wsma_rate_long': 0.
        }
        session['SM_worker_stats'][worker_name] = stats
        return stats

    @staticmethod
    def get_worker_stats(session, worker_name):
        return session['SM_worker_stats'][worker_name]

    @staticmethod
    def _compute_wsma(worker_stats, shares, submit_time, time_period,
                      shares_key, start_time_key, prev_rate_key):

        worker_stats[shares_key] += shares
        curr_window = submit_time - worker_stats[start_time_key] + 1

        prev_rate = worker_stats[prev_rate_key]
        # First period for the miner
        if not prev_rate:
            prev_rate = 0
            prev_window = 0
        # Any next period when some previous exists
        else:
            prev_window = time_period - curr_window
            if prev_window < 0:
                prev_window = 0

        # Calculate the new partly from the previous window and partly form the
        # currently growing one
        new_rate = float((prev_rate * prev_window) + worker_stats[shares_key]) / \
            (prev_window + curr_window)

        # Did we finished a complete computation window?
        if curr_window > time_period:
            worker_stats[shares_key] = shares
            worker_stats[start_time_key] = submit_time
            worker_stats[prev_rate_key] = new_rate

        return new_rate

    def _update_stats_by_submit(self, worker_stats, shares, submit_secs):
        """
        When shares = 0 then we don't count it as a valid shares submission, only
        rate stats are updated.
        """

        worker_stats['wsma_rate_short'] = \
            self._compute_wsma(worker_stats, shares, submit_secs, config.SHARE_MANAGER_SHORT_WSMA_PERIOD_S,
                               'wsma_shares_short', 'wsma_start_time_short', 'wsma_prev_rate_short')

        worker_stats['wsma_rate_long'] = \
            self._compute_wsma(worker_stats, shares, submit_secs, config.SHARE_MANAGER_LONG_WSMA_PERIOD_S,
                               'wsma_shares_long', 'wsma_start_time_long', 'wsma_prev_rate_long')

        if shares > 0:
            worker_stats['valid_submits'] += 1
            worker_stats['valid_shares'] += shares
            worker_stats['last_valid_share'] = submit_secs

        # Let the reporter know that some worker statistics have changed and it
        # should be reported
        Interfaces.reporter.worker_stats_changed(worker_stats)

    def update_stats_by_no_submit(self, session):
        now = posix_time()

        for worker_stats in session['SM_worker_stats'].itervalues():
            # Only update the stats when there was already at least one valid share
            # or some other fact could change
            if worker_stats['last_valid_share'] or \
                    worker_stats['invalid_submits'] > 0 or \
                    worker_stats['stale_submits'] > 0:
                self._update_stats_by_submit(worker_stats, 0, now)

    @staticmethod
    def get_session_short_wsma(session):
        result = 0
        for worker_stats in session['SM_worker_stats'].itervalues():
            result += worker_stats['wsma_rate_short']
        return result

    def push_shares(self):
        """Is called by PushWatchdog when we need
        to push buffered shares to db"""

        secs = posix_secs()

        self._db_share_backend.push_shares(secs)
        self._sink_share_backend.push_shares(secs)

        # Sometimes notification is faster than
        # database update (because of old pool?)
        self.load_block_info()

    @staticmethod
    def on_invalid_submit_share(session, worker_stats, shares, job_id):
        if config.LOG_INVALID_SHARE_SUBMIT:
            log.info("%x INVALID %d %s %s" % (job_id, shares, session['client_ip'],
                                              worker_stats['worker_name']))

        worker_stats['invalid_submits'] += 1
        Interfaces.reporter.worker_stats_changed(worker_stats)

    def on_submit_share(self, session, worker_stats, shares, submit_secs):

        #if config.LOG_VALID_SHARE_SUBMIT or not is_valid:
        #    log.info("%s %x %s %d %s %s" % (block_hash, job_id, 'valid' if is_valid else 'INVALID',
        #                                    shares, session['client_ip'], worker_stats['worker_name']))

        self._update_stats_by_submit(worker_stats, shares, submit_secs)

        self._db_share_backend.on_submit_share(session, worker_stats, shares, submit_secs)

        self._sink_share_backend.on_submit_share(session, worker_stats, shares, submit_secs)

    @defer.inlineCallbacks
    def on_submit_block(self, is_accepted, worker_stats, block_header, block_hash, submit_secs, value):
        log.info("BLOCK %s %s !!!" % (block_hash, 'ACCEPTED' if is_accepted else 'REJECTED'))

        if not is_accepted:
            if not config.ACCEPT_INVALID_BLOCK__NONCE:
                return
            else:
                log.error("Rejected block saved to DB %s" % block_hash)

        try:
            info = (yield self._bitcoin_rpc.getinfo())
            block_height = info['blocks']  # FIXME
            difficulty = math.floor(float(info['difficulty']))
        except:
            log.error("Exception during getting block info after block submission: %s" %
                      traceback.format_exc())
            block_height = -1
            difficulty = -1

        self._db_share_backend.on_submit_block(block_hash, block_height, difficulty, submit_secs, value, worker_stats)

        self._sink_share_backend.on_submit_block(block_hash, block_height, difficulty, submit_secs, value, worker_stats)

        self.load_block_info()

