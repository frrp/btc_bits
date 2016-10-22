import datetime
import math

from twisted.internet import reactor
from poolsrv import config
from poolsrv.posix_time import posix_secs

from mining.interfaces import Interfaces

import dbquery

import poolsrv.logger
log = poolsrv.logger.get_logger()


class SinkShareBackend(object):

    def __init__(self):
        # Current share-sink's pushing period
        self._curr_push_period = posix_secs()
        # Share-sink current push set - data collected during current collection period
        self._push_set = {}
        self._last_nonempty_push_period = None

    @staticmethod
    def on_submit_block(block_hash, block_height, difficulty, submit_secs, value, worker_stats):

        # Store block information in the share database - transfer it to the share sink
        Interfaces.share_sink.push_block(worker_stats['worker_id'], submit_secs, block_hash,
                                         block_height, difficulty, value)

    def on_submit_share(self, session, worker_stats, shares, submit_secs):
        worker_session_id = worker_stats['worker_session_id']

        # Handle new-style share-sink-push
        # Have we already finished the push period?
        if submit_secs > self._curr_push_period:
            # Flush everything so far to the share sink
            self._push_finalized_sets(submit_secs)

        facts = self._push_set.get(worker_session_id)
        if facts:
            self._push_set[worker_session_id] = \
                (facts[0], facts[1], facts[2] + shares)
        else:
            self._push_set[worker_session_id] = \
                (session['session_id'], worker_stats['worker_id'], shares)

    def push_shares(self, secs):
        # Push even empty set to the sink regularly
        if secs > self._curr_push_period:
            self._push_finalized_sets(secs)

    def _push_finalized_sets(self, submit_secs):
        if len(self._push_set) > 0:
            # Plan to push the shares as soon as possible
            reactor.callLater(0, Interfaces.share_sink.push_shares,
                              self._curr_push_period, self._push_set.values())

            log.info("Pushed %d worker sessions to share sink" % len(self._push_set))

            if self._last_nonempty_push_period and \
                    self._curr_push_period > self._last_nonempty_push_period + 1:
                log.warning("Submission stream gap %ds" % (submit_secs - self._last_nonempty_push_period - 1))

            self._last_nonempty_push_period = self._curr_push_period

            # Prepare empty space for the next submits
            self._push_set = {}
        # We do it even when there are no shares to be pushed
        else:
            Interfaces.share_sink.push_shares(self._curr_push_period, self._push_set)

        # Reset the push structures
        self._curr_push_period = submit_secs


class DbShareBackend(object):

    def __init__(self):
        self._shares = {}
        self._shares_count = 0  # Counter of shares just for stats

        # Slot-push related members
        self._current_slot_num_to_push = 0
        self._slot_score = 0.0  # Not pushed score in the current slot

        self._current_second_facts = {
            'secs': posix_secs() - 1,
            'score': 1.,
        }

        self._block_id = None
        self._block_started_secs = None

    def set_block_info(self, block_id, block_started_secs):

        if block_id != self._block_id:
            log.info("New round #%s, started at %s" % (block_id,
                                                       datetime.datetime.fromtimestamp(block_started_secs)))
        self._block_id = block_id
        self._block_started_secs = block_started_secs

    def on_submit_block(self, block_hash, block_height, difficulty, submit_secs, value, worker_stats):
        date_found = datetime.datetime.fromtimestamp(submit_secs)

        # Create block record in the database
        dbquery.submit_block(self._block_id, worker_stats['worker_name'], date_found, block_hash, block_height, difficulty,
                             value)
        # Increase 'found blocks' for the user
        dbquery.increase_worker_blocks(worker_stats['worker_id'])

    def on_submit_share(self, session, worker_stats, shares, submit_secs):

        second_facts = self._get_facts_for_second(submit_secs)

        # If the current slot is no longer valid (its time interval is over)
        # we need to push all the currently collected shares within the old slot
        # before storing new shares.
        if self._current_slot_num_to_push != second_facts['slot_num']:
            # We push it to the previous slot (last second)
            self.push_shares(self._get_last_second_for_slot(self._current_slot_num_to_push))
            self._current_slot_num_to_push = second_facts['slot_num']

        # Score of the submitted shares withing the current slot
        submit_slot_score = second_facts['slot_score'] * shares

        self._shares_count += shares
        self._slot_score += submit_slot_score

        worker_session_id = worker_stats['worker_session_id']

        if worker_session_id in self._shares:
            share = self._shares[worker_session_id]
            share['shares'] += shares
            share['score'] += second_facts['score'] * shares
            share['slot_score'] += submit_slot_score
            share['last_share'] = second_facts['last_share']
        else:
            share = {'worker_id': worker_stats['worker_id'],
                     'block_id': self._block_id,
                     'slot_num': second_facts['slot_num'],
                     'shares': shares,
                     'score': shares * second_facts['score'],
                     'slot_score': submit_slot_score,
                     'last_share': second_facts['last_share'],
                     'session_id': session['session_id'],
                     'stratum': config.STRATUM_UNIQUE_ID}
            self._shares[worker_session_id] = share

    @staticmethod
    def _get_slot_num_for_secs(secs):
        return int(secs - (secs % config.PUSH_SLOT_SIZE))

    @staticmethod
    def _get_first_second_for_slot(slot_num):
        return slot_num

    @staticmethod
    def _get_last_second_for_slot(slot_num):
        return slot_num + config.PUSH_SLOT_SIZE - 1

    @staticmethod
    def _get_in_slot_position(secs):
        return secs % config.PUSH_SLOT_SIZE

    def _get_facts_for_second(self, submit_secs):
        if self._current_second_facts['secs'] == submit_secs:
            return self._current_second_facts

        last_share = datetime.datetime.fromtimestamp(submit_secs)

        # Seconds since beginning of current hour
        # (score method renormalizes every hour)
        hour_duration = last_share.minute * 60 + last_share.second

        # Slot number for the given second
        slot_num = self._get_slot_num_for_secs(submit_secs)
        # Share position in the slot
        slot_pos = self._get_in_slot_position(submit_secs)
        # Score of one share on slot_pos position
        slot_score = math.exp(slot_pos/float(config.SCORE_MAGIC))

        if hour_duration < 3590:
            if config.TEMPORARY__SCORE_BY_TIME_ONLY:
                duration = hour_duration
            else:
                # Avoid possible negatives when block has been found
                round_duration = max(0, submit_secs - self._block_started_secs)
                duration = min(round_duration, hour_duration)
            # Calculate score for the submission time
            score = math.exp(duration/float(config.SCORE_MAGIC))
        else:
            # Veeery ugly hack preventing race condition
            # on hourly renormalization. I'm sorry for
            # some discarded reward, but I'm working on PPS/DGM...
            score = 1

        self._current_second_facts = {
            'secs': submit_secs,
            'score': score,
            'slot_score': slot_score,
            'slot_num': slot_num,
            'last_share': last_share
        }
        return self._current_second_facts

    def push_shares(self, push_second):
        cnt = len(self._shares)
        if not cnt:
            # Nothing to store
            return

        if self._current_slot_num_to_push != self._get_slot_num_for_secs(push_second):
            push_second = self._get_last_second_for_slot(self._current_slot_num_to_push)

        shares_iterator = self._shares.itervalues()

        # This is not a number of submits, but aggregated count of
        # shares including their difficulty
        shares_count = self._shares_count
        slot_score = self._slot_score
        self._shares_count = 0
        self._slot_score = 0.0
        self._shares = {}

        log.info("Pushing %d updates, %d shares, %f" % (cnt, shares_count,
                                                        self._current_second_facts['score']))

        dbquery.submit_shares(shares_iterator)

        # Beginning of the current slot
        d = datetime.datetime.utcfromtimestamp(self._get_first_second_for_slot(self._current_slot_num_to_push))
        dbquery.submit_share_stats(d, push_second, shares_count, self._current_slot_num_to_push, slot_score, self._block_id)
