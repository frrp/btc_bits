import random
import math
import traceback

from mining.interfaces import Interfaces

from poolsrv import config
from poolsrv.posix_time import posix_time, posix_secs
from poolsrv import logger

from twisted.internet import reactor

log = logger.get_logger()


class Simulator(object):

    _SESSION = {'session_id': -1}

    def __init__(self, filename, filter=None):
        file = open(filename, "r")
        lines = file.readlines()
        workers = []
        for w in self._parse_workers(self._get_clean_lines(lines)):
            if not filter or filter(w):
                workers.append(w)

        self._workers = workers
        self._backend = None

        log.info(self._workers[:10])

    @staticmethod
    def _get_clean_lines(lines):
        # If true, we're in the block comment section
        in_block_comment = False

        for line in lines:
            # Remove line comments
            line = line.split('#', 1)[0].split('//', 1)[0].strip()

            # Skip empty lines
            if not len(line):
                continue

            # Handle block comments
            if line == '/*':
                in_block_comment = True
                continue
            if line == '*/':
                in_block_comment = False
                continue
            if in_block_comment:
                continue

            yield line

    def _parse_workers(self, lines):
        workers = []
        for line in lines:
            try:
                parts = line.split(':', 3)
                w = {
                    'rank': int(parts[0]),
                    'worker_id': long(parts[1]),
                    'ghps': float(parts[2]),
                    'deterministic': parts[3].lower() == 'd'
                }
                w['difficulty'] = max(1, int(0.5 + w['ghps'] / (config.LIMITER_TARGET_SUBMISSION_RATE * 4.294967296)))
                workers.append(w)
            except:
                log.error(traceback.format_exc())
                continue
        return workers

    @staticmethod
    def get_next_share_wait_time(speed_ghs, difficulty, deterministic):
        wait_time = difficulty * 4.294967296 / speed_ghs

        if not deterministic:
            luck = random.random()
            # Inverse function for density function for exponential distribution
            # (which is a probability distribution of time between events in Poisson process)
            wait_time *= math.log(1/(1 - luck))

        return wait_time

    def start_simulation(self):
        self._backend = Interfaces.share_manager.get_db_share_backend()
        now_secs = posix_secs()
        for w in self._workers:
            w['stats'] = {
                'worker_id': w['worker_id'],
                'worker_session_id': -w['rank']
            }
            w['next_share_at'] = now_secs

        self._simulation_step(first=True)
        log.info('Simulation started')

    def _simulation_step(self, first=False):
        try:
            now = posix_time()
            now_secs = int(now)
            for w in self._workers:
                if w['next_share_at'] <= now:
                    next_share_at = w['next_share_at'] +\
                                    self.get_next_share_wait_time(w['ghps'],
                                                                  w['difficulty'],
                                                                  w['deterministic'])
                    w['next_share_at'] = max(now, next_share_at)
                    if not first:
                        self._backend.on_submit_share(self._SESSION,
                                                      w['stats'],
                                                      w['difficulty'],
                                                      now_secs)
        except:
            log.error(traceback.format_exc())

        self._plan_simulation_step()

    def _plan_simulation_step(self):
        reactor.callLater(0.5, self._simulation_step)



