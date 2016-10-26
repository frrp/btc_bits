import time
import weakref
import traceback
from twisted.internet import reactor, defer
from stratum.pubsub import Pubsub
import worker_db
from mining.difficulty import DifficultySubscription
from poolsrv import config
import poolsrv.logger
log = poolsrv.logger.get_logger()


class WorkerManager(object):
    def __init__(self):
        self.workers = {}

        # Partial update is cheap, let's load new workers often
        self.update_interval = 60

        # Workers are tried to be removed from memory time to time. It
        # can take two periods to be actually removed.
        self.mark_and_sweep_interval = 60

        # Fire deferred when manager is ready
        self.on_load = defer.Deferred()

        # Create a new instance of local worker database connection
        self.worker_db = worker_db.WorkerDB(config.LOCAL_WORKER_DB_FILE)

        # Run regular task loading/refreshing the workers database
        self._run_refresh_database()

        # Run regular task cleaning memory cache for workers
        self._run_mark_and_sweep_workers()


    def get_worker_difficulty(self, worker_name):
        return self._get_worker(worker_name)['difficulty_suggested']


    def authorize(self, worker_name, worker_password):
        worker = self._get_worker(worker_name)
        return worker is not None


    def register_worker(self, worker_name, connection):
        worker = self._get_worker(worker_name)
        if not worker:
            return None

        # Mark the worker as active, see _run_mark_and_sweep_workers
        # for details about workers activity detection
        worker['is_active'] = True

        # Register which connection is using this worker
        worker['connections'][connection] = True

        return worker['worker_id']


    def _get_worker(self, worker_name):
        '''
        Returns information about the requested worker from memory
        cache (if present) or retrieves the facts from local worker
        database and place it to the cache.

        Returns None if the worker is unknown
        '''
        result = self.workers.get(worker_name)
        # Found in the cache
        if result:
            return result

        # We need to load the worker from the database
        facts = self.worker_db.get_worker_facts(worker_name)
        if not facts:
            log.info("Worker not found in the local database: '%s'" % worker_name)
            return None

        # Decompose the retrieved tuple (worker_name is omitted)
        worker_id, _, difficulty_suggested = facts

        # Create new worker's cache representation
        result = {
            'worker_id': worker_id,
            'difficulty_suggested': difficulty_suggested,
            # See _run_mark_and_sweep_workers for details about 'is_active'
            'is_active': True,
            'connections': weakref.WeakKeyDictionary()
            }

        # Remember the worker facts and return it
        self.workers[worker_name] = result
        return result


    def _on_worker_refresh(self, facts):
        '''
        Callback passed to WorkerDB's refresh method for being
        notified about refreshed workers. We update the memory worker
        data appropriately.

        See WorkerDB.refresh_from_source for details about passed
        'facts' dictionary.
        '''
        worker_name = facts['worker_name']

        # The worker is not in memory, there is nothing to be updated
        worker = self.workers.get(worker_name)
        if not worker:
            return

        # We should update the id and password unconditionally
        worker['worker_id'] = facts['worker_id']

        # Suggested difficulty has been changed ..
        if worker['difficulty_suggested'] != facts['difficulty_suggested']:
            # Assign the new suggested difficulty
            worker['difficulty_suggested'] = facts['difficulty_suggested']
            # .. so let's poke DifficultySubscription and let it
            # recalculate difficulties
            for conn in worker['connections'].keys():
                diff_sub = Pubsub.get_subscription(conn, DifficultySubscription.event)
                diff_sub.recalculate_difficulty(worker_name)


    def _run_mark_and_sweep_workers(self):
        '''
        Periodically checks which workers are not active anymore and
        removes them from the memory cache.

        When the worker connects, its cache representation is marked
        as active (it it also created in active state when loading
        from db).

        Time to time we go thru all workers and check if they have
        some connection associated. If not, we mark them as NOT
        active. But they stay in the cache for the next mark & sweep
        time period.

        If we find some already NOT active worker the next pass, we
        drop it's representation from the memory cache. It means, when
        there are no connections associated to the worker in two
        consecutive mark & sweep iteration and no connection was made
        in that time period, the worker is dropped.
        '''

        to_inactive = 0
        to_be_dropped = []
        try:
            # Iterate over all cached workers
            for worker_name, worker in self.workers.iteritems():
                # Skip all workers with some associated connection,
                # they are active (and already marked so)
                if len(worker['connections']) > 0:
                    continue

                # Make the worker inactive or drop it when it already
                # is inactive
                if worker['is_active']:
                    worker['is_active'] = False
                    to_inactive += 1
                else:
                    # Remember the worker to be dropped (cannot modify
                    # the just iterating dictionary)
                    to_be_dropped.append(worker_name)

            # Drop the worker representations
            for worker_name in to_be_dropped:
                del self.workers[worker_name]
        except:
            log.error("Exception thrown while mark & sweeping cached workers: %s" %
                      traceback.format_exc())
            raise
        finally:
            # Re-plan the next run of the task
            reactor.callLater(self.mark_and_sweep_interval,
                              self._run_mark_and_sweep_workers)

        if to_inactive > 0 or len(to_be_dropped) > 0:
            log.info("Inactive / dropped / cached workers: %d / %d / %d" %
                     (to_inactive, len(to_be_dropped), len(self.workers)))


    @defer.inlineCallbacks
    def _run_refresh_database(self):
        start = time.time()

        counter = 0
        try:
            # Refresh workers changed from the last chack
            counter = (yield self.worker_db.refresh_from_source(False,
                                                                self._on_worker_refresh))
        except:
            log.error("Exception thrown while refreshing workers db: %s" %
                      traceback.format_exc())
            raise
        finally:
            reactor.callLater(self.update_interval, self._run_refresh_database)

        if counter:
            # Don't spam log with blank notifications
            log.info("%d workers loaded in %.03f sec" % (counter, time.time() - start))

        # Signal that the initial database load/refresh has been
        # finished and so that dependent processes can go ahead
        if not self.on_load.called:
            self.on_load.callback(True)
