import random
import sqlite3
import os
import datetime
import traceback
from twisted.internet import defer
import dbquery
from poolsrv import config
import poolsrv.logger
log = poolsrv.logger.get_logger()

class WorkerDB(object):
    """
    This class represents local database of workers.

    It works as a local persistent cache for remote database
    content. WorkerDB is able to synchronize itself incrementally with
    the remote source database.

    Two tables are held in the local database. local_worker and
    last_sync_time. local_worker contains all cached facts about
    workers. last_sync_time contains only one row with time when the
    last synchronization with the source database was executed.
    """
    SYNC_DATE_FORMAT = '%Y-%m-%d %H:%M:%S UTC'

    def __init__(self, path):

        self._create_connection(path)

        self._ensure_db_structure(path)

    def _create_connection(self, path):
        # Create the connection to the local database for workers
        connection = sqlite3.connect(path)
        connection.text_factory = str
        self.connection = connection

        # Prepare a cursor for executing statements
        self.cursor = connection.cursor()

    def _ensure_db_structure(self, path):
        # Check the database structure by querying it with a check
        # statement
        try:
            self.cursor.execute(CHECK_LOCAL_DB)
            if len(self.cursor.fetchmany()) > 0:
                raise Exception('Some check rows returned')
        except:
            log.error("Exception thrown while checking db structure: %s" %
                      traceback.format_exc())
            # The database is inconsistent, so we need to drop it and
            # recreate it again
            os.remove(path)
            self.cursor.close()
            self.connection.close()
            # Recreate the database
            self._create_connection(path)

        # TODO: Put this code block into the previous exception
        # handler (IF python can handle exceptions in exceptions
        # gracefully

        # Try to create the database structure, just for the case when
        # the database has not yet been created (new server instance
        # etc.) or discarded because of being inconsistent.
        try:
            self.cursor.executescript(LOCAL_DB_CREATION_SCRIPT)
            log.info("Database structure created")
        except:
            # Intentionally do nothing, the database can be already
            # created
            pass


    def get_worker_facts(self, worker_name):
        '''
        Returns tuple in form
        (id_worker, worker_name, difficulty_suggested)
        or None if the worker is not know.
        '''
        self.cursor.execute(GET_WORKER, {'worker_name': worker_name})
        # Just return the query result (single row is expected)
        return self.cursor.fetchone()


    def get_last_sync_time(self):

        self.cursor.execute(GET_LAST_SYNC_TIME, {})
        sync_time = self.cursor.fetchmany()
        log.info("Sync time: '%s'" % str(sync_time))
        # Detect inconsistent data and return no last sync time
        if len(sync_time) != 1 or len(sync_time[0]) != 1:
            log.error("Could not use last sync time")
            return None
        else:
            time = datetime.datetime.strptime(sync_time[0][0], self.SYNC_DATE_FORMAT)
            log.info("Parsed time: '%s'" % str(time))
            return time


    def _set_last_sync_time(self, sync_time):
        '''Just writes the specified time to the database'''
        self.cursor.execute(SET_LAST_SYNC_TIME, {'sync_time':
                                                 sync_time.strftime(self.SYNC_DATE_FORMAT)})


    @defer.inlineCallbacks
    def refresh_from_source(self, full_refresh, worker_change_callback):
        '''
        Refresh content of the local database with facts loaded from
        the source database.

        Incremental refresh is automatically selected whenever
        'full_refresh' is not requested and when the database itself
        is in an expected conditions (e.g. last sync. time is
        know). When incremental mode is selected then only a part of
        workers is updated - changed since the specified date.

        If 'full_refresh' is set then full refresh is performed and
        all workers are rewritten and all workers not contained in the
        source database are deleted locally as well.

        Returns number of updated workers.
        '''
        last_sync_time = self.get_last_sync_time()
        # When some last sync time was found, shift it by some overlap
        # time to ensure all changes will be cached (time difference
        # between servers etc).
        if not last_sync_time is None:
            last_sync_time -= datetime.timedelta(seconds=config.LOCAL_WORKER_DB_REFRESH_TIME_OVERLAP_S) #@UndefinedVariable
        else:
            # Full refresh is selected when the date
            full_refresh = True

        # We need to pass some valid date instead of NULL to the
        # query, but we use some really historical date for this
        # purpose
        if full_refresh:
            last_sync_time = config.NULL_DATE #@UndefinedVariable

        # Marker for detection of deleted workers
        refresh_mark = random.randint(0, 100000)
        offset = 0
        counter = 0

        # Take the new timestamp BEFORE updating. It will be saved
        # after all the work is done.
        new_sync_time = datetime.datetime.now()

        # Goes thru all workers that need to be refreshed from the
        # source database. We don't care here explicitly if we are
        # refreshing only a part of the database or not.
        while True:

            log.info('Loading workers...')

            # Load part of the workers to be updated, only one group
            # is loaded at a time. We are shifting each call to the
            # next group of workers. TODO: When streaming in supported
            # we can drop the grouping fully.
            group = (yield dbquery.get_workers_group(last_sync_time,
                                                     config.LOCAL_WORKER_DB_GROUP_SIZE,
                                                     offset))
            # Nothing to be updated, we can leave the loading loop.
            if len(group) == 0:
                break

            # Move the offset for the next query
            offset += config.LOCAL_WORKER_DB_GROUP_SIZE #@UndefinedVariable

            log.info('Loaded %d workers from source database' % len(group))

            # Go thru all the fetched workers from the source database
            # within the last group.
            for worker_id, username, suffix, difficulty_suggested in group:
                worker_name = "%s.%s" % (username, suffix)
                counter += 1

                # Prepare the new facts being written to the
                # database. Also works as a structure passed to the
                # worker change callback (see params)
                worker_update = {
                    'worker_id': worker_id,
                    'worker_name': worker_name,
                    'difficulty_suggested': difficulty_suggested,
                    'refresh_mark': refresh_mark
                }

                # Write the information to the local database. All
                # updated/inserted rows are marked with a random mark
                # to be distinguishable from not touched
                self.cursor.execute(WRITE_LOCAL_WORKER, worker_update)

                # We want to commit regularly to hold memory consumption
                # of the local database low.
                if (counter % config.LOCAL_WORKER_DB_REFRESH_COMMIT_SIZE) == 0:
                    self.connection.commit()

                # If there is a callback passed we need to notify the
                # caller about this update
                if worker_change_callback:
                    worker_change_callback(worker_update)

        # If there was some worker refreshed, we need to commit at the
        # end of the work (can be redundant in some rare cases)
        if counter:
            # Store the timestamp to the database before committing
            self._set_last_sync_time(new_sync_time)
            self.connection.commit()

        # If we refreshed all known workers from the source database,
        # we can understand all the rest as deleted from the source
        # and then delete them from the local database as well.
        if full_refresh:
            self.cursor.execute(REMOVE_OLD_WORKERS, {'refresh_mark': refresh_mark})
            # Again, we need to commit immediately
            self.connection.commit()

        # Return number of refreshed workers
        defer.returnValue(counter)


WRITE_LOCAL_WORKER = '''
insert or replace into local_worker (
    worker_id, worker_name, difficulty_suggested, refresh_mark
) values (
    :worker_id, :worker_name, :difficulty_suggested, :refresh_mark
);
'''

GET_WORKER = '''
select worker_id, worker_name, difficulty_suggested
from local_worker
where worker_name = :worker_name;
'''

REMOVE_OLD_WORKERS = '''
delete from local_worker
where refresh_mark != :refresh_mark
'''

GET_LAST_SYNC_TIME = '''
select sync_time from last_sync_time;
'''

SET_LAST_SYNC_TIME = '''
update last_sync_time
set sync_time = :sync_time;
'''

LOCAL_DB_CREATION_SCRIPT = '''
create table local_worker (
  worker_name text primary key,
  worker_id integer not null,
  difficulty_suggested integer not null,
  refresh_mark integer not null
);

create table last_sync_time (
  sync_time text not null
);

insert into last_sync_time values ('1970-01-01 00:00:00 UTC');

'''

# Checks existence of both tables and corresponding columns and checks
# that there is exactly one row in last_sync_time table.
CHECK_LOCAL_DB = '''
select 0 from (
  select worker_id, worker_name, difficulty_suggested
  from local_worker
  where 1=0
) x
union all
select cnt from (
  select count(sync_time) cnt
  from last_sync_time
) x where x.cnt != 1;

'''
