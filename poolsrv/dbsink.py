from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.enterprise import adbapi
import traceback
import time

from poolsrv.queues import DeliveryQueue, DeliveryStats

import poolsrv.logger
log = poolsrv.logger.get_logger()


STATEMENTS_LATE_LIMIT_SECONDS = 5
LOG_STATS_ON_ERROR = False


class DbSink(object):

    def __init__(self, db_settings, max_queue_len=None):

        # There is no upper limit on the queue length by default
        if not max_queue_len:
            max_queue_len = -1

        db_settings = self._filter_pool_settings(db_settings)

        # Create delivery queue for database statements to be executed
        self._queue = DeliveryQueue(self._process_statement,
                                    drop_callback=self._on_queue_drop,
                                    max_queue_len=max_queue_len,
                                    # Max parallel statements - should correspond with connection count
                                    max_active_queue_len=db_settings['pool_size'],
                                    # How often to retry deliver some
                                    retry_after_s=5)
        self._stats = DeliveryStats(STATEMENTS_LATE_LIMIT_SECONDS)

        self._drop_count = 0

        # Create db-pool with the provided arguments
        self._db_pool = self._create_db_pool(**db_settings)
        self._second_db_pool = None
        self._suspended = False

    @staticmethod
    def _on_connect(conn):
        log.info("New DB connection created")
        conn.cursor().execute("set session transaction isolation level read uncommitted")
        conn.autocommit(True)

    def _create_db_pool(self, driver, host, port, user, password, dbname, pool_size):
        return adbapi.ConnectionPool(driver, host=host, port=port, user=user, passwd=password,
                                     db=dbname, cp_openfun=self._on_connect, cp_reconnect=True,
                                     cp_min=1, cp_max=pool_size)

    @staticmethod
    def _filter_pool_settings(settings):
        settings = {
            'driver': settings.get('driver'),
            'host': settings.get('host'),
            'port': settings.get('port'),
            'user': settings.get('user'),
            'password': settings.get('password'),
            'dbname': settings.get('dbname'),
            'pool_size': settings.get('pool_size'),
        }
        if sum(val > 1 for val in settings.values()) != 7:
            raise Exception('Some database parameter not specified (%s)' % (str(settings)))

        return settings

    def get_queue_length(self):
        return self._queue.get_length()

    def get_stats(self):
        return self._stats.get_data()

    def reset_stats(self):
        return self._stats.reset()

    def execute(self, statement_name, statement):
        return self._queue.deliver((statement_name, statement))

    def direct_query(self, name, query):
        return self._inspect(name, self._db_pool.runQuery(query), True)

    def _process_statement(self, obj):
        """ Deliver queue's processor """
        # Try to deliver later, db_pool is not ready or the operation is suspended
        if self._suspended or not self._db_pool:
            return False
        try:
            (name, statement) = obj
            return self._inspect(name, self._db_pool.runOperation(statement), False)
        except BaseException, e:
            return e

    def prepare_second_db_pool(self, db_settings):
        if self._second_db_pool:
            raise Exception("Second pool already created")
        self._second_db_pool = self._create_db_pool(**self._filter_pool_settings(db_settings))

    def is_suspended(self):
        return self._suspended

    def suspend(self):
        self._suspended = True

    def resume(self):
        self._suspended = False

    def cancel_active_queue(self, remove_active=False, remove_redelivering=True):
        self._queue.cancel_active_queue(remove_active, remove_redelivering)

    def switch_db_pools(self, force=False):
        if not self._second_db_pool and not force:
            raise Exception("Second pool does NOT exist")

        temp = self._db_pool
        self._db_pool = self._second_db_pool
        self._second_db_pool = temp

    def close_second_db_pool(self, forget=False):
        if not self._second_db_pool:
            raise Exception("Second pool does NOT exist")

        if not forget:
            self._second_db_pool.close()
        self._second_db_pool = None

    @inlineCallbacks
    def test_second_db_pool(self):
        defer.returnValue((yield self._second_db_pool.runQuery('select now();')))

    def _on_queue_drop(self, obj, reason):
        log.error("Dropped statement '%s', reason: %s" % (str(obj), str(reason)))
        self._drop_count += 1

    def _statement_succeeded(self, result, sent_at, call_name, is_direct_query):
        """ The statement has been successfully processed so we should indicate that
        by returning True (queries return their results) """
        try:
            duration = (time.time() - sent_at)
            in_time = self._stats.success(call_name, duration)
            if not in_time:
                log.error("Statement %s is late (%0.0fs) !" % (call_name, duration))
                if LOG_STATS_ON_ERROR:
                    log.info(self._stats.get_stats_str())
        except:
            log.error("Exception ignored in '_statement_succeeded': %s" %
                      traceback.format_exc())

        # In case of a query, we pass the original result object to the caller
        if is_direct_query:
            return result

        # Otherwise the result is for delivery queue so that we signal successful delivery
        return True

    def _statement_failed(self, failure, sent_at, call_name, is_direct_query):
        """
        We need to recognize here which failure is a failure because of invalid statement (unrecoverable)
        and which failure is recoverable (e.g. database connection down).
        """
        try:
            self._stats.failure(call_name, (time.time() - sent_at))

            log.error("Statement %s failed! %s" % (call_name, str(failure)))
            if LOG_STATS_ON_ERROR:
                log.info(self._stats.get_stats_str())
        except:
            log.error("Exception ignored in 'statement_failed': %s" %
                      traceback.format_exc())

        # In case of a query, we pass the original failure object.
        if is_direct_query:
            return failure

        # By default all errors are unrecoverable
        # return failure

        # For now: all errors are expected to be recoverable
        return False

    def _inspect(self, call_name, db_statement_deferred, is_direct_query):

        self._stats.initiated(call_name)

        sent_at = time.time()
        args = (sent_at, call_name, is_direct_query)
        db_statement_deferred.addCallbacks(self._statement_succeeded, self._statement_failed,
                                           callbackArgs=args, errbackArgs=args)
        return db_statement_deferred


