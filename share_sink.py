import ujson as json
from twisted.internet import task
from poolsrv.reporting import JsonReporter
from poolsrv import config
from poolsrv import files
from poolsrv.identity import RUNTIME
from poolsrv.posix_time import posix_secs
import poolsrv.logger
log = poolsrv.logger.get_logger()

class ShareSink(JsonReporter):

    def __init__(self, host, port,
                 max_queue_len=3600,
                 max_active_queue_len=5,
                 retry_after_s=5,
                 start_suspended=False):

        JsonReporter.__init__(self, host, port,
                              max_queue_len=max_queue_len,
                              max_active_queue_len=max_active_queue_len,
                              retry_after_s=retry_after_s,
                              start_suspended=start_suspended)

        async_group = files.TimeBasedAsyncFileSequence(config.SHARE_TRACE_FILE_NAME_PATTERN,
                                                       config.SHARE_TRACE_FILE_DIR,
                                                       "a",
                                                       period_length=3600,  # One hour period
                                                       auto_switch=False,   # We want to control the file switching
                                                       buffer_limit=20000)
        self._file_group = async_group
        self._trace_file = async_group.get_async_file()

        task.LoopingCall(self._trace_file.flush_buf).start(3.0)

        self._last_push_period = posix_secs() - 1

    def _connection_made(self):
        # Send identity information to the remote side directly, without affecting the queue itself
        self._queue.process_directly(['INIT', config.STRATUM_UNIQUE_ID, RUNTIME.uuid,
                                      int(RUNTIME.created_at * 1000)])

    def push_block(self, worker_id, submit_secs, block_hash, block_height, difficulty, value):

        if not config.SHARE_SINK_ENABLED and not config.SHARE_TRACE_ENABLED:
            return

        block = (block_hash, block_height, submit_secs, value, difficulty, worker_id)
        timestamp = posix_secs()
        event = ['BLOCK', timestamp, block]

        if config.SHARE_TRACE_ENABLED:
            # Ensure the record is going to the right file
            self._file_group.check_switch(timestamp)

            self._trace_file.write_buf(json.dumps(event))
            self._trace_file.write_buf('\n')

        if config.SHARE_SINK_ENABLED:
            self.deliver(event)

    def push_shares(self, push_period, shares):

        if not config.SHARE_SINK_ENABLED and not config.SHARE_TRACE_ENABLED:
            return

        push_period = int(push_period)
        if len(shares) == 0:
            shares = ()

        # Push empty shares event for empty/missed periods
        for empty_period in xrange(self._last_push_period + 1, push_period):
            self._do_push(empty_period, ('SH', empty_period, ()))

        # Translate the push data into list an event
        event = ('SH', push_period, shares)

        self._last_push_period = push_period
        self._do_push(push_period, event)

    def _do_push(self, push_period, event):

        if config.SHARE_TRACE_ENABLED:
            # Ensure the record is going to the right file
            self._file_group.check_switch(push_period)

            self._trace_file.write_buf(json.dumps(event))
            self._trace_file.write_buf('\n')

        if config.SHARE_SINK_ENABLED:
            self.deliver(event)

    def on_drop(self, obj, reason):
        log.error("Dropped object %s, reason %s" % (str(obj), str(reason)))

