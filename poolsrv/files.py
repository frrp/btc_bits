import collections
import os
import os.path
import datetime
import time

from twisted.internet.threads import deferToThread
from twisted.internet import reactor

import traceback

import poolsrv.logger
log = poolsrv.logger.get_logger()


class AsyncFile(object):
    """
    The class implements it's own buffering because we want to minimize
    number of deferred calls to the underlying file object (it is blocking operation).
    So we explicitly disable the file's buffering and provide our own write buffer (by default).
    When the buffer is full, we write the whole content directly to the OS (thru file object).

    It is possible to read from the file but the class is mainly focused on efficient
    async write. No special handling of read buffers is implemented.
    """

    def __init__(self, file_name, mode, buffer_limit=30000,
                 stdfile_buffer=0):
        self._file = None
        self._queue = collections.deque()
        self._busy = False
        self._buffer = []
        self._buffer_size = 0
        self._buffer_limit = buffer_limit
        self._stdfile_buffer = stdfile_buffer

        # Switch in the specified file
        if file_name:
            self.switch(file_name, mode, buffer_limit)

    def switch(self, file_name, mode, buffer_limit=30000, create_parents=True):
        if self._file:
            # Issue buffer flush and async close
            self.flush_buf()
            self.close()
        self._buffer_limit = buffer_limit

        if create_parents:
            try:
                os.makedirs(os.path.dirname(file_name))
            except OSError:
                pass

        # Open the file by default without buffering!! We provide it manually.
        self._file = open(file_name, mode, self._stdfile_buffer)

    def file_name(self):
        if self._file:
            return self._file.name
        else:
            return None

    def read(self, callback, size):
        self._deferred_call(self._file.read, callback, size)

    def readlines(self, callback, size):
        self._deferred_call(self._file.readlines, callback, size)

    def write_buf(self, data):
        self._buffer.append(data)
        self._buffer_size += len(data)
        if self._buffer_size >= self._buffer_limit:
            self.flush_buf()

    def flush_buf(self):
        if self._buffer_size > 0:
            data = "".join(self._buffer)
            self._buffer = []
            self._buffer_size = 0
            self._deferred_call(self._file.write, None, data)

    def write(self, data, callback=None):
        # We need to write everything from the buffer first to maintain
        # write operation sequence
        self.flush_buf()
        self._deferred_call(self._file.write, callback, data)

    def flush(self, callback=None):
        self.flush_buf()
        self._deferred_call(os.fsync, callback, self._file.fileno())

    def close(self, callback=None):
        self.flush_buf()
        func = self._file.close
        self._file = None
        self._deferred_call(func, callback)

    def close_immediate(self):
        if self._file:
            self._file.close()
            self._file = None

    def wait(self, callback):
        self._deferred_call(lambda x: x, callback, None)

    def _deferred_call(self, func, callback, *args):
        if self._busy:
            self._queue.appendleft((func, callback, args))
        else:
            self._busy = True
            d = deferToThread(func, *args)
            d.addBoth(self._finished, callback)

    def _finished(self, result, callback):
        # It was the last operation in the queue?
        if len(self._queue) == 0:
            self._busy = False
        else:
            # Plan the next operation
            (func, new_callback, args) = self._queue.pop()
            d = deferToThread(func, *args)
            d.addBoth(self._finished, new_callback)
        # Report the result
        if callback:
            callback(result)


class AsyncFileSequence(object):

    def __init__(self, file_name_or_pattern, file_dir, mode, pattern=True,
                 switch_callback=None, buffer_limit=30000):
        self._index = -1
        self._mode = mode
        self._file_dir = file_dir
        self._buffer_limit = buffer_limit
        self._switch_callback = switch_callback

        if pattern:
            self._pattern = file_name_or_pattern
            file_name = None
        else:
            self._pattern = None
            file_name = file_name_or_pattern

        # Ensure there is the file directory created
        if not os.path.exists(file_dir):
            try:
                os.makedirs(file_dir)
            except OSError, _:
                pass

        # Create async file not associated with any underlying OS file yet
        self._async_file = AsyncFile(None, mode, buffer_limit)

        # If we already know the first file name, switch it in
        if file_name or self._pattern:
            # The stored pattern is used when file_name is None
            self.switch_to_next_file(file_name)

    def get_async_file(self):
        return self._async_file

    def _get_file_name(self, generate_new):
        if not self._pattern:
            raise Exception("File name pattern not specified")

        if generate_new:
            self._index += 1
        return self._pattern % self._index

    def switch_to_next_file(self, file_name=None):
        if not file_name:
            file_name = self._get_file_name(True)

        # Prepend file directory before the actual name
        file_name = os.path.join(self._file_dir, file_name)

        # Switch the underlying async file to the new one, all not yet executed operations
        # will be executed properly and then the file will be closed
        self._async_file.switch(file_name, self._mode, self._buffer_limit)

        # Notify the user about just completed file switch
        if self._switch_callback:
            try:
                self._switch_callback(self._async_file.file_name(), file_name)
            except:
                log.error(traceback.format_exc())

        return file_name


class TimeBasedAsyncFileSequence(AsyncFileSequence):
    """
    Files are switched on regular basis (after some time period). It can be done
    automatically when the class switches the files itself. Or a user can ask to switch the files
    manually by calling 'check_switch' - then it enables to precisely control which writes
    to underlying async file object go to which physical file.
    """

    def __init__(self, file_name_pattern, file_dir, mode,
                 # One hour is a default period
                 period_length=3600,
                 switch_callback=None, buffer_limit=30000, auto_switch=True):

        AsyncFileSequence.__init__(self, None, file_dir, mode, pattern=False,
                                   switch_callback=switch_callback, buffer_limit=buffer_limit)

        self._timed_pattern = file_name_pattern
        self._period_length = int(period_length)
        self._curr_time_period = 0
        self._auto_switch = auto_switch

        # Switch in the first file to be prepared for writing
        if auto_switch:
            self.check_switch()

    def check_switch(self, time_point=None):
        if time_point is None:
            time_point = time.time()

        time_period = int(time_point / self._period_length)

        if time_period != self._curr_time_period:
            self._do_switch(time_period)
        else:
            if self._auto_switch and not time_point:
                reactor.callLater(0.5, self.check_switch)

    def _do_switch(self, time_period):

        beginning = time_period * self._period_length
        d_time = datetime.datetime.fromtimestamp(beginning)
        file_name = d_time.strftime(self._timed_pattern)

        self._curr_time_period = time_period

        # Do we need to plan a switch check?
        if self._auto_switch:
            elapsed = time.time() - beginning
            reactor.callLater(max(1, self._period_length - elapsed - 1),
                              self.check_switch)

        self.switch_to_next_file(file_name)
