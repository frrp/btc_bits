from twisted.internet import defer
from twisted.internet import reactor

from exceptions import BaseException
import collections
import math
import datetime

import poolsrv.logger
from poolsrv.posix_time import posix_secs

log = poolsrv.logger.get_logger()


def generic_processor(call):
    if len(call) == 2:
        (func, args) = call
        return func(*args)
    else:
        (func, args, kwargs) = call
        return func(*args, **kwargs)


class DeliveryQueue(object):

    def __init__(self, processor,
                 drop_callback=None,
                 max_queue_len=1024,
                 max_active_queue_len=1,
                 retry_after_s=3,
                 start_suspended=False):
        """
        Return value from processor means:
            True/None - delivered
            False - not delivered, try again later
            Other/Exception - processing error, drop the item
            Deferred: callback behaves as return value errback as excption
        """

        # Remember the processor
        self._processor = processor
        # Initialize an empty queue for queuing new objects
        self._queue = collections.deque([])
        self._active_queue = collections.OrderedDict()
        self._last_active_id = 0
        # True means that there is some item in active queue
        # that should be re-delivered
        self._redeliver_ids = set()

        self._retry_after_s = retry_after_s
        assert self._retry_after_s > 0
        self._plan_retry_call()
        # We should suspend the queue's retry process, as requested
        if start_suspended:
            self.suspend()

        self._max_queue_len = int(max_queue_len)
        assert self._max_queue_len >= 1 or self._max_queue_len == -1

        self._max_active_queue_len = int(max_active_queue_len)
        assert self._max_active_queue_len >= 1

        self._in_deliver_from_queue = False
        # self.file_name_pattern = "%s_%(pid)d_%(id)d_%(seq)d"

        if drop_callback is None:
            self._drop_callback = self._default_drop_callback
        else:
            self._drop_callback = drop_callback

        self._retry_delayed_call = None

    def deliver(self, obj):
        # We check if we can immediately try to deliver the object or if
        # we have to wait and therefore enqueue the object for later delivery
        if not self._is_active_queue_full():
            # Either deliver the object or place it to the active queue
            self._try_to_deliver(obj)
        else:
            # Place the object to non-active queue
            self._enqueue(obj)

    def deliver_immediately(self, obj):
        """ Tries to deliver the object immediately. If active queue is full when called,
            it can cause the active queue to be larger then size understood as full queue
            (when the delivery is unsuccessful) !! """

        # Either deliver the object or place it to the active queue.
        self._try_to_deliver(obj)

    def process_directly(self, obj):
        """ Tries to deliver the object immediately. Doesn't affect any queue when not successful."""
        try:
            # Try to deliver
            result = self._processor(obj)
        except BaseException, e:
            result = e

        return result

    def cancel_active_queue(self, remove_active, remove_redelivering):
        for id, val in self._active_queue.iteritems():
            _, d = val
            # There is nothing happening with
            if d is None:
                if remove_redelivering:
                    del self._active_queue[id]
                    self._redeliver_ids.remove(id)
            else:
                if remove_active:
                    # Remove the statement by rising an error
                    d.errback(False)

    def _deliver_from_queues(self):
        """ Tries to deliver some next waiting object(s). At first it tries to re-deliver
        objects from active queue and then as many objects as possible from non-active queue."""

        if self._in_deliver_from_queue:
            return

        self._in_deliver_from_queue = True
        try:
            # Iterate over objects in the active queue, which should be re-delivered
            # We need to copy the set first to enable its modification from the loop
            for id in self._redeliver_ids.copy():
                (obj, d) = self._active_queue[id]
                self._redeliver_ids.remove(id)

                assert d is None
                try:
                    # Try to deliver
                    result = self._processor(obj)
                except BaseException, e:
                    result = e
                # Process the result as if it was from deferred
                # (we already have the object in the active queue
                if self._process_active_result(result, id) is not True:
                    # Break whenever we received False or an exception
                    break

            # While there are objects waiting in non-active queue and there is some space
            # in active queue, try to deliver new objects from the non-active queue
            while len(self._queue) > 0 and (not self._is_active_queue_full()):
                obj = self._queue.pop()
                self._try_to_deliver(obj)
        finally:
            self._in_deliver_from_queue = False

    def retry(self):
        try:
            self._deliver_from_queues()
        finally:
            self._plan_retry_call()

    def _plan_retry_call(self):
        self._retry_delayed_call = reactor.callLater(self._retry_after_s, self.retry)

    def _is_active_queue_full(self):
        return len(self._active_queue) >= self._max_active_queue_len

    def _enqueue(self, obj):
        """ Adds the object to the non-active queue. """

        q = self._queue
        q.appendleft(obj)
        # If there is no space left, we have to remove the oldest object
        if len(q) > self._max_queue_len > 0:
            drop_obj = q.pop()
            self._drop_callback(drop_obj, None)

    def _enqueue_active(self, d, obj):
        """ Adds the object and deferred to the active queue. """

        id = self._last_active_id + 1
        self._last_active_id = id

        self._active_queue[id] = (obj, d)
        if d is not None:
            self._install_callbacks(d, id)
        else:
            self._redeliver_ids.add(id)

    def _install_callbacks(self, d, id):
        args = (id,)
        d.addCallbacks(self._process_active_result, self._errorback,
                       callbackArgs=args, errbackArgs=args)

    def _errorback(self, result, id):
        """ Called when deferred delivery exceptionally failed. Normal delivery failure
        (of type "cannot deliver right now, try it later") is handled by _process_active_result.
        """

        (obj, _) = self._active_queue[id]

        # Drop the object permanently
        del self._active_queue[id]
        result = self._drop_callback(obj, result)

        # Try to deliver some next object from the queues, there is some space left
        # in the active queue
        self._deliver_from_queues()

        return result

    def _process_active_result(self, result, id):
        """ Called when deferred delivery was successful or failed in an expected manner
        (and we should try to re-deliver the object). Or called when we receive immediate
        result for re-delivered object."""

        # Successfully delivered object
        if result is None or result is True:
            # Remove the item from queue - it was processed
            del self._active_queue[id]

            # Try to deliver some next object from the queues, there is some space left
            # in the active queue
            self._deliver_from_queues()

            return True
        # Delivery failed, try again later
        elif result is False:
            (obj, _) = self._active_queue[id]
            # Remove the deferred from the queue
            # since it no longer represents active delivery attempt
            self._active_queue[id] = (obj, None)
            # Remember that the object should be "re-delivered"
            self._redeliver_ids.add(id)

            return False
        # Delivery is in progress, remember it
        elif isinstance(result, defer.Deferred):
            (obj, _) = self._active_queue[id]
            # Install callbacks for being notified when the result is ready
            self._install_callbacks(result, id)
            # Remember the deferredInterfaces
            self._active_queue[id] = (obj, None)

            return True
        else:
            # Unexpected result detected, call _errorback instead.
            return self._errorback(result, id)

    def _try_to_deliver(self, obj):
        """ Tries to deliver new object (newly incoming or taken from non-active queue).
            It is either delivered or placed to the active queue for next processing. """
        result = self.process_directly(obj)

        # Result is not known yet
        if isinstance(result, defer.Deferred):
            # place the object to the active queue
            self._enqueue_active(result, obj)
        # We know the result immediately
        elif isinstance(result, bool):
            if result:
                # Successfully delivered the item, nothing more to do
                pass
            else:
                # We need to retry it again in future
                self._enqueue_active(None, obj)
        # Unexpected situations -> drop the object
        elif isinstance(result, BaseException):
            self._drop_callback(obj, result)
        else:
            self._drop_callback(obj, "Invalid result from processor (type: %s)" % (type(result)))

    @staticmethod
    def _default_drop_callback(obj, reason=None):
        print "dropped object", obj, "reason", str(reason)
        return None

    def get_length(self):
        return len(self._queue) + len(self._active_queue)

    def suspend(self):
        if self._retry_delayed_call.active():
            self._retry_delayed_call.cancel()

    def resume(self):
        if not self._retry_delayed_call.active():
            self._plan_retry_call()


class DeliveryStats(object):

    def __init__(self, late_delivery_limit=5):
        self._statements_sent = {}
        self._statements_ok = {}
        self._statements_late = {}
        self._statements_err = {}
        self._timing = {}
        self._last_late_statement_at = None
        self._late_delivery_limit = late_delivery_limit
        self._reset_secs = posix_secs()

    def initiated(self, call_name):
        # Increase number of calls for the given name
        self._statements_sent[call_name] = self._statements_sent.get(call_name, 0) + 1

    def success(self, call_name, duration):
        bucket = 2 ** math.ceil(math.log(duration, 2))

        t = self._timing.get(call_name, {})
        t[bucket] = t.get(bucket, 0) + 1
        self._timing[call_name] = t

        if duration >= self._late_delivery_limit:
            self._statements_late[call_name] = self._statements_late.get(call_name, 0) + 1
            self._last_late_statement_at = datetime.datetime.now()
            return False
        else:
            self._statements_ok[call_name] = self._statements_ok.get(call_name, 0) + 1
            return True

    def failure(self, call_name, duration):
        self._statements_err[call_name] = self._statements_err.get(call_name, 0) + 1

    def get_stats_str(self):
        results_a = []
        results_b = []
        for stmt in self._statements_sent.keys():
            ok = self._statements_ok.get(stmt) or 0
            late = self._statements_late.get(stmt) or 0
            sent = self._statements_sent.get(stmt) or 0
            err = self._statements_err.get(stmt) or 0

            results_a.append("\n@%22s : %4d | %4d | %4d | %4d | %4d err|lt|pend|ok|ini" %
                             (stmt, err, late, sent - (err + ok + late), ok, sent))
            times = []
            keys = (self._timing.get(stmt) or {}).keys()
            keys.sort()
            keys.reverse()
            for bucket in keys:
                times.append("%2.03f: %4d" % (bucket, self._timing[stmt][bucket]))
            results_b.append("\n@%22s : %s" % (stmt, " | ".join(times),))

        return "".join(results_a) + "".join(results_b) + \
            ("\nlast late: %s" % str(self._last_late_statement_at))

    def reset(self):
        self._statements_sent.clear()
        self._statements_ok.clear()
        self._statements_late.clear()
        self._statements_err.clear()
        self._timing.clear()
        self._last_late_statement_at = None
        self._reset_secs = posix_secs()

    def get_data(self):
        timing = {}
        for key, value in self._timing.iteritems():
            timing[key] = value.copy()
        return {
            'sent': self._statements_sent.copy(),
            'ok': self._statements_ok.copy(),
            'late': self._statements_late.copy(),
            'err': self._statements_err.copy(),
            'timing': timing,
            'reset_secs': self._reset_secs
        }

