import twisted.trial.unittest as unittest
from twisted.internet.defer import inlineCallbacks
from twisted.internet import defer
from twisted.internet import reactor

from pprint import pprint


import poolsrv.test.deep_eq as deep_eq

from poolsrv import logger
logger.init_logging('test.log', '', 'DEBUG', True)

from poolsrv.queues import DeliveryQueue


class Processor(object):

    def __init__(self, accept_method):
        self._result = []
        self._rejected_last = False
        self._accept_method = {
            'always' : self._accept_always,
            'second' : self._accept_second,
            'deferred_always' : self._ret_deffered,
            'deferred_second' : self._ret_deffered_second,
            'exception' : self._ret_exception,
        }[accept_method]

    def accept(self, obj):
        return self._accept_method(obj)

    def _accept_always(self, obj):
        print "received", obj
        self._result.append(obj)
        return True

    def _accept_second(self, obj):
        self._rejected_last = not self._rejected_last
        if self._rejected_last:
            return self._accept_always(obj)
        else:
            return False

    def _ret_deffered(self, obj):
        d = defer.Deferred()
        def deliver():
            d.callback(self._accept_always(obj))

        reactor.callLater(0, deliver)
        return d

    def _ret_deffered_second(self, obj):
        d = defer.Deferred()
        def deliver():
            d.callback(self._accept_second(obj))

        reactor.callLater(0, deliver)
        return d


    def _ret_exception(self, obj):
        raise Exception("Cannot deliver")

    def get_result_list(self):
        return self._result

    def get_result_set(self):
        return set(self._result)


def sleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d


class Test(unittest.TestCase):

    def setUp(self):
        pass

    @inlineCallbacks
    def test_always(self):
        p = Processor('always')
        q = DeliveryQueue(p.accept)
        yield self._test_generic(q, range(10), p.get_result_list)

    @inlineCallbacks
    def test_second(self):
        p = Processor('second')
        q = DeliveryQueue(p.accept, max_active_queue_len=3, retry_after_s=0.1)
        yield self._test_generic(q, set(range(10)), p.get_result_set)

    @inlineCallbacks
    def test_deferred_always(self):
        p = Processor('deferred_always')
        q = DeliveryQueue(p.accept, max_active_queue_len=5, retry_after_s=0.1)
        yield self._test_generic(q, range(10), p.get_result_list)

    @inlineCallbacks
    def test_deferred_always_with_drops(self):
        p = Processor('deferred_always')
        q = DeliveryQueue(p.accept, max_queue_len=20, max_active_queue_len=1, retry_after_s=0.1)
        yield self._test_generic(q, range(50), p.get_result_list, [0] + range(30,50))

    @inlineCallbacks
    def test_deferred_second(self):
        p = Processor('deferred_second')
        q = DeliveryQueue(p.accept, max_active_queue_len=1, retry_after_s=0.1)
        yield self._test_generic(q, range(10), p.get_result_list)

    @inlineCallbacks
    def test_deferred_second_2(self):
        p = Processor('deferred_second')
        q = DeliveryQueue(p.accept, max_active_queue_len=5, retry_after_s=0.1)
        yield self._test_generic(q, set(range(10)), p.get_result_set)


    @inlineCallbacks
    def test_exception(self):
        p = Processor('exception')
        q = DeliveryQueue(p.accept, max_active_queue_len=1, retry_after_s=0.1)
        yield self._test_generic(q, set(range(10)), p.get_result_set, set([]))


    @inlineCallbacks
    def _test_generic(self, queue, data, results_getter, result_data=None):
        for obj in data:
            queue.deliver(obj)

        while queue.get_size() > 0:
            (yield sleep(0.5))

        if result_data is not None:
            data = result_data

        self._check_results(data, results_getter())
        queue.suspend()


    def _check_results(self, org, result):
        if not deep_eq.deep_eq(org, result):
            print "Original collection: "
            pprint(org)
            print "Delivered collection: "
            pprint(result)
            raise Exception("Delivered different objects")

