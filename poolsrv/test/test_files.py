import twisted.trial.unittest as unittest
from twisted.internet.defer import inlineCallbacks
from twisted.internet import defer
from twisted.internet import reactor


from poolsrv.files import WritableAsyncFile, WritableAsyncFileGroup

class Test(unittest.TestCase):

    @inlineCallbacks
    def test_simple(self):

        f = WritableAsyncFile('test_file', 'w')

        for i in xrange(100):
            f.write(('iteration %d\n' % i) * 1000)

        d = defer.Deferred()
        f.wait(d.callback)
        print (yield d)


    @inlineCallbacks
    def test_buffered(self):

        f = WritableAsyncFile('test_file_buffered', 'w', buffer_limit=3011)

        for i in xrange(10000):
            f.write_buf(('iteration %d\n' % i))

        f.flush_buf()

        d = defer.Deferred()
        f.wait(d.callback)
        print (yield d)


    @inlineCallbacks
    def test_group(self):

        g = WritableAsyncFileGroup('a', 'group/', 'buffered_%03d')
        f = g.get_async_file()

        for _ in xrange(20):
            for i in xrange(100):
                f.write_buf(('iteration %d\n' % i))
            g.switch_to_next_file()

        d = defer.Deferred()
        f.close(d.callback)
        print (yield d)

    @inlineCallbacks
    def test_group_explicit(self):

        g = WritableAsyncFileGroup('a', 'group/', None, pattern=False)
        f = g.get_async_file()

        for f_idx in xrange(20):
            g.switch_to_next_file("explicit_%03d" % f_idx)
            for i in xrange(100):
                f.write_buf(('iteration %d\n' % i))

        d = defer.Deferred()
        f.close(d.callback)
        print (yield d)

