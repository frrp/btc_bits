'''This module contains classes used by pool core to interact with the rest of the pool.
   Default implementation do almost nothing, you probably want to override these classes
   and customize references to interface instances in your launcher.
   (see launcher_demo.tac for an example).
'''

import time
from twisted.internet import reactor, defer

import poolsrv.logger
log = poolsrv.logger.get_logger()


class WorkerManagerInterface(object):
    def __init__(self):
        # Fire deferred when manager is ready
        self.on_load = defer.Deferred()
        self.on_load.callback(True)

    def authorize(self, worker_name, worker_password):
        return True

class ShareLimiterInterface(object):
    '''Implement difficulty adjustments here'''
    def on_submit_share(self, connection_ref, session):
        '''
        connection_ref - weak reference to Protocol instance
        session - session associated with the connection
        '''
        pass

class ShareManagerInterface(object):
    def __init__(self):
        # Fire deferred when manager is ready
        self.on_load = defer.Deferred()
        self.on_load.callback(True)

    def on_network_block(self, prevhash):
        '''Called when there's new block coming from the network (possibly new round)'''
        pass

    def on_submit_share(self, worker_name, ipaddr, block_header, block_hash, shares, timestamp, is_valid):
        log.info("%s %s %s" % (block_hash, 'valid' if is_valid else 'INVALID', worker_name))

    def on_submit_block(self, is_accepted, worker_name, block_header, block_hash, timestamp):
        log.info("Block %s %s" % (block_hash, 'ACCEPTED' if is_accepted else 'REJECTED'))

class TimestamperInterface(object):
    '''This is the only source for current time in the application.
    Override this for generating unix timestamp in different way.'''
    def time(self):
        return time.time()

class IdsProviderInterface(object):

    def get_new_session_id(self):
        raise

class DefaultIdsProvider(IdsProviderInterface):
    ''' Default implementation of the provider interface'''

    def __init__(self):
        self._last_session_id = 0

    def get_new_session_id(self):
        # We generate session ids as from a wrapping sequence.
        # It should never cross 32-bit size as it is used id db.
        self._last_session_id = (self._last_session_id + 1) % (2 << 32)
        return self._last_session_id


class PredictableTimestamperInterface(TimestamperInterface):
    '''Predictable timestamper may be useful for unit testing.'''
    start_time = 1345678900 # Some day in year 2012
    delta = 0

    def time(self):
        self.delta += 1
        return self.start_time + self.delta


class ReporterInterface(object):

    def report(self, event_name, session):
        pass


class Interfaces(object):
    worker_manager = None
    share_manager = None
    share_limiter = None
    timestamper = None
    template_registry = None
    ids_provider = None
    reporter = None
    share_sink = None
    admin = None

    @classmethod
    def set_share_sink(cls, share_sink):
        cls.share_sink = share_sink

    @classmethod
    def set_reporter(cls, reporter):
        cls.reporter = reporter

    @classmethod
    def set_worker_manager(cls, manager):
        cls.worker_manager = manager

    @classmethod
    def set_share_manager(cls, manager):
        cls.share_manager = manager

    @classmethod
    def set_share_limiter(cls, limiter):
        cls.share_limiter = limiter

    @classmethod
    def set_timestamper(cls, manager):
        cls.timestamper = manager

    @classmethod
    def set_template_registry(cls, registry):
        cls.template_registry = registry

    @classmethod
    def set_ids_provider(cls, provider):
        cls.ids_provider = provider

    @classmethod
    def set_admin(cls, admin):
        cls.admin = admin
