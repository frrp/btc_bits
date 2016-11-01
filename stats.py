from poolsrv.posix_time import posix_secs
import poolsrv.logger
log = poolsrv.logger.get_logger()


class PeerStats(object):
    """Stub for server statistics"""
    counter = 0
    changes = 0
    log_secs = posix_secs()

    @classmethod
    def client_connected(cls, ip):
        cls.counter += 1
        cls.changes += 1

        cls.print_stats()

    @classmethod
    def client_disconnected(cls, ip):
        cls.counter -= 1
        cls.changes += 1

        cls.print_stats()

    @classmethod
    def print_stats(cls):
        now = posix_secs()
        if cls.counter and (now < cls.log_secs + 3 or float(cls.changes) / cls.counter < 0.05):
            # Print connection stats only when more than
            # 5% connections change to avoid log spam
            return

        log.info("%d peers connected, state changed %d times" % (cls.counter, cls.changes))
        cls.log_secs = now
        cls.changes = 0

    @classmethod
    def get_connected_clients(cls):
        return cls.counter
