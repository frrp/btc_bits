from twisted.internet import reactor, defer
from poolsrv import config
import util
from poolsrv.posix_time import posix_time
import poolsrv.logger
log = poolsrv.logger.get_logger()

class BlockUpdater(object):
    '''
        Polls upstream's getinfo() and detecting new block on the network.
        This will call registry.update_block when new prevhash appear.

        This is just failback alternative when something
        with ./bitcoind -blocknotify will go wrong.
    '''

    def __init__(self, registry, bitcoin_rpc):
        self.bitcoin_rpc = bitcoin_rpc
        self.registry = registry
        self.clock = None
        self.schedule()

    def schedule(self):
        when = self._get_next_time()
        #log.debug("Next prevhash update in %.03f sec" % when)
        #log.debug("Merkle update in next %.03f sec" % \
        #          ((self.registry.last_update + config.MERKLE_REFRESH_INTERVAL)-posix_time()))
        self.clock = reactor.callLater(when, self.run) #@UndefinedVariable

    def _get_next_time(self):
        when = config.PREVHASH_REFRESH_INTERVAL - (posix_time() - self.registry.last_update) % config.PREVHASH_REFRESH_INTERVAL #@UndefinedVariable
        return when

    @defer.inlineCallbacks
    def run(self):
        try:
            template = self.registry.get_last_template()
            if template:
                current_prevhash = "%064x" % template.hashPrevBlock
            else:
                current_prevhash = None

            # Get prevhash
            if config.BITCOIN_VERSION_0_9_PLUS:
                prevhash = yield self.bitcoin_rpc.get_best_block_hash()
            else:
                prevhash = util.reverse_hash((yield self.bitcoin_rpc.prevhash()))

            if prevhash and prevhash != current_prevhash:
                log.info("New block! Prevhash: %s" % prevhash)
                self.registry.update_blank_block(prevhash)

            elif posix_time() - self.registry.last_update >= config.MERKLE_REFRESH_INTERVAL:
                log.info("Merkle update! Prevhash: %s" % prevhash)
                self.registry.update_block()

        except Exception:
            log.exception("UpdateWatchdog.run failed")
        finally:
            self.schedule()


