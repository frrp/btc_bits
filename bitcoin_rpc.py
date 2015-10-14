'''
    Implements simple interface to bitcoind's RPC.
'''

import ujson as json
import base64
from twisted.internet import defer, reactor
from twisted.web.client import HTTPClientFactory, _parse
import re
import poolsrv.logger
log = poolsrv.logger.get_logger()


def getPage(url, *args, **kwargs):
    scheme, host, port, path = _parse(url)
    factory = HTTPClientFactory(url, *args, **kwargs)
    connector = reactor.connectTCP(host, port, factory)
    return (connector, factory)

class BitcoinRPC(object):

    def __init__(self, host, port, username, password):
        self.bitcoin_url = 'http://%s:%d' % (host, port)
        self.credentials = base64.b64encode("%s:%s" % (username, password))
        self.headers = {
            'Content-Type': 'text/json',
            'Authorization': 'Basic %s' % self.credentials,
        }

    def _call_raw(self, data):
        return getPage(
            url=self.bitcoin_url,
            method='POST',
            headers=self.headers,
            postdata=data,
        )

    def _call(self, method, params):
        return self._call_raw(json.dumps({
                'jsonrpc': '2.0',
                'method': method,
                'params': params,
                'id': '1',
            }))

    @defer.inlineCallbacks
    def submitblock(self, block_hex):
        connector, factory = self._call('submitblock', [block_hex,])
        resp = (yield factory.deferred)
        connector.transport = None
        if json.loads(resp)['result'] == None:
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def getinfo(self):
        connector, factory = self._call('getinfo', [])
        resp = (yield factory.deferred)
        connector.transport = None
        defer.returnValue(json.loads(resp)['result'])

    @defer.inlineCallbacks
    def getblocktemplate(self):
        connector, factory = self._call('getblocktemplate', [])
        resp = (yield factory.deferred)
        connector.transport = None
        defer.returnValue(json.loads(resp)['result'])

    @defer.inlineCallbacks
    def prevhash(self):
        """" THIS IS UNUSABLE FROM v.0.9.x !!! """
        connector, factory = self._call('getwork', [])
        resp = (yield factory.deferred)
        connector.transport = None

        try:
            defer.returnValue(json.loads(resp)['result']['data'][8:72])
        except Exception as e:
            log.exception("Cannot decode prevhash %s" % str(e))
            raise

    @defer.inlineCallbacks
    def get_best_block_hash(self):
        """" MUST BE USED FROM v.0.9.x !!! """
        connector, factory = self._call('getblockchaininfo', [])
        resp = (yield factory.deferred)
        connector.transport = None

        try:
            best_hash = json.loads(resp)['result']['bestblockhash']
            if not re.match('^[0-9a-f]{64}$', best_hash):
                raise Exception("Invalid response for prevhash: %s" % best_hash)
            defer.returnValue(best_hash)
        except Exception as e:
            log.exception("Cannot decode prevhash %s" % str(e))
            raise

    @defer.inlineCallbacks
    def validateaddress(self, address):
        connector, factory = self._call('validateaddress', [address,])
        resp = (yield factory.deferred)
        connector.transport = None
        defer.returnValue(json.loads(resp)['result'])
