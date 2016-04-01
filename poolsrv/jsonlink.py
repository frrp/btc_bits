import ujson as json

from twisted.internet import defer
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.protocol import ServerFactory
from twisted.protocols.basic import NetstringReceiver

import weakref
import time

MAX_RECONNECT_DELAY_DEFAULT = 10
JSON_DOUBLE_PRECISION = 6


class JsonLinkClient(ReconnectingClientFactory):

    def __init__(self, max_reconnect_delay=MAX_RECONNECT_DELAY_DEFAULT):
        self._protocol = None
        self.maxDelay = max_reconnect_delay

    def isConnected(self):
        return self._protocol is not None

    def buildProtocol(self, addr):
        p = JsonLinkProtocol(self.receivedMessage)
        p._factory = self
        return p

    def receivedMessage(self, protocol, msg):
        return True

    def sendMessage(self, msg):
        if not self._protocol:
            return False

        return self._protocol.sendMessage(msg)

    def connectionMade(self, protocol):
        self._protocol = protocol

    def connectionLost(self, protocol, reason):
        if self._protocol is protocol:
            self._protocol = None


class JsonLinkServerFactory(ServerFactory):

    def __init__(self):
        self._protocols = weakref.WeakKeyDictionary()

    def buildProtocol(self, addr):
        p = JsonLinkProtocol(self.receivedMessage)
        p._factory = self
        return p

    def receivedMessage(self, protocol, msg):
        return True

    def connectionMade(self, protocol):
        self._protocols[protocol] = True

    def connectionLost(self, protocol, reason):
        del self._protocols[protocol]


class JsonLinkProtocol(NetstringReceiver):

    def __init__(self, receiver):
        self._queue = {}
        self._receiver = receiver
        self._closed = False
        self._last_id = 0

        self._connected_at = 0
        self._received_msgs = 0
        self._received_payload_size = 0
        self._sent_msgs = 0
        self._sent_payload_size = 0

    def get_stats(self):
        now = time.time()
        return (self._received_msgs,
                self._received_payload_size,
                self._sent_msgs,
                self._sent_payload_size,
                int(self._received_payload_size / (now - self._connected_at)),
                int(self._sent_payload_size / (now - self._connected_at)))

    def connectionLost(self, reason):
        self._closed = True
        # Cancel all messages waiting for confirmations
        for d in self._queue.itervalues():
            d.callback(False)
        self._factory.connectionLost(self, reason)
        self.transport = None

    def connectionMade(self):
        self._factory.connectionMade(self)
        self._connected_at = time.time()

    def stringReceived(self, line):
        self._received_msgs += 1
        self._received_payload_size += len(line)

        data = json.loads(line)
        first = data[0]
        second = data[1]

        if first == 'ACK':
            self._handle_ack(second)
        elif first == 'ERR':
            self._handle_err(second)
        elif isinstance(first, int):
            self._handle_message(first, second)
        else:
            self._handle_exception(second, data[2])

    def _handle_ack(self, id):
        self._queue[id].callback(True)
        del self._queue[id]

    def _handle_err(self, id):
        self._queue[id].callback(False)
        del self._queue[id]

    def _handle_exception(self, id, reason):
        self._queue[id].errback(reason)
        del self._queue[id]

    def _handle_message(self, id, payload):
        try:
            result = self._receiver(self, payload)
        except BaseException, e:
            self._send_exception(id, e)
        else:
            # Send callback directly
            self._message_callback(result, id)

    def _message_callback(self, result, id):
        # We received deferred
        if isinstance(result, defer.Deferred):
            self._install_callbacks(result, id)
        elif isinstance(result, bool):
            if result:
                self._send_ack(id)
            else:
                self._send_err(id)
        else:
            self._send_exception(id, result)

    def _install_callbacks(self, d, id):
        args = (id,)
        d.addCallbacks(self._message_callback, self._message_callback,
                       callbackArgs=args, errorbackArgs=args)

    def _send_err(self, id):
        self._send_direct(('ERR', id))

    def _send_ack(self, id):
        self._send_direct(('ACK', id))

    def _send_exception(self, id, reason):
        self._send_direct(('EXC', id, str(reason)))

    def _send_direct(self, response):
        payload = json.dumps(response, double_precision=JSON_DOUBLE_PRECISION)

        self.sendString(payload)

        self._sent_msgs += 1
        self._sent_payload_size += len(payload)

    def sendMessage(self, msg, response=True):
        if self._closed:
            return False

        if not response:
            self._send_direct((0, msg))
            return None
        else:
            self._last_id += 1
            # Use compact JSON encoding
            payload = json.dumps((self._last_id, msg), double_precision=JSON_DOUBLE_PRECISION)

            d = defer.Deferred()
            self._queue[self._last_id] = d
            self.sendString(payload)

            self._sent_msgs += 1
            self._sent_payload_size += len(payload)

            return d
