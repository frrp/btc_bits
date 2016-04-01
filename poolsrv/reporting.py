import socket
from poolsrv.queues import DeliveryQueue
from twisted.internet import reactor

from poolsrv.jsonlink import JsonLinkClient


class ReporterJsonLinkClient(JsonLinkClient):

    def __init__(self, conn_made_callback,
                 recv_msg_callback, max_reconnect_delay=5):
        JsonLinkClient.__init__(self, max_reconnect_delay)

        self._received_message = recv_msg_callback
        self._connection_made = conn_made_callback

    def receivedMessage(self, protocol, msg):
        return self._received_message(msg)

    def connectionMade(self, protocol):
        JsonLinkClient.connectionMade(self, protocol)
        # Delegate to the callback
        self._connection_made(protocol)


class JsonReporter(object):

    def __init__(self, host, port,
                 max_queue_len=2048,
                 max_active_queue_len=100,
                 retry_after_s=5,
                 start_suspended=False):

        self._link = ReporterJsonLinkClient(self._connection_made_int,
                                            self._received_message)
        self._queue = DeliveryQueue(self._link.sendMessage,
                                    drop_callback=self.on_drop,
                                    max_queue_len=max_queue_len,
                                    max_active_queue_len=max_active_queue_len,
                                    retry_after_s=retry_after_s,
                                    start_suspended=start_suspended)
        self._connector = reactor.connectTCP(host, port, self._link)

    def redirect(self, host, port):
        self._connector.stopConnecting()
        self._connector.disconnect()
        self._connector = reactor.connectTCP(host, port, self._link)

    def _callback(self, result):
        return True

    def _errorback(self, failure):
        return False

    def _received_message(self, msg):
        return True

    def _connection_made_int(self, protocol):
        try:
            protocol.transport.setTcpNoDelay(True)
            protocol.transport.setTcpKeepAlive(True)
            # Seconds before sending keepalive probes
            protocol.transport.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
            # Interval in seconds between keepalive probes
            protocol.transport.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
            # Failed keepalive probles before declaring other end dead
            protocol.transport.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)
        except:
            pass
        self._connection_made()

    def _connection_made(self):
        """Default implementation does nothing."""

    def deliver(self, event):
        self._queue.deliver(event)

    def report(self, event_name, payload):
        obj = (event_name, payload)
        self._queue.deliver(obj)

    def on_drop(self, obj, reason):
        pass

