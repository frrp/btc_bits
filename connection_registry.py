import weakref
from twisted.internet import reactor
from services import GenericService


class ConnectionRegistry(object):
    __connections = weakref.WeakKeyDictionary()

    @classmethod
    def add_connection(cls, conn):
        cls.__connections[conn] = True

    @classmethod
    def get_connections_count(cls):
        return len(cls.__connections)

    @classmethod
    def remove_connection(cls, conn):
        try:
            del cls.__connections[conn]
        except:
            print "Warning: Cannot remove connection from ConnectionRegistry"

    @classmethod
    def get_session(cls, conn):
        if isinstance(conn, weakref.ref):
            conn = conn()

        if isinstance(conn, GenericService):
            conn = conn.connection_ref()

        if conn is None:
            return None

        return conn.get_session()

    @classmethod
    def iterate(cls):
        return cls.__connections.iterkeyrefs()

    @classmethod
    def get_shallow_copy(cls):
        return cls.__connections.copy()


def dump_connections():
    for x in ConnectionRegistry.iterate():
        c = x()
        c.transport.write('cus')
        print '!!!', c
    reactor.callLater(5, dump_connections)

#reactor.callLater(0, dump_connections)
