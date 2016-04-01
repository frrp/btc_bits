# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""An interactive Python interpreter with syntax coloring.

Nothing interesting is actually defined here.  Two listening ports are
set up and attached to protocols which know how to properly set up a
ColoredManhole instance.
"""

from twisted.conch.insults import insults
from twisted.conch.telnet import TelnetTransport, TelnetBootstrapProtocol
from twisted.conch.manhole_ssh import ConchFactory, TerminalRealm

from twisted.internet import protocol
from twisted.application import internet, service
from twisted.cred import checkers, portal
import types


def make_service(args):
    def chain_protocol_factory():
        return insults.ServerProtocol(
            args['protocolFactory'],
            *args.get('protocolArgs', ()),
            **args.get('protocolKwArgs', {}))

    m = service.MultiService()

    # Telnet manhole
    if True:
        f = protocol.ServerFactory()
        f.protocol = lambda: TelnetTransport(TelnetBootstrapProtocol,
                                             insults.ServerProtocol,
                                             args['protocolFactory'],
                                             *args.get('protocolArgs', ()),
                                             **args.get('protocolKwArgs', {}))
        tsvc = internet.TCPServer(args['telnet'], f)  # @UndefinedVariable
        tsvc.setServiceParent(m)

    # SSH manhole
    if False:
        checker = checkers.InMemoryUsernamePasswordDatabaseDontUse(user="password")
        rlm = TerminalRealm()
        rlm.chainedProtocolFactory = chain_protocol_factory
        ptl = portal.Portal(rlm, [checker])
        f = ConchFactory(ptl)
        csvc = internet.TCPServer(args['ssh'], f)  # @UndefinedVariable
        csvc.setServiceParent(m)

    return m


def show(x):
    """Display the data attributes of an object in a readable format"""
    print "data attributes of %r" % (x,)
    names = dir(x)
    maxlen = max([0] + [len(n) for n in names])
    for k in names:
        v = getattr(x,k)
        t = type(v)
        if t == types.MethodType: continue
        if k[:2] == '__' and k[-2:] == '__': continue
        if t is types.StringType or t is types.UnicodeType:
            if len(v) > 80 - maxlen - 5:
                v = `v[:80 - maxlen - 5]` + "..."
        elif t in (types.IntType, types.NoneType):
            v = str(v)
        elif v in (types.ListType, types.TupleType, types.DictType):
            v = "%s (%d elements)" % (v, len(v))
        else:
            v = str(t)
        print "%*s : %s" % (maxlen, k, v)
    return x
