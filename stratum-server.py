from poolsrv import config

SERVER = None

def setup(setup_event=None):
    from twisted.application import service
    application = service.Application("stratum-server")

    if setup_event == None:
        setup_finalize(None, application)
    else:
        setup_event.addCallback(setup_finalize, application)

    return application

def setup_finalize(event, application):
    global SERVER

    from twisted.application import internet
    from twisted.internet import reactor

    from mining.service import MiningServiceEventHandler

    import socket_transport

    # Set up thread pool size for service threads
    reactor.suggestThreadPoolSize(config.THREAD_POOL_SIZE)

    # Attach Socket Transport service to application
    SERVER = internet.TCPServer(config.LISTEN_SOCKET_TRANSPORT,
                                socket_transport.SocketTransportFactory(debug=config.DEBUG,
                                                                        event_handler=MiningServiceEventHandler,
                                                                        tcp_proxy_protocol_enable=config.TCP_PROXY_PROTOCOL))
    SERVER.setServiceParent(application)

    return event

if __name__ == '__main__':
    print "This is not executable script. Try 'twistd -ny launcher.tac instead!"
