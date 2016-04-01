try:
    from twisted.internet import epollreactor
    epollreactor.install()
except ImportError:
    raise Exception("Failed to install epoll reactor")

