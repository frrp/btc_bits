
from time import time as posix_time

def posix_secs():
    return int(posix_time())

__all__ = ['posix_time', 'posix_secs']
