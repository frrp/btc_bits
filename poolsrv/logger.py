'''Simple wrapper around python's logging package'''

import os
import logging
import inspect

_stream_handler = None
_file_handler = None
_log_level = None


def get_logger(name=None):

    if name is None:
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        name = mod.__name__.split('.')[-1]

    if _stream_handler is None or _log_level is None:
        raise Exception("Logging not initialized, call init_logging")

    logger = logging.getLogger(name)
    logger.addHandler(_stream_handler)
    logger.setLevel(getattr(logging, _log_level))

    if _file_handler != None:
        logger.addHandler(_file_handler)

    logger.debug("Logging initialized")
    return logger


def init_logging(log_file, log_dir, log_level, debug=False):
    global _stream_handler, _file_handler, _log_level

    _log_level = log_level

    if debug:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(module)s.%(funcName)s # %(message)s")
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s # %(message)s")

    if log_file != None and log_dir != None:
        _file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
        _file_handler.setFormatter(fmt)

    _stream_handler = logging.StreamHandler()
    _stream_handler.setFormatter(fmt)
