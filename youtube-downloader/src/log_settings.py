# -*- coding: utf-8 -*-
"""
Logging settings
"""

import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler


loggers = {}

LOG_ENABLED = True  # enable logging
LOG_TO_CONSOLE = True  # output to console
LOG_TO_FILE = True  # output to file

LOG_DIR = 'logs'  # save log path
LOG_LEVEL = 'DEBUG'  # log level
LOG_FORMAT = '%(levelname)s-%(asctime)s-%(module)s-%(lineno)d - %(message)s'  # log format


def get_logger(name=None):
    """
    get logger by name
    :param name: name of logger
    :return: logger
    """
    global loggers

    if not name:
        name = __name__

    if loggers.get(name):
        return loggers.get(name)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # output to console
    if LOG_ENABLED and LOG_TO_CONSOLE:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # output to file
    if LOG_ENABLED and LOG_TO_FILE:
        # if path not exist, make directory
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        # 添加 FileHandler
        log_file_path = os.path.join(LOG_DIR, "{}.log".format(name))
        file_handler = TimedRotatingFileHandler(log_file_path, when='D', backupCount=7, encoding='utf-8')
        file_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # save to global loggers
    loggers[name] = logger
    return logger


if __name__ == '__main__':
    pass
