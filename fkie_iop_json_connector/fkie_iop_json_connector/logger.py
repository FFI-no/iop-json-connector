# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************


ROS_LOGGER = False
import logging
from logging import Logger
try:
    import rclpy
    ROS_LOGGER = True
except Exception:
    pass


# Workaround for logging with and without ROS
# see https://github.com/ros/ros_comm/issues/1384
class MyLogger:
    def __init__(self, name, loglevel='info'):
        self._ros_logger = False
        global ROS_LOGGER
        if ROS_LOGGER:
            self.logger = logging.getLogger('rosout.%s' % name)
            self.debug('Use ROS logger')
        else:
            self.logger = logging.getLogger(name)
        level = self.str2level(loglevel)
        self.logger.setLevel(level)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)

    def level(self):
        return self.level2str(self.logger.level)

    @classmethod
    def setAllLoglevel(cls, loglevel):
        level = cls.str2level(loglevel)
        for _lname, logger in Logger.manager.loggerDict.items():
            if not hasattr(logger, 'setLevel'):
                continue
            logger.setLevel(level=level)

    @classmethod
    def str2level(cls, loglevel):
        result = logging.INFO
        if loglevel == 'debug':
            result = logging.DEBUG
        elif loglevel == 'info':
            result = logging.INFO
        elif loglevel == 'warning':
            result = logging.WARNING
        elif loglevel == 'error':
            result = logging.ERROR
        elif loglevel == 'critical':
            result = logging.CRITICAL
        return result

    @classmethod
    def level2str(cls, loglevel):
        result = 'info'
        if loglevel == logging.DEBUG:
            result = 'debug'
        elif loglevel == logging.INFO:
            result = 'info'
        elif loglevel == logging.WARNING:
            result = 'warning'
        elif loglevel == logging.ERROR:
            result = 'error'
        elif loglevel == logging.CRITICAL:
            result = 'critical'
        return result


