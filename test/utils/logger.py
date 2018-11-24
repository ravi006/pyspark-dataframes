"""Log Wrapper Module."""

import logging


class TestLogger:
    """Logger Wrapper for mars."""

    def __init__(self, name):
        """Init a logger."""
        self.logger = logging.getLogger(name)

    def info(self, *args, **kwargs):
        """Wrap original info method."""
        self.logger.info(self.construct_log(args, kwargs))

    def error(self, *args, **kwargs):
        """Wrap original error method."""
        self.logger.error(self.construct_log(args, kwargs))

    def warn(self, *args, **kwargs):
        """Wrap original warn method."""
        self.logger.warn(self.construct_log(args, kwargs))

    def construct_log(self, args, kwargs):
        """Construct a dictionary as a log entry.

        :param args: arguments without keys
        :type args: tuple
        :param kwargs: dictionary that stores key-value pair
        :type args: dictionary
        """
        log_entry = {}

        for ind in xrange(len(args)):
            log_entry[ind] = args[ind]
        for key in kwargs:
            log_entry[key] = kwargs[key]

        return log_entry
