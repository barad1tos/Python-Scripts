"""
Logger Utilities

Provides three loggers:
1) console_logger (INFO+ to console),
2) error_logger (ERROR+ to rotating file),
3) analytics_logger (INFO+ or DEBUG+ to rotating file).
"""

import logging
import sys
import os
from typing import Tuple
from logging import Logger
from logging.handlers import RotatingFileHandler

# ANSI escape codes for colors
RESET = "\033[0m"
RED = "\033[31m"


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter to colorize log messages.
    Errors in red, others in normal color.
    """
    def format(self, record):
        original_message = super().format(record)
        if record.levelno >= logging.ERROR:
            return f"{RED}{original_message}{RESET}"
        return original_message


# Common formatter for console and files
FORMATTER = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s")


def get_loggers(log_file: str, analytics_log_file: str = None) -> Tuple[Logger, Logger, Logger]:
    """
    Create and return three loggers:
      1) console_logger (INFO+ to console),
      2) error_logger (ERROR+ to rotating file),
      3) analytics_logger (DEBUG+ to rotating file or console).

    :param log_file: Path to main log file.
    :param analytics_log_file: Path to analytics log file (optional).
    :return: (console_logger, error_logger, analytics_logger).
    """
    console_logger = logging.getLogger("console_logger")
    if not console_logger.handlers:
        console_logger.setLevel(logging.INFO)
        ch_stream = logging.StreamHandler(sys.stdout)
        ch_stream.setFormatter(FORMATTER)
        console_logger.addHandler(ch_stream)
        console_logger.propagate = False


    error_logger = logging.getLogger("error_logger")
    if not error_logger.handlers:
        error_logger.setLevel(logging.ERROR)
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
        fh.setFormatter(FORMATTER)
        error_logger.addHandler(fh)
        error_logger.propagate = False

    analytics_logger = logging.getLogger("analytics_logger")
    if not analytics_logger.handlers:
        # Use DEBUG or INFO as you prefer for analytics
        analytics_logger.setLevel(logging.INFO)
        if analytics_log_file:
            os.makedirs(os.path.dirname(analytics_log_file), exist_ok=True)
            ah = RotatingFileHandler(analytics_log_file, maxBytes=5_000_000, backupCount=3)
            ah.setFormatter(FORMATTER)
            analytics_logger.addHandler(ah)
        else:
            analytics_logger.addHandler(logging.StreamHandler(sys.stdout))

        analytics_logger.propagate = False

    return console_logger, error_logger, analytics_logger