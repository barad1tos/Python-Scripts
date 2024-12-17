# logger_utils.py
# This module provides two loggers:
# 1. console_logger: log from INFO level and above to the console
# 2. error_logger: log from the ERROR level to the file

from typing import Tuple
import logging
from logging.handlers import RotatingFileHandler
import sys
from logging import Logger

FORMATTER = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

def get_loggers(log_file: str) -> Tuple[Logger, Logger]:
    # Logger for the console
    console_logger = logging.getLogger("console_logger")
    if not console_logger.handlers:
        console_logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(FORMATTER)
        console_logger.addHandler(ch)
        console_logger.propagate = False

    # Logger for errors to file
    error_logger = logging.getLogger("error_logger")
    if not error_logger.handlers:
        error_logger.setLevel(logging.ERROR)
        fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
        fh.setFormatter(FORMATTER)
        error_logger.addHandler(fh)
        error_logger.propagate = False

    return console_logger, error_logger