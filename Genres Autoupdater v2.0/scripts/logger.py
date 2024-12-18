# logger_utils.py
# This module provides two loggers:
# 1. console_logger: log from INFO level and above to the console
# 2. error_logger: log from the ERROR level to the file

import logging
import sys
from typing import Tuple
from logging import Logger
from logging.handlers import RotatingFileHandler

# ANSI escape codes for colors
RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"

class ColoredFormatter(logging.Formatter):
    """
    Custom formatter to add colors to log messages based on their severity level.
    Errors are displayed in red, and INFO messages have a green "OK" status appended.
    """
    def format(self, record):
        original_message = super().format(record)
        if record.levelno >= logging.ERROR:
            # Add red color for error messages
            colored_message = f"{RED}{original_message}{RESET}"
        elif record.levelno == logging.INFO:
            # Append green "OK" status to INFO messages
            colored_message = f"{GREEN}OK{RESET} - {original_message}"
        else:
            colored_message = original_message
        return colored_message

FORMATTER = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s")

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