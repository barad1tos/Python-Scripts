#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logger Module

Provides four loggers:
1) console_logger (INFO+ to console, without dividing line),
2) error_logger (ERROR+ to rotating file),
3) analytics_logger (INFO+ or DEBUG+ to rotating file).
4) cleaning_exceptions_logger (for cleaning/exceptions, to rotating file).

Colors the console output using ANSI escape codes.
Additionally, provides a helper function to ensure a directory exists, and a function to get the full log path.
"""

import sys
import os
import logging
from typing import Tuple
from logging import Logger
from logging.handlers import RotatingFileHandler

# ANSI escape codes for colours
RESET = "\033[0m"
RED = "\033[31m"
BLUE = "\033[34m"

def ensure_directory(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def get_full_log_path(config: dict, key: str, default: str) -> str:
    """
    Returns the full log path by joining the base logs directory with the relative path.
    """
    logs_base_dir = config.get("logs_base_dir", ".")
    relative_path = config.get("logging", {}).get(key, default)
    return os.path.join(logs_base_dir, relative_path)

class ColoredFormatter(logging.Formatter):
    """
    Custom formatter with an optional dividing line.
    If the LogRecord has attribute 'section_end' set to True, a blue dividing line is appended.
    """
    def __init__(self, fmt=None, datefmt=None, style='%', include_separator: bool = False):
        super().__init__(fmt, datefmt, style)
        self.include_separator = include_separator
        self.separator_line = f"\n{BLUE}{'-'*80}{RESET}"
    
    def format(self, record):
        original_message = super().format(record)
        if getattr(record, "section_end", False) and self.include_separator:
            return f"{original_message}{self.separator_line}"
        return original_message

def get_loggers(config: dict) -> Tuple[Logger, Logger, Logger, Logger]:
    """
    Creates and returns four loggers:
      1) console_logger (INFO+ to console, without dividing line),
      2) error_logger (ERROR+ to rotating file, with dividing line),
      3) analytics_logger (INFO+ to rotating file, with dividing line),
      4) cleaning_exceptions_logger (for cleaning/exceptions, to rotating file, with dividing line).
    
    Paths are built using the 'logs_base_dir' parameter from the configuration.
    """
    # Ensure base directory and subdirectories exist
    logs_base_dir = config.get("logs_base_dir", ".")
    ensure_directory(logs_base_dir)
    # Additionally create subdirectories, if necessary
    ensure_directory(os.path.join(logs_base_dir, "analytics"))
    ensure_directory(os.path.join(logs_base_dir, "main logic"))
    
    # Build full paths using our helper function
    main_log_file = get_full_log_path(config, "main_log_file", "main logic/main.log")
    analytics_log_file = get_full_log_path(config, "analytics_log_file", "analytics/analytics.log")
    cleaning_exceptions_log_file = get_full_log_path(config, "cleaning_exceptions_log_file", "main logic/cleaning_exceptions.log")
    
    # Ensure directories for each file exist
    ensure_directory(os.path.dirname(main_log_file))
    ensure_directory(os.path.dirname(analytics_log_file))
    ensure_directory(os.path.dirname(cleaning_exceptions_log_file))
    
    # Create formatters
    console_formatter = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s", include_separator=False)
    file_formatter = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s", include_separator=True)
    
    # Initialize console_logger
    console_logger = logging.getLogger("console_logger")
    if not console_logger.handlers:
        console_logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(console_formatter)
        console_logger.addHandler(ch)
        console_logger.propagate = False
    
    # Initialize error_logger
    error_logger = logging.getLogger("error_logger")
    if not error_logger.handlers:
        error_logger.setLevel(logging.ERROR)
        max_bytes = config.get("logging", {}).get("max_bytes", 5000000)
        backup_count = config.get("logging", {}).get("backup_count", 3)
        fh = RotatingFileHandler(main_log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh.setFormatter(file_formatter)
        error_logger.addHandler(fh)
        error_logger.propagate = False
    
    # Initialize analytics_logger
    analytics_logger = logging.getLogger("analytics_logger")
    if not analytics_logger.handlers:
        analytics_logger.setLevel(logging.INFO)
        max_bytes = config.get("logging", {}).get("max_bytes", 5000000)
        backup_count = config.get("logging", {}).get("backup_count", 3)
        ah = RotatingFileHandler(analytics_log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        ah.setFormatter(file_formatter)
        analytics_logger.addHandler(ah)
        analytics_logger.propagate = False
    
    # Initialize cleaning_exceptions_logger
    cleaning_exceptions_logger = logging.getLogger("cleaning_exceptions_logger")
    if not cleaning_exceptions_logger.handlers:
        cleaning_exceptions_logger.setLevel(logging.INFO)
        max_bytes = config.get("logging", {}).get("max_bytes", 5000000)
        backup_count = config.get("logging", {}).get("backup_count", 3)
        ceh = RotatingFileHandler(cleaning_exceptions_log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        ceh.setFormatter(file_formatter)
        cleaning_exceptions_logger.addHandler(ceh)
        cleaning_exceptions_logger.propagate = False

    return console_logger, error_logger, analytics_logger, cleaning_exceptions_logger