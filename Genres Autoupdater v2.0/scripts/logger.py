#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logger Module

Provides three loggers:
1) console_logger (INFO+ to console, without dividing line),
2) error_logger (ERROR+ to rotating file),
3) analytics_logger (INFO+ or DEBUG+ to rotating file).

Colors the console output using ANSI escape codes.
Additionally, provides a helper function to ensure a directory exists, and
functions to get the full log path from config.
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
    """
    Ensure that the given directory path exists, creating it if necessary.
    """
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def get_full_log_path(config: dict, key: str, default: str) -> str:
    """
    Returns the full log path by joining the base logs directory with the relative path.

    Args:
        config (dict): Configuration dictionary.
        key (str): The logging key (e.g. 'main_log_file').
        default (str): Default relative path if not found in config.

    Returns:
        str: The absolute path to the log file.
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

def get_loggers(config: dict) -> Tuple[Logger, Logger, Logger]:
    """
    Creates and returns three loggers:
        1) console_logger (INFO+ to console, without dividing line),
        2) error_logger (ERROR+ to rotating file, with dividing line),
        3) analytics_logger (INFO+ to rotating file, with dividing line).

    - The 'main_log_file' from config is used by error_logger.
    - The 'analytics_log_file' from config is used by analytics_logger.
    - The console_logger just writes to stdout at INFO level.

    Paths are built using the 'logs_base_dir' parameter from the configuration.
    """
    # Ensure base directory and subdirectories exist
    logs_base_dir = config.get("logs_base_dir", ".")
    ensure_directory(logs_base_dir)
    # Additionally create subdirectories if necessary
    ensure_directory(os.path.join(logs_base_dir, "analytics"))
    ensure_directory(os.path.join(logs_base_dir, "main"))
    
    # Build full paths
    main_log_file = get_full_log_path(config, "main_log_file", "main/main.log")
    analytics_log_file = get_full_log_path(config, "analytics_log_file", "analytics/analytics.log")
    
    # Ensure directories for each file exist
    ensure_directory(os.path.dirname(main_log_file))
    ensure_directory(os.path.dirname(analytics_log_file))
    
    # Create formatters
    console_formatter = ColoredFormatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        include_separator=False
    )
    file_formatter = ColoredFormatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        include_separator=True
    )
    
    # Initialize console_logger
    console_logger = logging.getLogger("console_logger")
    if not console_logger.handlers:
        console_logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(console_formatter)
        console_logger.addHandler(ch)
        console_logger.propagate = False
    
    # Initialize error_logger (writes only ERROR+ to rotating file)
    error_logger = logging.getLogger("error_logger")
    if not error_logger.handlers:
        error_logger.setLevel(logging.ERROR)
        max_bytes = config.get("logging", {}).get("max_bytes", 5000000)
        backup_count = config.get("logging", {}).get("backup_count", 2)
        fh = RotatingFileHandler(
            main_log_file, maxBytes=max_bytes,
            backupCount=backup_count, encoding="utf-8"
        )
        fh.setFormatter(file_formatter)
        error_logger.addHandler(fh)
        error_logger.propagate = False
    
    # Initialize analytics_logger (writes INFO+ to rotating file)
    analytics_logger = logging.getLogger("analytics_logger")
    if not analytics_logger.handlers:
        analytics_logger.setLevel(logging.INFO)
        max_bytes = config.get("logging", {}).get("max_bytes", 5000000)
        backup_count = config.get("logging", {}).get("backup_count", 2)
        ah = RotatingFileHandler(
            analytics_log_file, maxBytes=max_bytes,
            backupCount=backup_count, encoding="utf-8"
        )
        ah.setFormatter(file_formatter)
        analytics_logger.addHandler(ah)
        analytics_logger.propagate = False
    
    return console_logger, error_logger, analytics_logger