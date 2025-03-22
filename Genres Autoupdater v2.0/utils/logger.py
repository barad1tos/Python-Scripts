#!/usr/bin/env python3

"""
Enhanced Logger Module

Provides a comprehensive logging system with detailed tracking and visual formatting. Features:

1. Run tracking - headers/footers and duration tracking for script executions
2. Log rotation - keeps only the most recent N runs in log files
3. Color coding - visual distinction between log levels using ANSI colors
4. Path aliases - converts absolute paths to readable aliases ($MUSIC, $LOGS, etc.)
5. Multiple loggers - specialized loggers for console output, file logs, and analytics
6. Visual indicators - emojis and separators for improved readability
7. Compact formatting - abbreviated log levels and shortened timestamps
8. HTML report generation - support for both incremental and full analytics reports
"""

import logging
import os
import re
import sys
import time

from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# ANSI escape codes for colors
RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
GRAY = "\033[90m"
BOLD = "\033[1m"

# Level abbreviations and colors
LEVEL_COLORS = {
    'DEBUG': GRAY,
    'INFO': GREEN,
    'WARNING': YELLOW,
    'ERROR': RED,
    'CRITICAL': MAGENTA + BOLD
}

LEVEL_ABBREV = {
    'DEBUG': 'D',
    'INFO': 'I',
    'WARNING': 'W',
    'ERROR': 'E',
    'CRITICAL': 'C'
}

class RunHandler:
    """
    Handles tracking of script runs, adding separators between runs,
    and limiting logs to max number of runs.
    """
    def __init__(self, max_runs: int = 3):
        self.max_runs = max_runs
        self.current_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_start_time = time.time()
    
    def format_run_header(self, logger_name: str) -> str:
        """Creates a formatted header for a new run"""
        return (
            f"\n\n{'='*80}\n"
            f"🚀 NEW RUN: {logger_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'='*80}\n\n"
        )
    
    def format_run_footer(self, logger_name: str) -> str:
        """Creates a formatted footer for the end of a run"""
        elapsed = time.time() - self.run_start_time
        return (
            f"\n\n{'='*80}\n"
            f"🏁 END RUN: {logger_name} - Total time: {elapsed:.2f}s\n"
            f"{'='*80}\n\n"
        )
    
    def trim_log_to_max_runs(self, log_file: str) -> None:
        """
        Trims a log file to contain only the most recent N runs.
        
        Args:
            log_file: Path to the log file
        """
        if not os.path.exists(log_file) or self.max_runs <= 0:
            return
            
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Find run separators
            run_headers = re.findall(r'\n\n={80}\n🚀 NEW RUN:', content)
            
            if len(run_headers) <= self.max_runs:
                return  # No trimming needed
                
            # Find the position of the N-th most recent run
            start_pos = 0
            for i in range(len(run_headers) - self.max_runs):
                match = re.search(r'\n\n={80}\n🚀 NEW RUN:', content[start_pos:])
                if match:
                    start_pos += match.start() + 1
                else:
                    break
            
            # Keep only the most recent runs
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(content[start_pos:])
                
        except Exception as e:
            print(f"Error trimming log file {log_file}: {e}")

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

def shorten_path(path: str, config: dict = None) -> str:
    """
    Shorten a path to make it more readable in logs.
    
    Replaces known base directories with shorter aliases.
    For AppleScripts, extracts just the script name.
    
    Args:
        path (str): The path to shorten
        config (dict, optional): Configuration to extract base paths
        
    Returns:
        str: Shortened, more readable path
    """
    if not path:
        return path
        
    # Extract just the script name for AppleScript calls
    if 'osascript' in path and '.applescript' in path:
        script_match = re.search(r'([^/\\]+\.applescript)', path)
        if script_match:
            return f"script:{script_match.group(1)}"
    
    # Try to use paths from config
    if config:
        apple_scripts_dir = config.get("apple_scripts_dir", "")
        music_library_path = config.get("music_library_path", "")
        logs_base_dir = config.get("logs_base_dir", "")
        
        if apple_scripts_dir and path.startswith(apple_scripts_dir):
            return path.replace(apple_scripts_dir, "$SCRIPTS")
            
        if music_library_path and path.startswith(music_library_path):
            return path.replace(music_library_path, "$MUSIC")
            
        if logs_base_dir and path.startswith(logs_base_dir):
            return path.replace(logs_base_dir, "$LOGS")
    
    # Try common directories if config not provided
    home = str(Path.home())
    if path.startswith(home):
        return path.replace(home, "~")
    
    # If no matches, just return filename for absolute paths
    if os.path.isabs(path):
        return os.path.basename(path)
        
    return path

class CompactFormatter(logging.Formatter):
    """
    Formatter that creates compact, readable log entries.
    - Abbreviates log levels
    - Uses short timestamps
    - Shortens paths
    - Applies colors
    - Supports run tracking
    """
    def __init__(
        self, 
        fmt=None, 
        datefmt='%H:%M:%S', 
        style='%', 
        include_separator=False, 
        config=None,
        run_handler=None
    ):
        """
        Initialize the CompactFormatter.

        Args:
            fmt (str): The format string.
            datefmt (str): The date format string.
            style (str): The style of the format string.
            include_separator (bool): Whether to include a dividing line after log messages.
            config (dict): Configuration dictionary for path shortening.
            run_handler (RunHandler): Handler for tracking script runs.
        """
        super().__init__(fmt, datefmt, style)
        self.include_separator = include_separator
        self.separator_line = f"\n{BLUE}{'-'*50}{RESET}"
        self.config = config
        self.run_handler = run_handler
    
    def format(self, record):
        # Store original values to restore them later
        original_msg = record.msg
        original_levelname = record.levelname
        original_pathname = getattr(record, 'pathname', '')
        original_filename = getattr(record, 'filename', '')
        
        # Apply modifications
        try:
            # Shorten paths in the message if it's a string
            if isinstance(record.msg, str):
                record.msg = re.sub(
                    r'((?:/[^/]+)+/[^/\s:]+\.(?:applescript|py|yaml|csv))', 
                    lambda m: shorten_path(m.group(1), self.config), 
                    record.msg
                )
            
            # Apply level abbreviation with color
            level_color = LEVEL_COLORS.get(record.levelname, '')
            record.levelname = f"{level_color}{LEVEL_ABBREV.get(record.levelname, record.levelname)}{RESET}"
            
            # Format the record
            result = super().format(record)
            
            # Add separator if requested
            if getattr(record, "section_end", False) and self.include_separator:
                result = f"{result}{self.separator_line}"
                
            return result
        finally:
            # Restore original values
            record.msg = original_msg
            record.levelname = original_levelname
            if original_pathname:
                record.pathname = original_pathname
            if original_filename:
                record.filename = original_filename

class RunTrackingHandler(logging.FileHandler):
    """
    A file handler that adds run separation headers and footers
    and can limit the log to a maximum number of runs.
    """
    def __init__(
        self, 
        filename, 
        mode='a', 
        encoding='utf-8', 
        delay=False, 
        run_handler=None,
        logger_name="Logger"
    ):
        super().__init__(filename, mode, encoding, delay)
        self.run_handler = run_handler
        self.logger_name = logger_name
        self.header_written = False
        self._closed = False
        
        # Write run header
        if self.run_handler and os.path.exists(filename):
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(self.run_handler.format_run_header(self.logger_name))
            self.header_written = True
    
    def close(self):
        """Write run footer when handler is closed"""
        if self.run_handler and not self._closed:
            with open(self.baseFilename, 'a', encoding='utf-8') as f:
                f.write(self.run_handler.format_run_footer(self.logger_name))
            
            # Trim log file to max runs if needed
            if hasattr(self.run_handler, 'max_runs') and self.run_handler.max_runs > 0:
                self.run_handler.trim_log_to_max_runs(self.baseFilename)
                
        self._closed = True
        super().close()

    # Add property for compatibility with closed checks
    @property
    def closed(self):
        return self._closed

def get_loggers(config: dict) -> Tuple[logging.Logger, logging.Logger, logging.Logger]:
    """
    Creates and returns three loggers with optimized formatting:
        1) console_logger: Compact format with colors
        2) error_logger: Detailed format with context (now combined with main_logger)
        3) analytics_logger: Detailed format with separators

    Args:
        config (dict): Configuration dictionary.
        
    Returns:
        Tuple of three loggers (console, error, analytics)
    """
    # Ensure base directory and subdirectories exist
    logs_base_dir = config.get("logs_base_dir", ".")
    ensure_directory(logs_base_dir)
    ensure_directory(os.path.join(logs_base_dir, "analytics"))
    ensure_directory(os.path.join(logs_base_dir, "main"))
    
    # Build full paths
    main_log_file = get_full_log_path(config, "main_log_file", "main/main.log")
    analytics_log_file = get_full_log_path(config, "analytics_log_file", "analytics/analytics.log")
    
    # Ensure directories for each file exist
    ensure_directory(os.path.dirname(main_log_file))
    ensure_directory(os.path.dirname(analytics_log_file))
    
    # Create run handler for tracking script runs
    max_runs = config.get("logging", {}).get("max_runs", 3)
    run_handler = RunHandler(max_runs=max_runs)
    
    # Create formatters
    console_formatter = CompactFormatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt='%H:%M:%S',
        include_separator=False,
        config=config,
        run_handler=run_handler
    )
    
    file_formatter = CompactFormatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        include_separator=True,
        config=config,
        run_handler=run_handler
    )
    
    # Initialize console_logger
    console_logger = logging.getLogger("console_logger")
    if not console_logger.handlers:
        console_logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(console_formatter)
        console_logger.addHandler(ch)
        console_logger.propagate = False
    
    # Initialize main logger (now combines error and main loggers)
    main_logger = logging.getLogger("main_logger")
    if not main_logger.handlers:
        main_logger.setLevel(logging.INFO)  # Capture INFO level and above
        
        # Set up run tracking handler
        fh = RunTrackingHandler(
            main_log_file,
            run_handler=run_handler,
            logger_name="Main Logger"
        )
        fh.setFormatter(file_formatter)
        main_logger.addHandler(fh)
        main_logger.propagate = False
    
    # Initialize analytics_logger
    analytics_logger = logging.getLogger("analytics_logger")
    if not analytics_logger.handlers:
        analytics_logger.setLevel(logging.INFO)
        
        analytics_fh = RunTrackingHandler(
            analytics_log_file,
            run_handler=run_handler,
            logger_name="Analytics"
        )
        analytics_fh.setFormatter(file_formatter)
        analytics_logger.addHandler(analytics_fh)
        analytics_logger.propagate = False
    
    # Get error_logger pointing to the same handlers as main_logger
    error_logger = main_logger
    
    return console_logger, error_logger, analytics_logger


def get_html_report_path(config: dict, force_mode: bool = False) -> str:
    """
    Get the path for HTML analytics report based on run mode.
    
    Args:
        config: Configuration dictionary
        force_mode: Whether the script is running in force mode
        
    Returns:
        Path to HTML report file
    """
    logs_base_dir = config.get("logs_base_dir", ".")
    analytics_dir = os.path.join(logs_base_dir, "analytics")
    
    # Ensure directory exists
    ensure_directory(analytics_dir)
    
    # Choose file based on mode
    if force_mode:
        return os.path.join(analytics_dir, "analytics_full.html")
    else:
        return os.path.join(analytics_dir, "analytics_incremental.html")