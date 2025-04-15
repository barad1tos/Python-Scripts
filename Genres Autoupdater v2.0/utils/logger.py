#!/usr/bin/env python3
# utils/logger.py
"""
Enhanced Logger Module using QueueHandler for non-blocking file IO.

Provides a comprehensive logging system with detailed tracking and visual formatting. Features:

1.  **Run Tracking:** Adds headers/footers with timestamps and duration for script executions (`RunHandler`).
2.  **Log Rotation by Runs:** Keeps only the most recent N runs in log files (`RunHandler.trim_log_to_max_runs`).
3.  **Color Coding:** Uses ANSI colors for visual distinction between log levels in console output (`CompactFormatter`).
4.  **Path Aliases:** Shortens file paths in log messages to readable aliases like `$SCRIPTS`, `$LOGS`, `$MUSIC_LIB`, `~` (`shorten_path`).
5.  **Multiple Loggers:** Configures distinct loggers for console output, main application/error logging, and analytics (`get_loggers`).
6.  **Non-Blocking File Logging:** Employs `QueueHandler` and `QueueListener` to prevent file I/O from blocking the main application thread.
7.  **Visual Indicators:** Uses emojis (🚀, 🏁) and separators for improved readability in log files and console.
8.  **Compact Formatting:** Provides a `CompactFormatter` with abbreviated log levels and short timestamps for console.
9.  **HTML Report Path:** Includes a helper function (`get_html_report_path`) to determine paths for analytics reports.
10. **Configuration Driven:** Relies on a configuration dictionary for paths, log levels, and other settings.
"""

import logging
import os
import queue
import re
import sys
import time

from datetime import datetime
from logging.handlers import QueueHandler, QueueListener

# --------------------------------------
from pathlib import Path
from typing import Dict, Optional, Tuple

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
LEVEL_COLORS = {'DEBUG': GRAY, 'INFO': GREEN, 'WARNING': YELLOW, 'ERROR': RED, 'CRITICAL': MAGENTA + BOLD}
LEVEL_ABBREV = {'DEBUG': 'D', 'INFO': 'I', 'WARNING': 'W', 'ERROR': 'E', 'CRITICAL': 'C'}


class RunHandler:
    """
    Handles tracking of script runs, adding separators between runs,
    and limiting logs to max number of runs.
    """

    def __init__(self, max_runs: int = 3):
        """
        Initializes the RunHandler.

        Args:
            max_runs (int): The maximum number of runs to keep in the log file.
        """
        self.max_runs = max_runs
        self.current_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_start_time = time.monotonic()  # Use monotonic for duration

    def format_run_header(self, logger_name: str) -> str:
        """Creates a formatted header for a new run"""
        # Use UTC time for logs potentially? Or keep local time? Keep local for now.
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header = f"\n\n{'='*80}\n"
        header += f"🚀 NEW RUN: {logger_name} - {now_str}\n"
        header += f"{'='*80}\n\n"
        return header

    def format_run_footer(self, logger_name: str) -> str:
        """Creates a formatted footer for the end of a run"""
        elapsed = time.monotonic() - self.run_start_time
        footer = f"\n\n{'='*80}\n"
        footer += f"🏁 END RUN: {logger_name} - Total time: {elapsed:.2f}s\n"
        footer += f"{'='*80}\n\n"
        return footer

    def trim_log_to_max_runs(self, log_file: str) -> None:
        """
        Trims a log file to contain only the most recent N runs, identified by run headers.

        Args:
            log_file (str): Path to the log file.
        """
        if not os.path.exists(log_file) or self.max_runs <= 0:
            return

        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                # Read lines efficiently, especially for potentially large files
                lines = f.readlines()

            # Find indices of run headers
            header_indices = [
                i
                for i, line in enumerate(lines)
                if re.match(r'^={80}$', line.strip()) and i + 1 < len(lines) and re.match(r'^🚀 NEW RUN:', lines[i + 1].strip())
            ]

            if len(header_indices) <= self.max_runs:
                return  # No trimming needed

            # Keep only the last 'max_runs' sections
            start_line_index = header_indices[-self.max_runs]  # Index of the oldest header to keep

            with open(log_file, 'w', encoding='utf-8') as f:
                f.writelines(lines[start_line_index:])
            # print(f"Trimmed log file {log_file}, kept last {self.max_runs} runs.") # Optional debug print

        except Exception as e:
            # Use print for errors during logging setup/teardown as logger might not be available
            print(f"Error trimming log file {log_file}: {e}", file=sys.stderr)


def ensure_directory(path: str) -> None:
    """
    Ensure that the given directory path exists, creating it if necessary.
    Handles potential race conditions during creation.
    """
    try:
        if path and not os.path.exists(path):  # Check if path is not empty
            os.makedirs(path, exist_ok=True)
    except OSError as e:
        # Handle potential errors during directory creation
        print(f"Error creating directory {path}: {e}", file=sys.stderr)
    except Exception as e:  # Catch other potential errors like permission issues
        print(f"Unexpected error ensuring directory {path}: {e}", file=sys.stderr)


def get_full_log_path(config: dict, key: str, default: str) -> str:
    """
    Returns the full log path by joining the base logs directory with the relative path.
    Ensures the base directory exists.

    Args:
        config (dict): Configuration dictionary.
        key (str): The logging key within the 'logging' section (e.g., 'main_log_file').
        default (str): Default relative path if the key is not found in config.

    Returns:
        str: The absolute path to the log file or directory. Returns default if config is invalid.
    """
    if not isinstance(config, dict):
        return default  # Safety check

    logs_base_dir = config.get("logs_base_dir", ".")
    # Ensure the base directory exists when path is calculated
    ensure_directory(logs_base_dir)

    logging_config = config.get("logging", {})
    if not isinstance(logging_config, dict):
        return os.path.join(logs_base_dir, default)  # Safety

    relative_path = logging_config.get(key, default)
    return os.path.join(logs_base_dir, relative_path)


def shorten_path(path: str, config: dict = None) -> str:
    """
    Shorten a file path for more readable log output.

    Replaces known base directories (from config or common locations) with aliases.
    Extracts script names for AppleScript calls.

    Args:
        path (str): The file path string to shorten.
        config (dict, optional): Configuration dictionary to get base paths.

    Returns:
        str: Shortened path string.
    """
    if not path or not isinstance(path, str):
        return str(path)  # Return original if None or not a string

    # Handle AppleScript calls specifically
    if 'osascript' in path and '.applescript' in path:
        script_match = re.search(r'([^/\\]+\.applescript)', path)
        if script_match:
            return f"script:{script_match.group(1)}"

    # Use config for path replacements if available
    if config and isinstance(config, dict):
        # Use os.path.normpath for consistent separator handling
        norm_path = os.path.normpath(path)
        # Use get_full_log_path helper to ensure consistency if possible, or direct config access
        scripts_dir = os.path.normpath(config.get("apple_scripts_dir", ""))
        music_lib = os.path.normpath(config.get("music_library_path", ""))
        logs_dir = os.path.normpath(config.get("logs_base_dir", ""))

        if scripts_dir and norm_path.startswith(scripts_dir + os.sep):
            # Use basename to just get the script file name
            return f"$SCRIPTS{os.sep}{os.path.basename(norm_path)}"
            # return norm_path.replace(scripts_dir, "$SCRIPTS", 1) # Old way
        if logs_dir and norm_path.startswith(logs_dir + os.sep):
            return norm_path.replace(logs_dir, "$LOGS", 1)
        # Music library path might be a file, check startswith carefully
        if music_lib and norm_path.startswith(music_lib):
            return "$MUSIC_LIB"  # Just replace the whole thing for brevity

    # Fallback to common directory replacements
    try:
        home = str(Path.home())
        norm_home = os.path.normpath(home)
        if norm_path.startswith(norm_home + os.sep):
            return norm_path.replace(norm_home, "~", 1)
    except Exception:
        pass  # Ignore errors getting home directory

    # If it's an absolute path not matched above, return just the filename
    if os.path.isabs(norm_path):
        return os.path.basename(norm_path)

    # Otherwise, return the path as is (likely relative)
    return path


class CompactFormatter(logging.Formatter):
    """
    Custom log formatter for compact, readable, colored output.
    Features: Level abbreviation, short timestamps, path shortening, color coding.
    """

    def __init__(self, fmt=None, datefmt='%H:%M:%S', style='%', include_separator=False, config=None, run_handler=None):
        """
        Initialize the CompactFormatter.

        Args:
            fmt (str, optional): Log record format string. Defaults to None.
            datefmt (str, optional): strftime format for timestamps. Defaults to '%H:%M:%S'.
            style (str, optional): Format string style ('%', '{', '$'). Defaults to '%'.
            include_separator (bool, optional): Whether to add a visual separator line. Defaults to False.
            config (dict, optional): Application config for path shortening. Defaults to None.
            run_handler (RunHandler, optional): RunHandler instance (unused directly here). Defaults to None.
        """
        # Provide a default format string if None is given
        if fmt is None:
            fmt = "%(asctime)s %(levelname)s %(message)s"
        super().__init__(fmt, datefmt, style)
        self.include_separator = include_separator
        self.separator_line = f"\n{BLUE}{'-'*60}{RESET}"  # Slightly shorter separator
        self.config = config or {}  # Ensure config is at least an empty dict
        # run_handler is not used directly in format() but kept for consistency

    def format(self, record: logging.LogRecord) -> str:
        """
        Formats the LogRecord into a compact, colored string.
        Includes path shortening applied *after* standard formatting.
        """
        # Store original level name for color lookup
        original_levelname = record.levelname
        # Abbreviate level name for display
        record.levelname = LEVEL_ABBREV.get(original_levelname, original_levelname[:1])  # Fallback to first letter

        # Apply color based on original level
        level_color = LEVEL_COLORS.get(original_levelname, '')
        # Add color codes to the abbreviated level name
        record.levelname = f"{level_color}{record.levelname}{RESET}"

        # --- Perform Standard Formatting FIRST ---
        # Let the parent class handle the core formatting using original msg and args
        try:
            # This ensures getMessage() is called correctly before path shortening
            formatted = super().format(record)
        except Exception as format_error:
            # If the *initial* formatting fails (e.g., bad log call like logger.info("%s", val1, val2)),
            # log the error and return a basic representation.
            print(f"CRITICAL LOGGING ERROR during initial format: {format_error}", file=sys.stderr)
            print(f"Original Log Record: msg='{record.msg}', args={record.args}", file=sys.stderr)
            # Restore original level name before returning basic info
            record.levelname = original_levelname
            return f"FORMATTING ERROR: {record.getMessage()}"  # Return basic formatted message if possible

        # --- Path Shortening on the *Formatted* String ---
        try:
            # Use regex to find potential paths in the already formatted string
            # Adjusted pattern to be less greedy and potentially safer
            path_pattern = r'((?:[A-Za-z]:|\$|\.|~)?(?:[\\/][\w\.\-\s~]+)+|(?:\.\/)[\w\.\-\s~]+)'
            # Apply shorten_path to matched potential paths within the formatted string
            # Check if formatted is actually a string before applying re.sub
            if isinstance(formatted, str):
                formatted = re.sub(path_pattern, lambda match: shorten_path(match.group(0), self.config), formatted)
        except Exception as shorten_error:
            # Log error during path shortening, but keep the original formatted message
            print(f"Error during log path shortening: {shorten_error}", file=sys.stderr)
            # Optionally log this error using a basic logger configuration if available

        # Restore original levelname on the record object after formatting is done
        # This is important if the record object is reused or inspected elsewhere
        record.levelname = original_levelname

        # Add separator line if requested
        if getattr(record, "section_end", False) and self.include_separator:
            formatted += self.separator_line

        return formatted


class RunTrackingHandler(logging.FileHandler):
    """
    A file handler that adds run separation headers/footers and trims the log
    to a maximum number of runs based on headers.
    """

    def __init__(self, filename, mode='a', encoding='utf-8', delay=False, run_handler=None, logger_name="Logger"):
        """
        Initialize the handler.

        Args:
            filename (str): The log file path.
            mode (str): File open mode. Defaults to 'a'.
            encoding (str, optional): File encoding. Defaults to 'utf-8'.
            delay (bool): If True, file opening is deferred until the first emit(). Defaults to False.
            run_handler (RunHandler, optional): Instance for run tracking logic. Defaults to None.
            logger_name (str): Name used in header/footer. Defaults to "Logger".
        """
        # Ensure directory exists before initializing FileHandler
        ensure_directory(os.path.dirname(filename))
        super().__init__(filename, mode, encoding, delay)
        self.run_handler = run_handler
        self.logger_name = logger_name
        self._header_written = False  # Track if header for the *current* run has been written

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a record. Writes the run header before the first record of a new run.
        """
        if self.run_handler and not self._header_written:
            try:
                # Write header if it hasn't been written for this instance/run
                header = self.run_handler.format_run_header(self.logger_name)
                self.stream.write(header)
                self._header_written = True
            except Exception:
                self.handleError(record)  # Use standard error handling

        # Emit the actual record using the parent class method
        super().emit(record)

    def close(self) -> None:
        """
        Closes the stream, writes run footer, and trims the log file.
        """
        # Check if already closed to prevent recursion or multiple writes
        if self.stream is None or getattr(self, "_closed", False):
            return

        try:
            if self.run_handler:
                # Write footer before closing
                footer = self.run_handler.format_run_footer(self.logger_name)
                if self.stream and hasattr(self.stream, 'write'):
                    self.stream.write(footer)
                    self.flush()  # Ensure footer is written

        except Exception:
            # Handle errors during footer writing
            # Cannot use handleError as stream might be closing
            pass  # Silently ignore footer errors on close? Or print to stderr?

        finally:
            # Close the stream using parent method *before* trimming
            # Set closed flag early to prevent issues during close()
            self._closed = True
            super().close()

            # Trim log file *after* closing the stream
            if self.run_handler and hasattr(self.run_handler, 'max_runs') and self.run_handler.max_runs > 0:
                try:
                    self.run_handler.trim_log_to_max_runs(self.baseFilename)
                except Exception as trim_e:
                    print(f"Error trimming log file {self.baseFilename} after close: {trim_e}", file=sys.stderr)


def get_loggers(config: dict) -> Tuple[logging.Logger, logging.Logger, logging.Logger, Optional[QueueListener]]:
    """
    Creates and returns loggers using QueueHandler for non-blocking file logging.

    Args:
        config (dict): Configuration dictionary.

    Returns:
        Tuple[logging.Logger, logging.Logger, logging.Logger, Optional[QueueListener]]:
            Tuple containing (console_logger, error_logger, analytics_logger, queue_listener).
            The queue_listener object needs to be stopped explicitly on application shutdown.
            Returns None for listener if file logging setup fails.
    """
    listener: Optional[QueueListener] = None  # Initialize listener
    configured_loggers: Dict[str, logging.Logger] = {}  # Track configured loggers

    try:
        # --- Ensure Base Directories Exist ---
        logs_base_dir = config.get("logs_base_dir", ".")
        ensure_directory(logs_base_dir)  # Ensure base first

        # Determine log file paths using helper
        logging_config = config.get("logging", {})
        main_log_file = get_full_log_path(config, "main_log_file", "main/main.log")
        analytics_log_file = get_full_log_path(config, "analytics_log_file", "analytics/analytics.log")

        # Ensure specific log directories exist (get_full_log_path might not create subdirs)
        ensure_directory(os.path.dirname(main_log_file))
        ensure_directory(os.path.dirname(analytics_log_file))

        # --- Common Setup ---
        max_runs = logging_config.get("max_runs", 3)
        run_handler_instance = RunHandler(max_runs=max_runs)  # Single instance for run tracking

        # --- Formatters ---
        console_formatter = CompactFormatter("%(asctime)s %(levelname)s %(message)s", datefmt='%H:%M:%S', config=config)
        file_formatter = CompactFormatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S',
            include_separator=False,
            config=config,  # Separators added by RunTrackingHandler header/footer
        )

        # --- Console Logger (Direct Handling) ---
        console_logger = logging.getLogger("console_logger")
        if not console_logger.handlers:  # Configure only once
            console_logger.setLevel(logging.INFO)  # Set level (consider reading from config)
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(console_formatter)
            console_logger.addHandler(ch)
            console_logger.propagate = False  # Prevent duplicate messages in root logger
        configured_loggers["console"] = console_logger

        # --- File Logging Setup (Using Queue) ---
        log_queue = queue.Queue(-1)  # Create the queue

        # Create the actual file handlers that the listener will use
        main_fh = RunTrackingHandler(main_log_file, mode='a', encoding='utf-8', run_handler=run_handler_instance, logger_name="Main Logger")
        main_fh.setFormatter(file_formatter)
        main_fh.setLevel(logging.INFO)  # Filter level at the handler

        analytics_fh = RunTrackingHandler(analytics_log_file, mode='a', encoding='utf-8', run_handler=run_handler_instance, logger_name="Analytics")
        analytics_fh.setFormatter(file_formatter)
        analytics_fh.setLevel(logging.INFO)  # Filter level at the handler

        # --- Queue Listener (Background Thread) ---
        # Pass all file handlers to the listener
        listener = QueueListener(log_queue, main_fh, analytics_fh, respect_handler_level=True)
        listener.start()
        print("QueueListener started.", file=sys.stderr)  # Use print for setup logs

        # --- Queue Handler (Used by Loggers) ---
        # This handler puts records into the queue
        queue_handler = QueueHandler(log_queue)

        # --- Configure Main/Error Logger ---
        main_logger = logging.getLogger("main_logger")
        if not main_logger.handlers:  # Configure only once
            main_logger.addHandler(queue_handler)
            main_logger.setLevel(logging.INFO)  # Logger level must be <= handler level to pass messages
            main_logger.propagate = False
        error_logger = main_logger  # Alias error logger to main logger
        configured_loggers["main"] = main_logger
        configured_loggers["error"] = error_logger

        # --- Configure Analytics Logger ---
        analytics_logger = logging.getLogger("analytics_logger")
        if not analytics_logger.handlers:  # Configure only once
            analytics_logger.addHandler(queue_handler)
            analytics_logger.setLevel(logging.INFO)  # Logger level
            analytics_logger.propagate = False
        configured_loggers["analytics"] = analytics_logger

        # Optionally configure root logger to use queue as well (catches other module logs)
        # Be cautious with this, might log too much if libraries are verbose
        # root = logging.getLogger()
        # if not any(isinstance(h, QueueHandler) for h in root.handlers):
        #    root.addHandler(queue_handler)
        #    root.setLevel(logging.WARNING) # Set root level higher

        console_logger.debug("Logging setup with QueueListener complete.")
        return console_logger, error_logger, analytics_logger, listener

    except Exception as e:
        # Fallback to basic config if custom setup fails
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        # Log the error using the fallback basic config
        logging.critical(f"Failed to configure custom logging with QueueListener: {e}", exc_info=True)
        # Return basic loggers and None for listener
        console_fallback = logging.getLogger("console_fallback")
        error_fallback = logging.getLogger("error_fallback")
        analytics_fallback = logging.getLogger("analytics_fallback")
        return console_fallback, error_fallback, analytics_fallback, None


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
    ensure_directory(analytics_dir)  # Ensure directory exists

    # Choose file based on mode
    report_filename = "analytics_full.html" if force_mode else "analytics_incremental.html"
    return os.path.join(analytics_dir, report_filename)
