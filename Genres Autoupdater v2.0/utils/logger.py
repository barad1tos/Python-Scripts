#!/usr/bin/env python3
# utils/logger.py
"""Enhanced Logger Module using QueueHandler for non-blocking file IO, with RichHandler for improved console output.

Provides a comprehensive logging system with detailed tracking and visual formatting. Features:

1.  **Run Tracking:** Adds headers/footers with timestamps and duration for script executions (`RunHandler`).
2.  **Log Rotation by Runs:** Keeps only the most recent N runs in log files (`RunHandler.trim_log_to_max_runs`).
3.  **Rich Console Output:** Uses `rich.logging.RichHandler` for color coding, formatting, and improved readability in the terminal.
4.  **Path Aliases:** Shortens file paths in log messages to readable aliases like `$SCRIPTS`, `$LOGS`, `$MUSIC_LIB`, `~` (`shorten_path`).
5.  **Multiple Loggers:** Configures distinct loggers for console output, main application/error logging, and analytics (`get_loggers`).
6.  **Non-Blocking File Logging:** Employs `QueueHandler` and `QueueListener` to prevent file I/O from blocking the main application thread.
7.  **Visual Indicators:** Uses emojis (噫, 潤) and separators for improved readability in log files and console.
8.  **Compact Formatting:** Provides a `CompactFormatter` (primarily for files) and configures `RichHandler` for a compact console view.
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
from pathlib import Path
from typing import Any, Literal

from rich.console import Console
from rich.logging import RichHandler

# ANSI escape codes for colors - used by CompactFormatter (primarily for files now)
# RichHandler handles console colors automatically
RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
GRAY = "\033[90m"
BOLD = "\033[1m"

# Level abbreviations and colors - used by CompactFormatter (primarily for files now)
LEVEL_COLORS = {
    "DEBUG": GRAY,
    "INFO": GREEN,
    "WARNING": YELLOW,
    "ERROR": RED,
    "CRITICAL": MAGENTA + BOLD,
}
LEVEL_ABBREV = {
    "DEBUG": "D",
    "INFO": "I",
    "WARNING": "W",
    "ERROR": "E",
    "CRITICAL": "C",
}

# Removed LOG_LEVELS_MAP as we will use logging.getLevelName directly


class RunHandler:
    """Handles tracking of script runs, adding separators between runs, and limiting logs to max number of runs."""

    def __init__(self, max_runs: int = 3):
        " Initialize the RunHandler."""
        self.max_runs = max_runs
        self.current_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_start_time = time.monotonic()  # Use monotonic for duration

    def format_run_header(self, logger_name: str) -> str:
        "Create a formatted header for a new run."""
        # Use local time for now.
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Use ANSI colors for file headers for readability in terminals that support them
        header = f"\n\n{BLUE}{'=' * 80}{RESET}\n"
        header += f"噫 NEW RUN: {logger_name} - {now_str}\n"
        header += f"{BLUE}{'=' * 80}{RESET}\n\n"
        return header

    def format_run_footer(self, logger_name: str) -> str:
        "Create a formatted footer for the end of a run."""
        elapsed = time.monotonic() - self.run_start_time
        # Use ANSI colors for file footers
        footer = f"\n\n{BLUE}{'=' * 80}{RESET}\n"
        footer += f"潤 END RUN: {logger_name} - Total time: {elapsed:.2f}s\n"
        footer += f"{BLUE}{'=' * 80}{RESET}\n\n"
        return footer

    def trim_log_to_max_runs(self, log_file: str) -> None:
        """Trims a log file to contain only the most recent N runs, identified by run headers."""
        if not os.path.exists(log_file) or self.max_runs <= 0:
            return

        try:
            # Use text mode with utf-8 and ignore errors for simplicity.
            with open(log_file, encoding="utf-8", errors="ignore") as f:
                # Read lines efficiently, especially for potentially large files
                lines = f.readlines()

            # Find indices of run headers
            # Look for the separator line followed by the "噫 NEW RUN:" line
            header_indices = [
                i
                for i, line in enumerate(lines)
                # Check for the separator line (handle potential color codes)
                if re.match(r"^(\x1b\[\d+m)?={80}(\x1b\[0m)?$", line.strip())
                and i + 1 < len(lines)
                # Check for the run header line (handle potential color codes and emoji)
                and re.match(r"^(\x1b\[\d+m)?噫 NEW RUN:", lines[i + 1].strip())
            ]

            if len(header_indices) <= self.max_runs:
                return  # No trimming needed

            # Keep only the last 'max_runs' sections
            start_line_index = header_indices[
                -self.max_runs
            ]  # Index of the oldest header to keep

            # Use atomic write pattern to avoid data loss if writing fails
            temp_log_file = f"{log_file}.tmp"
            with open(temp_log_file, "w", encoding="utf-8") as f:
                f.writelines(lines[start_line_index:])

            # Replace original file with temporary file
            os.replace(temp_log_file, log_file)

            # print(f"Trimmed log file {log_file}, kept last {self.max_runs} runs.") # Optional debug print

        except Exception as e:
            # Use print for errors during logging setup/teardown as logger might not be available
            print(f"Error trimming log file {log_file}: {e}", file=sys.stderr)


# Added optional error_logger argument for logging within the utility
def ensure_directory(path: str, error_logger: logging.Logger | None = None) -> None:
    """Ensure that the given directory path exists, creating it if necessary.

    Handles potential race conditions during creation. Logs errors using the provided logger.
    """
    try:
        # Check if path is not empty and doesn't exist before trying to create
        if path and not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
    except OSError as e:
        # Handle potential errors during directory creation (e.g., permission issues)
        if error_logger:
            error_logger.error(f"Error creating directory {path}: {e}")
        else:
            print(f"ERROR: Error creating directory {path}: {e}", file=sys.stderr)
    except Exception as e:  # Catch other potential errors
        if error_logger:
            error_logger.error(f"Unexpected error ensuring directory {path}: {e}")
        else:
            print(
                f"ERROR: Unexpected error ensuring directory {path}: {e}",
                file=sys.stderr,
            )


# Added optional loggers arguments for logging within the utility
def get_full_log_path(
    config: dict[str, Any] | None, key: str, default: str, error_logger: logging.Logger | None = None
) -> str:
    """Return the full log path by joining the base logs directory with the relative path.

    Ensures the base directory and the log file's directory exist. Logs errors using the provided logger.
    """
    # Initialize with default values
    logs_base_dir_value = "."
    relative_path = default

    # Process config if it's a dictionary
    if isinstance(config, dict):
        # Get base directory from config or use default
        logs_base_dir_value = config.get("logs_base_dir", ".")

        # Get logging config
        logging_config = config.get("logging", {})
        if isinstance(logging_config, dict):
            # Get relative path from config, use default if key is missing
            relative_path = logging_config.get(key, default)
        elif error_logger:
            error_logger.error("Invalid 'logging' section in config.")
    elif error_logger:
        error_logger.error("Invalid config passed to get_full_log_path.")
    else:
        print("ERROR: Invalid config passed to get_full_log_path.", file=sys.stderr)

    # Ensure the base directory exists
    ensure_directory(logs_base_dir_value, error_logger)

    # Initialize with default value
    relative_path = default

    # Only try to get logging config if config is a dictionary
    if isinstance(config, dict):
        logging_config = config.get("logging", {})
        if not isinstance(logging_config, dict):
            # Log error if logging config is invalid, use default relative path
            if error_logger:
                error_logger.error("Invalid 'logging' section in config.")
        else:
            # Get relative path from config, use default if key is missing
            relative_path = logging_config.get(key, default)

    # Join base directory and relative path
    # Use value directly, removing variable assignment that linter flagged
    full_path = os.path.join(logs_base_dir_value, relative_path)

    # Ensure the directory for the specific log file exists
    # The path is constructed directly when calling get_full_log_path in get_loggers
    ensure_directory(
        os.path.dirname(full_path), error_logger
    )  # This is correct place to ensure log file directory exists

    return full_path


# Added optional loggers arguments for logging within the utility
def shorten_path(
    path: str,
    config: dict[str, Any] | None = None,
    error_logger: logging.Logger | None = None,
) -> str:
    """Shorten a file path for more readable log output.

    Replaces known base directories (from config or common locations) with aliases.
    Logs errors using the provided loggers.
    """
    # Initialize result with default value (original path)
    if not path or not isinstance(path, str):
        return str(path) if path is not None else ""

    # Normalize path separators for consistency
    norm_path = os.path.normpath(path)
    result = norm_path  # Default to normalized path if no shortening is needed

    # Check config-based path replacements
    if config and isinstance(config, dict):
        scripts_dir = os.path.normpath(config.get("apple_scripts_dir", ""))
        music_lib = os.path.normpath(config.get("music_library_path", ""))
        logs_dir = os.path.normpath(config.get("logs_base_dir", ""))

        # Check for script directory match
        if scripts_dir and norm_path.startswith(scripts_dir + os.sep):
            relative = os.path.relpath(norm_path, scripts_dir)
            result = f"$SCRIPTS{os.sep}{relative}"
        # Check for logs directory match
        elif logs_dir and norm_path.startswith(logs_dir + os.sep):
            relative = os.path.relpath(norm_path, logs_dir)
            result = f"$LOGS{os.sep}{relative}"
        # Check for music library match
        elif music_lib and norm_path.startswith(music_lib):
            if norm_path == music_lib:
                result = "$MUSIC_LIB"
            elif norm_path.startswith(music_lib + os.sep):
                relative = os.path.relpath(norm_path, music_lib)
                result = f"$MUSIC_LIB{os.sep}{relative}"

    # If no config matches, try home directory replacement
    if result == norm_path:
        try:
            home = str(Path.home())
            norm_home = os.path.normpath(home)
            if norm_path == norm_home:
                result = "~"
            elif norm_path.startswith(norm_home + os.sep):
                result = norm_path.replace(norm_home, "~", 1)
        except Exception as e:
            warning_msg = f"Could not get home directory to shorten path: {e}"
            if error_logger:
                error_logger.warning(warning_msg)
            else:
                print(f"WARNING: {warning_msg}", file=sys.stderr)

    # If it's an absolute path not matched above, return just the filename
    if result == norm_path and os.path.isabs(norm_path) and os.path.dirname(norm_path) != ".":
        result = os.path.basename(norm_path)

    return result


class CompactFormatter(logging.Formatter):
    """Custom log formatter for compact, readable output, primarily for file logs.

    Features: Level abbreviation, short timestamps, and precise path shortening
    applied to specific log record fields.
    RichHandler is used for console output.
    """

    _FormatStyleType = Literal["%", "{", "$"]  # Define type alias for style parameter

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str = "%H:%M:%S",
        style: _FormatStyleType = "%",
        include_separator: bool = False,
        config: dict[str, Any] | None = None,
    ):
        """Initialize the CompactFormatter."""
        # Define a default format string that includes placeholders for shortened paths
        # We will populate 'short_pathname' and 'short_filename' in the format method
        # Default format including time, level, logger name, shortened path/filename, and message
        # Example format string using custom attributes: %(short_pathname)s:%(lineno)d
        default_fmt = "%(asctime)s %(levelname)s [%(name)s] %(short_pathname)s:%(lineno)d - %(message)s"

        # Use the provided format string if available, otherwise use the default
        used_fmt = fmt if fmt is not None else default_fmt

        super().__init__(used_fmt, datefmt, style)
        self.include_separator = include_separator
        # Separator line uses ANSI colors for file logs
        self.separator_line = f"\n{BLUE}{'-' * 60}{RESET}"
        self.config = config or {}  # Ensure config is at least an empty dict

    def format(self, record: logging.LogRecord) -> str:
        """Format the LogRecord into a compact string, applying path shortening to specific attributes before standard formatting."""
        # Store original level name for abbreviation lookup
        original_levelname = record.levelname

        # --- Apply Path Shortening to Specific Attributes ---
        # Apply shorten_path to the original pathname and filename attributes
        # and store them in new attributes on the record object.
        # Ensure the record has the necessary attributes before trying to shorten
        if hasattr(record, "pathname"):
            # Apply shorten_path and store in a new attribute
            record.short_pathname = shorten_path(record.pathname, self.config)
        else:
            # If pathname is missing, use the original value (or provide a placeholder)
            record.short_pathname = getattr(
                record, "pathname", "N/A"
            )  # Use 'N/A' if attribute doesn't exist

        if hasattr(record, "filename"):
            # Apply shorten_path and store in a new attribute
            record.short_filename = shorten_path(record.filename, self.config)
        else:
            # If filename is missing, use the original value (or provide a placeholder)
            record.short_filename = getattr(
                record, "filename", "N/A"
            )  # Use 'N/A' if attribute doesn't exist

        # You could add other attributes here if they might contain paths, e.g., record.module
        # Example for module:
        # if hasattr(record, 'module'):
        #     record.short_module = shorten_path(record.module, self.config)
        # else:
        #     record.short_module = getattr(record, 'module', 'N/A')

        # --- Abbreviate Level Name ---
        # Abbreviate level name for display in the formatted message
        # This modifies the record's levelname attribute temporarily for formatting
        record.levelname = LEVEL_ABBREV.get(
            original_levelname, original_levelname[:1]
        )  # Fallback to first letter

        # --- Perform Standard Formatting ---
        # Let the parent class handle the core formatting using the (potentially modified) record attributes
        try:
            # The format string defined in __init__ or passed as fmt will now use
            # %(short_pathname)s, %(short_filename)s, etc.
            formatted = super().format(record)
        except Exception as format_error:
            # If the formatting fails, log the error and return a basic representation.
            # Use print for logging errors within the formatter as logger might be unavailable
            print(
                f"CRITICAL LOGGING ERROR during format: {format_error}", file=sys.stderr
            )
            # Restore original level name before returning basic info
            record.levelname = original_levelname
            # Attempt to format a basic message to provide some info
            try:
                # Use original attributes here as custom ones might not be set if error happened early
                basic_msg = f"{getattr(record, 'asctime', 'N/A')} {original_levelname} [{getattr(record, 'name', 'N/A')}] {record.getMessage()}"
                return f"FORMATTING ERROR: {basic_msg}"
            except Exception:
                return "CRITICAL FORMATTING ERROR: Unable to format log record."

        # --- Restore Original Level Name ---
        # Restore original levelname on the record object after formatting is done
        # This is important if the record object is reused or inspected elsewhere by other handlers/formatters
        record.levelname = original_levelname

        # --- Remove Custom Attributes ---
        # Clean up the custom attributes we added to the record
        # This prevents them from potentially interfering with other handlers/formatters
        if hasattr(record, "short_pathname"):
            delattr(record, "short_pathname")
        if hasattr(record, "short_filename"):
            delattr(record, "short_filename")
        # if hasattr(record, 'short_module'): # If you added short_module
        #     del record.short_module

        # Add separator line if requested (for file logs)
        # This logic remains the same
        if getattr(record, "section_end", False) and self.include_separator:
            formatted += self.separator_line

        return formatted


class RunTrackingHandler(logging.FileHandler):
    """A file handler that adds run separation headers/footers and trims the log to a maximum number of runs based on headers."""

    def __init__(
        self,
        filename: str,
        mode: str = "a",
        encoding: str | None = "utf-8",
        delay: bool = False,
        run_handler: Any | None = None,
        logger_name: str = "Logger",
    ) -> None:
        """Initialize the handler."""
        # Ensure directory exists before initializing FileHandler
        # Pass logger to ensure_directory
        # Cannot pass error_logger here easily before loggers are fully setup, use print fallback in ensure_directory
        ensure_directory(os.path.dirname(filename))
        super().__init__(filename, mode, encoding, delay)
        self.run_handler: Any | None = run_handler
        self.logger_name: str = logger_name
        self._header_written: bool = False  # Track if header for the *current* run has been written

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a record. Writes the run header before the first record of a new run."""
        if self.run_handler and not self._header_written:
            try:
                # Write header if it hasn't been written for this instance/run
                header = self.run_handler.format_run_header(self.logger_name)
                # Use self.stream.write for direct writing to the file
                self.stream.write(header)
                # Ensure the header is immediately written to the file
                self.flush()
                self._header_written = True
            except Exception:
                # Use standard error handling provided by logging
                self.handleError(record)

        # Emit the actual record using the parent class method
        super().emit(record)

    def close(self) -> None:
        """Close the stream, writes run footer, and trims the log file."""
        # Check if already closed to prevent recursion or multiple writes
        # Use a flag to prevent multiple close calls
        if getattr(self, "_closed", False):
            return

        # Set closed flag early
        self._closed = True

        try:
            if self.run_handler:
                # Write footer before closing, only if stream is available
                if self.stream and hasattr(self.stream, "write"):
                    footer = self.run_handler.format_run_footer(self.logger_name)
                    # Use self.stream.write for direct writing to the file
                    self.stream.write(footer)
                    self.flush()  # Ensure footer is written

        except Exception as e:
            # Log the error to stderr as loggers might be shutting down
            print(
                f"ERROR: Failed to write log footer or flush stream for {self.baseFilename}: {e}",
                file=sys.stderr,
            )

        finally:
            # Close the stream using parent method *before* trimming
            try:
                super().close()
            except Exception as close_err:
                print(
                    f"Error closing file stream for {self.baseFilename}: {close_err}",
                    file=sys.stderr,
                )

            # Trim log file *after* closing the stream
            if (
                self.run_handler
                and hasattr(self.run_handler, "max_runs")
                and self.run_handler.max_runs > 0
            ):
                try:
                    self.run_handler.trim_log_to_max_runs(self.baseFilename)
                except Exception as trim_e:
                    print(
                        f"Error trimming log file {self.baseFilename} after close: {trim_e}",
                        file=sys.stderr,
                    )


# Added loggers as return values and ensure_directory calls
def get_loggers(
    config: dict[str, Any],
) -> tuple[logging.Logger, logging.Logger, logging.Logger, QueueListener | None]:
    """Create and returns loggers using QueueHandler for non-blocking file logging, and RichHandler for console output.

    Ensures log directories exist and sets levels based on config.
    """
    listener: QueueListener | None = None  # Initialize listener

    try:
        # --- Get Log Levels from Config ---
        logging_config = config.get("logging", {})
        levels_config = logging_config.get("levels", {})

        # Helper function to get level from config string safely using logging.getLevelName
        def get_level_from_config(level_name: str, default_level: int = logging.INFO) -> int:
            """Get logging level constant from a string name, with fallback.

            Args:
                level_name: String name of the log level (e.g., 'INFO', 'DEBUG')
                default_level: Default log level to return if level_name is not recognized

            Returns:
                int: Logging level constant from logging module

            """
            # logging.getLevelName returns the level constant if found, or the string name if not found
            level = logging.getLevelName(level_name.upper())

            if isinstance(level, str):  # If getLevelName returns a string, it means the name was not found
                # Log a warning here if logger is available, using default
                # print(f"WARNING: Unknown log level name '{level_name}' in config. Using {default_level}.", file=sys.stderr)
                return default_level
            return int(level)  # Ensure we return an int

        # Get level for each logger/handler using the helper
        console_level = get_level_from_config(levels_config.get("console", "INFO"))
        main_file_level = get_level_from_config(levels_config.get("main_file", "INFO"))
        analytics_file_level = get_level_from_config(
            levels_config.get("analytics_file", "INFO")
        )
        year_updates_file_level = get_level_from_config(
            levels_config.get("year_updates_file", "INFO")
        )

        # --- Ensure Base Directories Exist ---
        # get_full_log_path handles base directory and specific log directory creation
        # Pass None for logger here, as loggers are not fully set up yet.
        # get_full_log_path will use print as a fallback for errors.
        main_log_file = get_full_log_path(
            config, "main_log_file", "main/main.log", None
        )
        analytics_log_file = get_full_log_path(
            config, "analytics_log_file", "analytics/analytics.log", None
        )
        year_changes_log_file = get_full_log_path(
            config, "year_changes_log_file", "main/year_changes.log", None
        )  # Ensure year changes log path is handled
        # last_db_verify_log_file = get_full_log_path( # Unused variable
        #     config, "last_db_verify_log", "main/last_db_verify.log", None
        # )  # Ensure db verify log path is handled

        # --- Common Setup ---
        max_runs = logging_config.get(
            "max_runs", 3
        )  # Get max_runs from logging section of config
        run_handler_instance = RunHandler(
            max_runs=max_runs
        )  # Single instance for run tracking

        # --- Formatters ---
        # CompactFormatter is now primarily for file handlers
        # Define the format string to use the new short_pathname and short_filename attributes
        # Use %(short_pathname)s to show the shortened file path, %(lineno)d for line number
        file_log_format_string = "%(asctime)s %(levelname)s [%(name)s] %(short_pathname)s:%(lineno)d - %(message)s"

        file_formatter = CompactFormatter(
            file_log_format_string,  # Use the new format string
            datefmt="%Y-%m-%d %H:%M:%S",
            include_separator=False,  # Separators added by RunTrackingHandler header/footer
            config=config,
        )

        # --- Console Logger (Using RichHandler) ---
        console_logger = logging.getLogger("console_logger")
        # Prevent adding handlers multiple times if get_loggers is called more than once
        if not console_logger.handlers:
            # Use RichHandler for console output
            # Configure RichHandler for a compact look
            ch = RichHandler(
                level=console_level,  # Set handler level from config
                console=Console(),  # Use default console or pass a custom one
                show_time=True,
                show_level=True,
                show_path=False,  # Hide default path/line number from RichHandler
                enable_link_path=False,  # Disable clickable paths
                log_time_format="%H:%M:%S",  # Compact time format
                # RichHandler doesn't have a direct 'config' parameter for shorten_path
                # Path shortening for console output would require a custom RichHandler subclass
                # or applying shortening before logging (less ideal).
                # For now, rely on RichHandler's default formatting which is usually clean for console.
            )
            ch.setLevel(console_level)  # Set handler level from config
            console_logger.addHandler(ch)
            console_logger.setLevel(console_level)  # Set logger level from config
            console_logger.propagate = (
                False  # Prevent messages from going to the root logger
            )

        # --- File Logging Setup (Using Queue) ---
        log_queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)  # Create the queue

        # Create the actual file handlers that the listener will use
        # Use RunTrackingHandler for run headers/footers and trimming
        # Pass console_logger and error_logger to handlers if they need to log internally (RunTrackingHandler does not currently)
        main_fh = RunTrackingHandler(
            main_log_file,
            mode="a",
            encoding="utf-8",
            run_handler=run_handler_instance,
            logger_name="Main Logger",
        )
        main_fh.setFormatter(file_formatter)  # Use the file formatter
        main_fh.setLevel(main_file_level)  # Set handler level from config

        analytics_fh = RunTrackingHandler(
            analytics_log_file,
            mode="a",
            encoding="utf-8",
            run_handler=run_handler_instance,
            logger_name="Analytics",
        )
        analytics_fh.setFormatter(file_formatter)  # Use the file formatter
        analytics_fh.setLevel(analytics_file_level)  # Set handler level from config

        # Add handler for year changes log
        year_changes_fh = RunTrackingHandler(
            year_changes_log_file,
            mode="a",
            encoding="utf-8",
            run_handler=run_handler_instance,
            logger_name="Year Updates",
        )
        year_changes_fh.setFormatter(file_formatter)  # Use the file formatter
        year_changes_fh.setLevel(
            year_updates_file_level
        )  # Set handler level from config

        # --- Queue Listener (Background Thread) ---
        # Pass all file handlers to the listener
        listener = QueueListener(
            log_queue,
            main_fh,
            analytics_fh,
            year_changes_fh,
            respect_handler_level=True,
        )
        listener.start()
        # Use print for setup logs as loggers might not be fully ready
        print("QueueListener started.", file=sys.stderr)

        # --- Queue Handler (Used by Loggers) ---
        # This handler puts records into the queue for file handlers
        queue_handler = QueueHandler(log_queue)

        # --- Configure Main/Error Logger ---
        # Get logger instance
        main_logger = logging.getLogger("main_logger")
        # Prevent adding handlers multiple times
        if not main_logger.handlers:
            main_logger.addHandler(queue_handler)  # Add queue handler for file logging
            # No console handler needed here, console_logger handles console output
            main_logger.setLevel(main_file_level)  # Set logger level from config
            main_logger.propagate = False
        error_logger = main_logger  # Alias error logger to main logger

        # --- Configure Analytics Logger ---
        # Get logger instance
        analytics_logger = logging.getLogger("analytics_logger")
        # Prevent adding handlers multiple times
        if not analytics_logger.handlers:
            analytics_logger.addHandler(
                queue_handler
            )  # Add queue handler for file logging
            # No console handler needed here
            analytics_logger.setLevel(
                analytics_file_level
            )  # Set logger level from config
            analytics_logger.propagate = False

        # --- Configure Year Updates Logger ---
        # Get logger instance
        year_updates_logger = logging.getLogger("year_updates")
        # Prevent adding handlers multiple times
        if not year_updates_logger.handlers:
            year_updates_logger.addHandler(
                queue_handler
            )  # Add queue handler for file logging
            # No console handler needed here
            year_updates_logger.setLevel(
                year_updates_file_level
            )  # Set logger level from config
            year_updates_logger.propagate = False

        # Note: The root logger (logging.getLogger()) is not explicitly configured here
        # to avoid duplicate messages if other libraries log to the root logger.
        # Messages sent to console_logger, main_logger, analytics_logger, year_updates_logger
        # will be handled by their respective handlers.

        console_logger.debug(
            "Logging setup with QueueListener and RichHandler complete."
        )
        # Return the configured loggers and the listener
        return console_logger, error_logger, analytics_logger, listener

    except Exception as e:
        # Fallback to basic config if custom setup fails
        # Use print for initial error logging if logger setup fails
        print(
            f"FATAL ERROR: Failed to configure custom logging with QueueListener and RichHandler: {e}",
            file=sys.stderr,
        )
        import traceback

        traceback.print_exc(file=sys.stderr)

        # Configure basic logging as a fallback
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        # Log the error using the fallback basic config
        logging.critical(f"Fallback basic logging configured due to error: {e}")
        # Return basic loggers and None for listener
        console_fallback = logging.getLogger("console_fallback")
        error_fallback = logging.getLogger("error_fallback")
        analytics_fallback = logging.getLogger("analytics_fallback")
        # Ensure fallback loggers have at least one handler to output messages
        if not console_fallback.handlers:
            console_fallback.addHandler(logging.StreamHandler(sys.stdout))
        if not error_fallback.handlers:
            error_fallback.addHandler(logging.StreamHandler(sys.stderr))
        if not analytics_fallback.handlers:
            analytics_fallback.addHandler(logging.StreamHandler(sys.stdout))

        return console_fallback, error_fallback, analytics_fallback, None


def get_html_report_path(config: dict[str, Any] | None, force_mode: bool = False) -> str:
    """Get the path for HTML analytics report based on run mode."""
    if not isinstance(config, dict):
        # Use print for error if config is invalid, as logger might not be available
        print("ERROR: Invalid config passed to get_html_report_path.", file=sys.stderr)
        # Fallback to current directory
        logs_base_dir = "."
    else:
        logs_base_dir = config.get("logs_base_dir", ".")

    analytics_dir = os.path.join(logs_base_dir, "analytics")
    # Ensure directory exists
    ensure_directory(analytics_dir)

    # Choose file based on mode
    report_filename = (
        "analytics_full.html" if force_mode else "analytics_incremental.html"
    )
    return os.path.join(analytics_dir, report_filename)
