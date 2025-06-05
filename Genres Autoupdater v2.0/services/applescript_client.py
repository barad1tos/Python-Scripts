#!/usr/bin/env python3

"""AppleScript Client Module.

This module provides an abstraction for executing AppleScript commands asynchronously.
It centralizes the logic for interacting with AppleScript via the `osascript` command,
handles errors, applies concurrency limits via a semaphore, and ensures non-blocking execution.

The module supports both executing AppleScript files and inline AppleScript code.

Refactored: Semaphore initialization moved to an async initialize method,
as it requires an active asyncio event loop.

Example:
    >>> import asyncio
    >>> from services.applescript_client import AppleScriptClient
    >>> config = {
    ...     "apple_scripts_dir": "path/to/apple_scripts",
    ...     "applescript_timeout_seconds": 600,
    ...     "apple_script_concurrency": 5
    ... }
    >>> # Assume loggers are initialized elsewhere
    >>> import logging
    >>> console_logger = logging.getLogger("console_logger")
    >>> error_logger = logging.getLogger("error_logger")
    >>>
    >>> async def test():
    ...     client = AppleScriptClient(config, console_logger, error_logger)
    ...     await client.initialize() # IMPORTANT: Call initialize
    ...     # Execute a script file
    ...     result1 = await client.run_script("fetch_tracks.applescript", arguments=["Some Artist"])
    ...     # Execute inline AppleScript code
    ...     result2 = await client.run_script_code('tell application "Music" to get name of current track')
    ...     print(result1, result2)
    ...
    >>> # Run the test in an event loop
    >>> # asyncio.run(test())

"""

import asyncio
import logging
import os
import re

# trunk-ignore(bandit/B404)
import subprocess
import time

from pathlib import Path
from typing import Any

class EnhancedRateLimiter:
    """Advanced rate limiter using a moving window approach.

    This ensures maximum throughput while strictly adhering to API rate limits.
    """

    def __init__(
        self,
        requests_per_window: int,
        window_size: float,
        max_concurrent: int = 3,
        logger: logging.Logger | None = None,
    ):
        """Initialize the rate limiter."""
        if not isinstance(requests_per_window, int) or requests_per_window <= 0:
            raise ValueError("requests_per_window must be a positive integer")
        if not isinstance(window_size, int | float) or window_size <= 0:
            raise ValueError("window_size must be a positive number")
        if not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")

        self.requests_per_window = requests_per_window
        self.window_size = float(window_size)
        # Using list as deque for simplicity, but collections.deque might be more performant for large windows
        self.request_timestamps: list[float] = []
        # Semaphore is initialized separately in the async initialize method
        self.semaphore: asyncio.Semaphore | None = None  # Initialized as None
        self.max_concurrent = max_concurrent  # Store max_concurrent

        self.logger = logger or logging.getLogger(__name__)
        self.total_requests: int = 0
        self.total_wait_time: float = 0.0

    async def initialize(self) -> None:
        """Asynchronously initialize the rate limiter semaphore.

        Must be called within an active event loop.
        """
        if self.semaphore is None:
            try:
                # Create semaphore when the event loop is available
                self.semaphore = asyncio.Semaphore(self.max_concurrent)
                self.logger.debug(
                    f"RateLimiter initialized with max_concurrent: {self.max_concurrent}"
                )
            except Exception as e:
                self.logger.error(
                    f"Error initializing RateLimiter semaphore: {e}", exc_info=True
                )
                # Depending on error handling strategy, might re-raise or handle failure

    async def acquire(self) -> float:
        """Acquire permission to make a request, waiting if necessary due to rate limits or concurrency limits.

        Requires initialize() to have been called.
        """
        if self.semaphore is None:
            self.logger.error(
                "RateLimiter semaphore not initialized. Call initialize() first."
            )
            # Depending on error handling, might raise an exception or return immediately
            raise RuntimeError("RateLimiter not initialized")

        # Wait for rate limit first
        rate_limit_wait_time = await self._wait_if_needed()
        self.total_requests += 1
        self.total_wait_time += rate_limit_wait_time

        # Then wait for concurrency semaphore
        await self.semaphore.acquire()

        return rate_limit_wait_time

    def release(self) -> None:
        """Release the concurrency semaphore after the request is completed or failed.

        Requires initialize() to have been called.
        """
        if self.semaphore is None:
            self.logger.error("RateLimiter semaphore not initialized. Cannot release.")
            # Depending on error handling, might raise an exception or pass
            return  # Safely exit if not initialized

        self.semaphore.release()

    async def _wait_if_needed(self) -> float:
        """Check rate limits and wait if necessary."""
        now = time.monotonic()  # Use monotonic clock for interval timing

        # Clean up old timestamps outside the window
        # This check ensures we don't keep growing the list indefinitely
        # It iterates from the left (oldest) and stops when a timestamp is within the window
        while (
            self.request_timestamps
            and now - self.request_timestamps[0] > self.window_size
        ):
            self.request_timestamps.pop(0)

        # If we've reached the limit, calculate wait time until the oldest request expires
        if len(self.request_timestamps) >= self.requests_per_window:
            oldest_timestamp = self.request_timestamps[0]
            wait_duration = (oldest_timestamp + self.window_size) - now

            # Only wait if the calculated duration is positive
            if wait_duration > 0:
                # Use 3 decimal places for milliseconds precision in logs
                self.logger.debug(
                    f"Rate limit reached ({len(self.request_timestamps)}/{self.requests_per_window} "
                    f"in {self.window_size}s). Waiting {wait_duration:.3f}s"
                )
                await asyncio.sleep(wait_duration)
                # After waiting, check again recursively in case multiple requests waited simultaneously
                # The recursive call handles scenarios where the wait wasn't quite enough
                # and adds the new wait time to the original
                return wait_duration + await self._wait_if_needed()
            # If wait_duration is <= 0, it means the slot freed up while we were checking.
            # No need to wait, but we should remove the expired timestamp before proceeding.
            # This case is implicitly handled by the while loop above if run again, or the check below.

        # Record this request's timestamp *after* potential waiting and cleanup
        self.request_timestamps.append(time.monotonic())
        return 0.0  # No waiting was required in this pass

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about rate limiter usage."""
        # Ensure current window usage is up-to-date by cleaning expired timestamps
        now = time.monotonic()
        self.request_timestamps = [
            ts for ts in self.request_timestamps if now - ts <= self.window_size
        ]

        return {
            "total_requests": self.total_requests,
            "total_wait_time": self.total_wait_time,
            "avg_wait_time": self.total_wait_time
            / max(1, self.total_requests),  # Avoid division by zero
            "current_window_usage": len(self.request_timestamps),
            "max_requests_per_window": self.requests_per_window,
        }


class AppleScriptClient:
    """A client to run AppleScript commands asynchronously using the osascript command.

    Semaphore initialization is done in the async initialize method.

    Attributes:
        config (dict): Configuration dictionary loaded from my-config.yaml.
        apple_scripts_dir (str): Directory containing AppleScript files.
        console_logger (logging.Logger): Logger for console output.
        error_logger (logging.Logger): Logger for error output.
        semaphore (Optional[asyncio.Semaphore]): Semaphore to limit concurrent AppleScript executions (initialized asynchronously).

    """

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger | None = None,
        error_logger: logging.Logger | None = None,
    ):
        """Initialize the AppleScript client."""
        self.config = config
        self.console_logger = (
            console_logger
            if console_logger is not None
            else logging.getLogger(__name__)
        )
        self.error_logger = (
            error_logger if error_logger is not None else self.console_logger
        )

        self.apple_scripts_dir = config.get("apple_scripts_dir")
        if not self.apple_scripts_dir:
            # Log critical error but don't raise here in __init__, let initialize handle it
            self.error_logger.critical(
                "Configuration error: 'apple_scripts_dir' key is missing."
            )

        # Semaphore is initialized in the async initialize method
        self.semaphore: asyncio.Semaphore | None = None  # Initialize as None

    async def initialize(self) -> None:
        """Asynchronously initializes the AppleScriptClient by creating the semaphore.

        Must be called within an active event loop.
        """
        if self.apple_scripts_dir is None:
            self.error_logger.critical(
                "AppleScript directory is not set. Cannot initialize client."
            )
            raise ValueError("AppleScript directory is not set.")

        if self.semaphore is None:
            try:
                # Create semaphore when the event loop is available
                concurrent_limit = self.config.get("apple_script_concurrency", 5)
                if concurrent_limit <= 0:
                    self.error_logger.critical(
                        f"Invalid 'apple_script_concurrency' value in config: {concurrent_limit}. Must be positive."
                    )
                    raise ValueError(
                        f"Invalid 'apple_script_concurrency' value: {concurrent_limit}"
                    )
                self.semaphore = asyncio.Semaphore(concurrent_limit)
                self.console_logger.debug(
                    f"AppleScriptClient semaphore initialized with concurrency: {concurrent_limit}"
                )
            except Exception as e:
                self.error_logger.error(
                    f"Error initializing AppleScriptClient semaphore: {e}",
                    exc_info=True,
                )
                raise  # Re-raise to signal initialization failure

        self.console_logger.info(
            "AppleScriptClient asynchronous initialization complete."
        )

    def _validate_script_path(self, script_path: str) -> bool:
        """Validate that the script path is safe to execute.

        :param script_path: Path to the script to validate
        :return: True if path is safe, False otherwise
        """
        try:
            if not script_path or not self.apple_scripts_dir:
                return False

            # Resolve the path to prevent directory traversal
            resolved_path = os.path.abspath(os.path.normpath(script_path))
            scripts_dir = os.path.abspath(self.apple_scripts_dir)

            # Ensure the path is within the allowed directory
            if not resolved_path.startswith(scripts_dir):
                self.error_logger.error(
                    "Script path is outside allowed directory: %s", script_path
                )
                return False

            # Check for suspicious patterns
            if any(
                part.startswith((".", "~")) or part == ".."
                for part in Path(script_path).parts
            ):
                self.error_logger.error("Suspicious script path: %s", script_path)
                return False

            return True

        except (ValueError, TypeError) as e:
            self.error_logger.error("Invalid script path %s: %s", script_path, e)
            return False

    async def run_script(
        self,
        script_name: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Execute an AppleScript asynchronously and return its output.

        Requires initialize() to have been called.

        :param script_name: The name of the AppleScript file to execute.
        :param arguments: List of arguments to pass to the script.
        :param timeout: Timeout in seconds for script execution
        :return: The output of the script, or None if an error occurred
        """
        if self.apple_scripts_dir is None:
            self.error_logger.error(
                "AppleScript directory is not set. Cannot run script."
            )
            return None

        # Set timeout from configuration, if not explicitly specified
        if timeout is None:
            timeout = self.config.get(
                "applescript_timeout_seconds", 600
            )  # 10 minutes by default

        script_path = os.path.join(self.apple_scripts_dir, script_name)

        # Validate script path
        if not self._validate_script_path(script_path):
            self.error_logger.error("Invalid script path: %s", script_path)
            return None

        if not os.path.exists(script_path):
            self.error_logger.error("AppleScript file does not exist: %s", script_path)
            return None

        # Build command list with safe arguments
        cmd = ["osascript", script_path]
        if arguments:
            # Sanitize each argument to prevent injection
            safe_arguments = []
            for arg in arguments:
                if not isinstance(arg, str):
                    self.error_logger.error(
                        "Invalid argument type: %s", type(arg).__name__
                    )
                    return None
                # Basic check for potentially dangerous characters
                if any(c in arg for c in [";", "&", "|", "`", "$", ">", "<", "!"]):
                    self.error_logger.error(
                        "Potentially dangerous characters in argument: %s", arg
                    )
                    return None
                safe_arguments.append(arg)
            cmd.extend(safe_arguments)

        # Format arguments for logging
        # Safely format arguments string
        args_str = (
            f"args: {', '.join(f'{arg}' for arg in arguments)}"
            if arguments
            else "no args"
        )
        self.console_logger.info("▷ %s (%s) [t:%ss]", script_name, args_str, timeout)

        # Ensure semaphore is initialized before using it
        if self.semaphore is None:
            self.error_logger.error(
                "AppleScriptClient semaphore not initialized. Call initialize() first."
            )
            return None  # Cannot proceed without semaphore

        async with self.semaphore:
            try:
                # First, create a process
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )

                # Then we wait for the result with Timeout
                try:
                    start_time = time.time()
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), timeout=timeout
                    )
                    elapsed = time.time() - start_time

                    if stderr:
                        stderr_text = stderr.decode(
                            "utf-8", errors="ignore"
                        ).strip()  # Decode safely
                        # Log stderr as warning, might contain useful info even on success
                        self.console_logger.warning(
                            "◁ %s stderr: %s", script_name, stderr_text[:200]
                        )  # Log a bit more for debug

                    if proc.returncode != 0:
                        # Log the full stderr on failure
                        self.error_logger.error(
                            "◁ %s failed with return code %s: %s",
                            script_name,
                            proc.returncode,
                            stderr.decode("utf-8", errors="ignore").strip(),
                        )
                        return None

                    result = stdout.decode(
                        "utf-8", errors="ignore"
                    ).strip()  # Decode safely
                    result_preview = result[:50] + "..." if len(result) > 50 else result
                    self.console_logger.info(
                        "◁ %s (%dB, %.1fs) %s",
                        script_name,
                        len(result),
                        elapsed,
                        result_preview,
                    )
                    return result

                except TimeoutError:
                    self.error_logger.error(
                        "⊗ %s timeout: %ss exceeded", script_name, timeout
                    )
                    # Ensure process is terminated on timeout
                    try:
                        proc.kill()
                        await asyncio.wait_for(
                            proc.wait(), timeout=5
                        )  # Wait a bit for process to terminate
                        self.console_logger.debug(
                            "Process for %s killed after timeout.", script_name
                        )
                    except (TimeoutError, ProcessLookupError):
                        self.console_logger.warning(
                            "Could not kill or wait for process %s after timeout.",
                            script_name,
                        )

                    return None
                except asyncio.CancelledError:
                    self.console_logger.info("⊗ %s cancelled", script_name)
                    # Ensure process is terminated on cancellation
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                        self.console_logger.debug(
                            "Process for %s killed after cancellation.", script_name
                        )
                    except (TimeoutError, ProcessLookupError):
                        self.console_logger.warning(
                            "Could not kill or wait for process %s after cancellation.",
                            script_name,
                        )

                    raise  # Re-raise cancellation
                except (subprocess.CalledProcessError, OSError) as e:
                    # These exceptions might occur during communicate() or process creation
                    self.error_logger.error(
                        "⊗ %s error during execution: %s", script_name, e
                    )
                    # Ensure process is terminated in case of OSError during execution
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                    except (TimeoutError, ProcessLookupError):
                        pass  # Ignore errors if process already finished or can't be killed

                    return None
                except (
                    Exception
                ) as e:  # Catch any other unexpected errors during communication/waiting
                    self.error_logger.exception(
                        f"⊗ {script_name} unexpected error during communicate/wait: {e}"
                    )
                    # Attempt to kill the process
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                    except (TimeoutError, ProcessLookupError):
                        pass  # Ignore errors if process already finished or can't be killed

                    return None

            except (FileNotFoundError, OSError) as e:
                # These exceptions occur if osascript command is not found or other system errors before process creation
                self.error_logger.error("⊗ %s subprocess error: %s", script_name, e)
                return None
            except (
                Exception
            ) as e:  # Catch any other unexpected errors before process creation
                self.error_logger.exception(
                    f"⊗ {script_name} unexpected error before process creation: {e}"
                )
                return None

    def _format_script_preview(self, script_code):
        """Format AppleScript code for log output, showing only essential parts."""
        if not isinstance(script_code, str):  # Handle non-string input gracefully
            return str(script_code)  # Return string representation

        # Normalize whitespace
        script_code = re.sub(
            r"\s+", " ", script_code.replace("\n", " ").replace("\r", " ")
        ).strip()

        # Find "tell application" pattern
        tell_match = re.search(
            r'tell application\s+["\'](.*?)["\']', script_code, re.IGNORECASE
        )  # Use IGNORECASE
        if tell_match:
            app_name = tell_match.group(1)
            # Include first part of the command for better context
            command_preview = script_code[tell_match.end() :].strip()[:30] + "..."
            return f'tell application "{app_name}" {command_preview}'

        # Fallback if pattern not found
        return (
            script_code[:50] + "..." if len(script_code) > 50 else script_code
        )  # Show a bit more fallback context

    async def run_script_code(
        self,
        script_code: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Execute an AppleScript code asynchronously and return its output.

        Requires initialize() to have been called.

        :param script_code: The AppleScript code to execute.
        :param arguments: List of arguments to pass to the script.
        :param timeout: Timeout in seconds for script execution
        :return: The output of the script, or None if an error occurred
        """
        if (
            not isinstance(script_code, str) or not script_code.strip()
        ):  # Basic validation
            self.error_logger.error("No script code provided.")
            return None

        # Set timeout from configuration, if not explicitly specified
        if timeout is None:
            timeout = self.config.get(
                "applescript_timeout_seconds", 600
            )  # 10 minutes by default

        # Build command list
        cmd = ["osascript", "-e", script_code]
        if arguments:
            cmd.extend(arguments)

        code_preview = self._format_script_preview(script_code)
        self.console_logger.info(
            "▷ inline-script '%s' (%dB) [t:%ss]",
            code_preview,
            len(script_code.encode("utf-8")),
            timeout,
        )  # Use byte length

        # Ensure semaphore is initialized before using it
        if self.semaphore is None:
            self.error_logger.error(
                "AppleScriptClient semaphore not initialized. Call initialize() first."
            )
            return None  # Cannot proceed without semaphore

        async with self.semaphore:
            try:
                start_time = time.time()
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )

                # Wait with timeout
                try:
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), timeout=timeout
                    )
                    elapsed = time.time() - start_time

                    if stderr:
                        stderr_text = stderr.decode(
                            "utf-8", errors="ignore"
                        ).strip()  # Decode safely
                        self.console_logger.warning(
                            "◁ inline-script stderr: %s", stderr_text[:200]
                        )  # Log a bit more

                    if proc.returncode != 0:
                        self.error_logger.error(
                            "◁ inline-script failed with return code %s: %s",
                            proc.returncode,
                            stderr.decode("utf-8", errors="ignore").strip(),
                        )
                        return None

                    result = stdout.decode(
                        "utf-8", errors="ignore"
                    ).strip()  # Decode safely
                    result_preview = result[:50] + "..." if len(result) > 50 else result
                    self.console_logger.info(
                        "◁ inline-script (%dB, %.1fs) %s",
                        len(result.encode("utf-8")),
                        elapsed,
                        result_preview,
                    )  # Use byte length
                    return result

                except TimeoutError:
                    self.error_logger.error(
                        "⊗ inline-script timeout: %ss exceeded", timeout
                    )
                    # Ensure process is terminated on timeout
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                        self.console_logger.debug(
                            "Process for inline-script killed after timeout."
                        )
                    except (TimeoutError, ProcessLookupError):
                        self.console_logger.warning(
                            "Could not kill or wait for process for inline-script after timeout."
                        )
                    return None
                except asyncio.CancelledError:
                    self.console_logger.info("⊗ inline-script cancelled")
                    # Ensure process is terminated on cancellation
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                        self.console_logger.debug(
                            "Process for inline-script killed after cancellation."
                        )
                    except (TimeoutError, ProcessLookupError):
                        self.console_logger.warning(
                            "Could not kill or wait for process for inline-script after cancellation."
                        )

                    raise  # Re-raise cancellation
                except (subprocess.SubprocessError, OSError) as e:
                    self.error_logger.error(
                        "⊗ inline-script error during execution: %s", e
                    )
                    # Ensure process is terminated
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                    except (TimeoutError, ProcessLookupError):
                        pass  # Ignore errors

                    return None
                except (
                    Exception
                ) as e:  # Catch any other unexpected errors during communication/waiting
                    self.error_logger.exception(
                        f"⊗ inline-script unexpected error during communicate/wait: {e}"
                    )
                    # Attempt to kill the process
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=5)  # Wait a bit
                    except (TimeoutError, ProcessLookupError):
                        pass  # Ignore errors

                    return None

            except (FileNotFoundError, OSError, subprocess.SubprocessError) as e:
                # These exceptions occur if osascript command is not found or other system errors before process creation
                self.error_logger.error("⊗ inline-script subprocess error: %s", e)
                return None
            except (
                Exception
            ) as e:  # Catch any other unexpected errors before process creation
                self.error_logger.exception(
                    f"⊗ inline-script unexpected error before process creation: {e}"
                )
                return None
