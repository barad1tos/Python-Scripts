#!/usr/bin/env python3

"""AppleScript Client Module.

This module provides an abstraction for executing AppleScript commands asynchronously.
It centralizes the logic for interacting with AppleScript via the `osascript` command,
handles errors, applies concurrency limits via a semaphore, and ensures non-blocking execution.

The module supports both executing AppleScript files and inline AppleScript code.
"""

import asyncio
import logging
import os
import re

# trunk-ignore(bandit/B404)
import subprocess
import time

from pathlib import Path
from typing import Any, Protocol, runtime_checkable

RESULT_PREVIEW_LEN = 50
DANGEROUS_ARG_CHARS = [";", "&", "|", "`", "$", ">", "<", "!"]

@runtime_checkable
class AppleScriptClientProtocol(Protocol):
    """Protocol defining the interface for AppleScript clients.

    This allows both AppleScriptClient and DryRunAppleScriptClient to be used
    interchangeably as long as they implement these methods.
    """

    async def run_script(
        self,
        script_name: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run an AppleScript by name."""
        ...

    async def run_script_code(
        self,
        script_code: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run raw AppleScript code."""
        ...

    async def initialize(self) -> None:
        """Initialize the AppleScript client."""
        ...


class EnhancedRateLimiter:
    """Advanced rate limiter using a moving window approach."""

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
        self.request_timestamps: list[float] = []
        self.semaphore: asyncio.Semaphore | None = None
        self.max_concurrent = max_concurrent
        self.logger = logger or logging.getLogger(__name__)
        self.total_requests: int = 0
        self.total_wait_time: float = 0.0

    async def initialize(self) -> None:
        """Initialize the rate limiter."""
        if self.semaphore is None:
            try:
                self.semaphore = asyncio.Semaphore(self.max_concurrent)
                self.logger.debug(
                    f"RateLimiter initialized with max_concurrent: {self.max_concurrent}"
                )
            except Exception as e:
                self.logger.error(
                    f"Error initializing RateLimiter semaphore: {e}", exc_info=True
                )

    async def acquire(self) -> float:
        """Acquire permission to make a request, waiting if necessary due to rate limits or concurrency limits."""
        if self.semaphore is None:
            raise RuntimeError("RateLimiter not initialized")
        rate_limit_wait_time = await self._wait_if_needed()
        self.total_requests += 1
        self.total_wait_time += rate_limit_wait_time
        await self.semaphore.acquire()
        return rate_limit_wait_time

    def release(self) -> None:
        """Release the semaphore, allowing another request to proceed."""
        if self.semaphore is None:
            return
        self.semaphore.release()

    async def _wait_if_needed(self) -> float:
        now = time.monotonic()
        while (
            self.request_timestamps
            and now - self.request_timestamps[0] > self.window_size
        ):
            self.request_timestamps.pop(0)
        if len(self.request_timestamps) >= self.requests_per_window:
            oldest_timestamp = self.request_timestamps[0]
            wait_duration = (oldest_timestamp + self.window_size) - now
            if wait_duration > 0:
                self.logger.debug(
                    f"Rate limit reached. Waiting {wait_duration:.3f}s"
                )
                await asyncio.sleep(wait_duration)
                return wait_duration + await self._wait_if_needed()
        self.request_timestamps.append(time.monotonic())
        return 0.0

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about rate limiter usage."""
        now = time.monotonic()
        self.request_timestamps = [
            ts for ts in self.request_timestamps if now - ts <= self.window_size
        ]
        return {
            "total_requests": self.total_requests,
            "total_wait_time": self.total_wait_time,
            "avg_wait_time": self.total_wait_time
            / max(1, self.total_requests),
            "current_window_usage": len(self.request_timestamps),
            "max_requests_per_window": self.requests_per_window,
        }


class AppleScriptClient(AppleScriptClientProtocol):
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

    async def _run_osascript(
        self,
        cmd: list[str],
        label: str,
        timeout: float,
    ) -> str | None:
        """Run an osascript command and return output.

        Args:
            cmd: Command to execute as list of strings
            label: Label for logging
            timeout: Timeout in seconds

        Returns:
            str: Command output if successful, None otherwise

        """
        result: str | None = None

        if self.semaphore is None:
            self.error_logger.error(
                "AppleScriptClient semaphore not initialized. Call initialize() first."
            )
            return None

        async def handle_process() -> tuple[str | None, bool]:
            """Handle process execution and return (result, should_continue) tuple."""
            result = None
            should_continue = True

            try:
                start_time = time.time()
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )

                try:
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), timeout=timeout
                    )
                    elapsed = time.time() - start_time

                    # Process stderr if present
                    if stderr:
                        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
                        self.console_logger.warning("◁ %s stderr: %s", label, stderr_text[:200])

                    # Handle process completion
                    if proc.returncode == 0:
                        result = stdout.decode("utf-8", errors="ignore").strip()
                        preview = (
                            f"{result[:RESULT_PREVIEW_LEN]}..."
                            if len(result) > RESULT_PREVIEW_LEN
                            else result
                        )
                        self.console_logger.info(
                            "◁ %s (%dB, %.1fs) %s",
                            label,
                            len(result.encode("utf-8")),
                            elapsed,
                            preview,
                        )
                        should_continue = True
                    else:
                        self.error_logger.error(
                            "◁ %s failed with return code %s: %s",
                            label,
                            proc.returncode,
                            stderr.decode("utf-8", errors="ignore").strip() if stderr else "",
                        )
                        should_continue = True

                except TimeoutError:
                    self.error_logger.error("⊗ %s timeout: %ss exceeded", label, timeout)
                    should_continue = False

                except (subprocess.SubprocessError, OSError) as e:
                    self.error_logger.error("⊗ %s error during execution: %s", label, e)
                    should_continue = False

                except asyncio.CancelledError:
                    self.console_logger.info("⊗ %s cancelled", label)
                    raise

                except Exception as e:
                    self.error_logger.exception(
                        f"⊗ {label} unexpected error during communicate/wait: {e}"
                    )
                    should_continue = False

                finally:
                    await self._cleanup_process(proc, label)

            except OSError as e:
                self.error_logger.error("⊗ %s subprocess error: %s", label, e)
                should_continue = False

            return result, should_continue

        async with self.semaphore:
            result, _ = await handle_process()
            return result

    async def _cleanup_process(self, proc: asyncio.subprocess.Process, label: str) -> None:
        """Clean up process resources.

        Args:
            proc: Process to clean up
            label: Label for logging

        """
        if proc.returncode is None:  # Process is still running
            try:
                proc.kill()
                await asyncio.wait_for(proc.wait(), timeout=5)
                self.console_logger.debug("Process for %s cleaned up", label)
            except (TimeoutError, ProcessLookupError):
                self.console_logger.warning(
                    "Could not kill or wait for process %s during cleanup", label
                )

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

        if timeout is None:
            timeout = self.config.get("applescript_timeout_seconds", 600)

        script_path = os.path.join(self.apple_scripts_dir, script_name)

        if not self._validate_script_path(script_path):
            self.error_logger.error("Invalid script path: %s", script_path)
            return None

        if not os.path.exists(script_path):
            self.error_logger.error("AppleScript file does not exist: %s", script_path)
            return None

        cmd = ["osascript", script_path]
        if arguments:
            for arg in arguments:
                if any(c in arg for c in DANGEROUS_ARG_CHARS):
                    self.error_logger.error(
                        "Potentially dangerous characters in argument: %s", arg
                    )
                    return None
            cmd.extend(arguments)

        args_str = (
            f"args: {', '.join(f'{arg}' for arg in arguments)}" if arguments else "no args"
        )

        # Ensure timeout is a float
        timeout_float = float(timeout) if timeout is not None else 600.0

        self.console_logger.info("▷ %s (%s) [t:%ss]", script_name, args_str, timeout_float)

        return await self._run_osascript(cmd, script_name, timeout_float)

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
            script_code[:RESULT_PREVIEW_LEN] + "..."
            if len(script_code) > RESULT_PREVIEW_LEN
            else script_code
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
        if not isinstance(script_code, str) or not script_code.strip():
            self.error_logger.error("No script code provided.")
            return None

        if timeout is None:
            timeout = self.config.get("applescript_timeout_seconds", 600)

        # Ensure timeout is a float
        timeout_float = float(timeout) if timeout is not None else 600.0

        cmd = ["osascript", "-e", script_code]
        if arguments:
            cmd.extend(arguments)

        code_preview = self._format_script_preview(script_code)
        self.console_logger.info(
            "▷ inline-script (%dB) [t:%ss] %s",
            len(script_code.encode("utf-8")),
            timeout_float,
            code_preview,
        )

        return await self._run_osascript(cmd, "inline-script", timeout_float)
