#!/usr/bin/env python3

"""
AppleScript Client Module

This module provides an abstraction for executing AppleScript commands asynchronously.
It centralizes the logic for interacting with AppleScript via the `osascript` command,
handles errors, applies concurrency limits via a semaphore, and ensures non-blocking execution.

The module supports both executing AppleScript files and inline AppleScript code.

Example:
    >>> import asyncio
    >>> from applescript_client import AppleScriptClient
    >>> config = {
    ...     "apple_scripts_dir": "path/to/apple_scripts",
    ...     "applescript_timeout_seconds": 600,
    ...     "apple_script_concurrency": 5
    ... }
    >>> client = AppleScriptClient(config)
    >>> async def test():
    ...     # Execute a script file
    ...     result1 = await client.run_script("fetch_tracks.applescript", arguments=["Some Artist"])
    ...     # Execute inline AppleScript code
    ...     result2 = await client.run_script_code('tell application "Music" to get name of current track')
    ...     print(result1, result2)
    >>> asyncio.run(test())
"""

import asyncio
import logging
import os
import re
import subprocess
import time

from typing import List, Optional, Union


class AppleScriptClient:
    """
    A client to run AppleScript commands asynchronously using the osascript command.

    Attributes:
        config (dict): Configuration dictionary loaded from my-config.yaml.
        apple_scripts_dir (str): Directory containing AppleScript files.
        console_logger (logging.Logger): Logger for console output.
        error_logger (logging.Logger): Logger for error output.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent AppleScript executions.
    """

    def __init__(self, config: dict, console_logger: logging.Logger = None, error_logger: logging.Logger = None):
        self.config = config
        self.console_logger = console_logger if console_logger is not None else logging.getLogger(__name__)
        self.error_logger = error_logger if error_logger is not None else self.console_logger

        self.apple_scripts_dir = config.get("apple_scripts_dir")
        if not self.apple_scripts_dir:
            raise ValueError("The 'apple_scripts_dir' key is missing in the configuration.")
        # Limit concurrent AppleScript calls; default value can be set in config under 'apple_script_concurrency'
        self.semaphore = asyncio.Semaphore(config.get("apple_script_concurrency", 5))

    async def run_script(self, script_name: str, arguments: Union[List[str], None] = None, timeout: Optional[float] = None) -> Optional[str]:
        """
        Execute an AppleScript asynchronously and return its output.

        :param script_name: The name of the AppleScript file to execute.
        :param arguments: List of arguments to pass to the script.
        :param timeout: Timeout in seconds for script execution
        :return: The output of the script, or None if an error occurred
        """
        if not self.apple_scripts_dir:
            self.error_logger.error("AppleScript directory is not set.")
            return None

        # Set timeout from configuration, if not explicitly specified
        if timeout is None:
            timeout = self.config.get("applescript_timeout_seconds", 600)  # 10 minutes by default

        script_path = os.path.join(self.apple_scripts_dir, script_name)
        if not os.path.exists(script_path):
            self.error_logger.error("AppleScript file does not exist: %s", script_path)
            return None

        # Build command list
        cmd = ["osascript", script_path]
        if arguments:
            cmd.extend(arguments)

        # Format arguments for logging
        args_str = f"args: {', '.join(f'{a}' for a in arguments)}" if arguments else "no args"
        self.console_logger.info("▷ %s (%s) [t:%ss]", script_name, args_str, timeout)

        async with self.semaphore:
            try:
                # First, create a process
                proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

                # Then we wait for the result with Timeout
                try:
                    start_time = time.time()
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                    elapsed = time.time() - start_time

                    if stderr:
                        stderr_text = stderr.decode().strip()
                        self.console_logger.warning("◁ %s stderr: %s", script_name, stderr_text[:80])

                    if proc.returncode != 0:
                        self.error_logger.error("◁ %s failed: %s", script_name, stderr.decode().strip())
                        return None

                    result = stdout.decode().strip()
                    result_preview = result[:50] + "..." if len(result) > 50 else result
                    self.console_logger.info("◁ %s (%dB, %.1fs) %s", script_name, len(result), elapsed, result_preview)
                    return result

                except asyncio.TimeoutError:
                    self.error_logger.error("⊗ %s timeout: %ss exceeded", script_name, timeout)
                    proc.kill()
                    await proc.wait()
                    return None
                except asyncio.CancelledError:
                    self.console_logger.info("⊗ %s cancelled", script_name)
                    proc.kill()
                    await proc.wait()
                    raise
                except (subprocess.CalledProcessError, OSError) as e:
                    self.error_logger.error("⊗ %s error: %s", script_name, e)
                    proc.kill()
                    await proc.wait()
                    return None
            except (FileNotFoundError, OSError) as e:
                self.error_logger.error("⊗ %s subprocess error: %s", script_name, e)
                return None

    def _format_script_preview(self, script_code):
        """Format AppleScript code for log output, showing only essential parts."""
        # Normalize whitespace
        script_code = re.sub(r'\s+', ' ', script_code.replace('\n', ' ').replace('\r', ' ')).strip()

        # Find "tell application" pattern
        tell_match = re.search(r'tell application\s+["\'](.*?)["\']', script_code)
        if tell_match:
            app_name = tell_match.group(1)
            return f'tell application "{app_name}" ...'

        # Fallback if pattern not found
        return script_code[:20] + "..." if len(script_code) > 20 else script_code

    async def run_script_code(self, script_code: str, arguments: Union[List[str], None] = None, timeout: Optional[float] = None) -> Optional[str]:
        """
        Execute an AppleScript code asynchronously and return its output.

        :param script_code: The AppleScript code to execute.
        :param arguments: List of arguments to pass to the script.
        :param timeout: Timeout in seconds for script execution
        :return: The output of the script, or None if an error occurred
        """
        # Set timeout from configuration, if not explicitly specified
        if timeout is None:
            timeout = self.config.get("applescript_timeout_seconds", 600)  # 10 minutes by default

        # Build command list
        cmd = ["osascript", "-e", script_code]
        if arguments:
            cmd.extend(arguments)

        code_preview = self._format_script_preview(script_code)
        self.console_logger.info("▷ inline-script '%s' (%dB) [t:%ss]", code_preview, len(script_code), timeout)

        async with self.semaphore:
            try:
                start_time = time.time()
                proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

                # Wait with timeout
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                    elapsed = time.time() - start_time

                    if stderr:
                        stderr_text = stderr.decode().strip()
                        self.console_logger.warning("◁ inline-script stderr: %s", stderr_text[:80])

                    if proc.returncode != 0:
                        self.error_logger.error("◁ inline-script failed: %s", stderr.decode().strip())
                        return None

                    result = stdout.decode().strip()
                    result_preview = result[:50] + "..." if len(result) > 50 else result
                    self.console_logger.info("◁ inline-script (%dB, %.1fs) %s", len(result), elapsed, result_preview)
                    return result

                except asyncio.TimeoutError:
                    self.error_logger.error("⊗ inline-script timeout: %ss exceeded", timeout)
                    proc.kill()
                    await proc.wait()
                    return None
                except asyncio.CancelledError:
                    self.console_logger.info("⊗ inline-script cancelled")
                    proc.kill()
                    await proc.wait()
                    raise
                except (subprocess.SubprocessError, OSError) as e:
                    self.error_logger.error("⊗ inline-script error: %s", e)
                    proc.kill()
                    await proc.wait()
                    return None
            except (FileNotFoundError, OSError, subprocess.SubprocessError) as e:
                self.error_logger.error("⊗ inline-script subprocess error: %s", e)
                return None
