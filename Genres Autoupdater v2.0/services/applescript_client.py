#!/usr/bin/env python3

"""
AppleScript Client Module

This module provides an abstraction for executing AppleScript commands asynchronously.
It centralizes the logic for interacting with AppleScript via the `osascript` command,
handles errors, applies concurrency limits via a semaphore, and ensures non-blocking execution.

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
    ...     result = await client.run_script("fetch_tracks.applescript", args=["Some Artist"])
    ...     print(result)
    >>> asyncio.run(test())
"""

import asyncio
import logging
import os
import subprocess

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
    
    async def run_script(self, script_name: str, arguments: Union[List[str], None] = None, 
                        timeout: Optional[float] = None) -> Optional[str]:
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
            self.error_logger.error(f"AppleScript not found: {script_path}")
            return None
        
        # Build command list
        cmd = ["osascript", script_path]
        if arguments:
            cmd.extend(arguments)
        
        self.console_logger.info(f"Executing AppleScript: {' '.join(cmd)}")
        async with self.semaphore:
            try:
                # First, create a process
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                # Then we wait for the result with Taimut
                self.console_logger.info(f"Running script with timeout {timeout}s: {script_name}")
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                except asyncio.TimeoutError:
                    self.error_logger.error(f"Timeout ({timeout}s) exceeded while executing {script_name}")
                    proc.kill()
                    await proc.wait()
                    return None
                except asyncio.CancelledError:
                    self.error_logger.info(f"AppleScript '{script_name}' execution cancelled. Killing subprocess.")
                    proc.kill()
                    await proc.wait()
                    raise
                except Exception as e:
                    self.error_logger.error(f"Error during subprocess communication for '{script_name}': {e}", exc_info=True)
                    proc.kill()
                    await proc.wait()
                    return None
            except Exception as e:
                self.error_logger.error(f"Error creating subprocess for '{script_name}': {e}", exc_info=True)
                return None

            if stderr:
                self.console_logger.warning(f"stderr from {script_name}: {stderr.decode().strip()}")
            
            if proc.returncode != 0:
                self.error_logger.error(f"AppleScript failed: {stderr.decode().strip()}")
                return None
            
            result = stdout.decode().strip()
            self.console_logger.debug(f"AppleScript result length: {len(result)} bytes")
            return result
    
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
        
        self.console_logger.info(f"Executing AppleScript code: {script_code[:50]}...")
        async with self.semaphore:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                # Wait with timeout
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                except asyncio.TimeoutError:
                    self.error_logger.error(f"Timeout ({timeout}s) exceeded while executing script code")
                    proc.kill()
                    await proc.wait()
                    return None
                except asyncio.CancelledError:
                    self.error_logger.info("AppleScript code execution cancelled. Killing subprocess.")
                    proc.kill()
                    await proc.wait()
                    raise
                except Exception as e:
                    self.error_logger.error(f"Error during subprocess communication: {e}", exc_info=True)
                    proc.kill()
                    await proc.wait()
                    return None
            except Exception as e:
                self.error_logger.error(f"Error creating subprocess for script code: {e}", exc_info=True)
                return None

            if proc.returncode != 0:
                self.error_logger.error(f"AppleScript failed: {stderr.decode().strip()}")
                return None

            result = stdout.decode().strip()
            self.console_logger.debug(f"AppleScript result length: {len(result)} bytes")
            return result