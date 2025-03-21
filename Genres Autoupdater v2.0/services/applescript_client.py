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
            self.error_logger.error(f"Script not found: {script_name}")
            return None
        
        # Build command list
        cmd = ["osascript", script_path]
        if arguments:
            cmd.extend(arguments)
        
        # Format arguments for logging
        args_str = f"args: {', '.join(f'{a}' for a in arguments)}" if arguments else "no args"
        self.console_logger.info(f"▷ {script_name} ({args_str}) [t:{timeout}s]")
        
        async with self.semaphore:
            try:
                # First, create a process
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                # Then we wait for the result with Timeout
                try:
                    start_time = time.time()
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                    elapsed = time.time() - start_time
                    
                    if stderr:
                        stderr_text = stderr.decode().strip()
                        self.console_logger.warning(f"◁ {script_name} stderr: {stderr_text[:80]}")
                    
                    if proc.returncode != 0:
                        self.error_logger.error(f"◁ {script_name} failed: {stderr.decode().strip()}")
                        return None
                    
                    result = stdout.decode().strip()
                    result_preview = result[:50] + "..." if len(result) > 50 else result
                    self.console_logger.info(f"◁ {script_name} ({len(result)}B, {elapsed:.1f}s) {result_preview}")
                    return result
                    
                except asyncio.TimeoutError:
                    self.error_logger.error(f"⊗ {script_name} timeout: {timeout}s exceeded")
                    proc.kill()
                    await proc.wait()
                    return None
                except asyncio.CancelledError:
                    self.console_logger.info(f"⊗ {script_name} cancelled")
                    proc.kill()
                    await proc.wait()
                    raise
                except Exception as e:
                    self.error_logger.error(f"⊗ {script_name} error: {e}")
                    proc.kill()
                    await proc.wait()
                    return None
            except Exception as e:
                self.error_logger.error(f"⊗ {script_name} subprocess error: {e}")
                return None
    
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
        
        code_preview = script_code[:30] + "..." if len(script_code) > 30 else script_code
        self.console_logger.info(f"▷ inline-script ({len(script_code)}B) [t:{timeout}s]")
        
        async with self.semaphore:
            try:
                start_time = time.time()
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                # Wait with timeout
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                    elapsed = time.time() - start_time
                    
                    if stderr:
                        stderr_text = stderr.decode().strip()
                        self.console_logger.warning(f"◁ inline-script stderr: {stderr_text[:80]}")
                        
                    if proc.returncode != 0:
                        self.error_logger.error(f"◁ inline-script failed: {stderr.decode().strip()}")
                        return None

                    result = stdout.decode().strip()
                    result_preview = result[:50] + "..." if len(result) > 50 else result
                    self.console_logger.info(f"◁ inline-script ({len(result)}B, {elapsed:.1f}s) {result_preview}")
                    return result
                    
                except asyncio.TimeoutError:
                    self.error_logger.error(f"⊗ inline-script timeout: {timeout}s exceeded")
                    proc.kill()
                    await proc.wait()
                    return None
                except asyncio.CancelledError:
                    self.console_logger.info(f"⊗ inline-script cancelled")
                    proc.kill()
                    await proc.wait()
                    raise
                except Exception as e:
                    self.error_logger.error(f"⊗ inline-script error: {e}")
                    proc.kill()
                    await proc.wait()
                    return None
            except Exception as e:
                self.error_logger.error(f"⊗ inline-script subprocess error: {e}")
                return None