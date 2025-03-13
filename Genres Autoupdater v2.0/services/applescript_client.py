#!/usr/bin/env python3

"""
AppleScript Client Module

This module provides an abstraction for executing AppleScript commands asynchronously.
It centralizes the logic for interacting with AppleScript via the `osascript` command,
handles errors, applies concurrency limits via a semaphore, and ensures non-blocking execution.

Example:
    >>> import asyncio
    >>> from applescript_client import AppleScriptClient
    >>> # Assume CONFIG is already loaded from my-config.yaml
    >>> client = AppleScriptClient(CONFIG)
    >>> async def test():
    ...     result = await client.run_script("fetch_tracks.applescript", args=["Some Artist"])
    ...     print(result)
    >>> asyncio.run(test())
"""

import asyncio
import logging
import os

from typing import List, Optional, Union

class AppleScriptClient:
    """
    A client to run AppleScript commands asynchronously using the osascript command.

    Attributes:
        config (dict): Configuration dictionary loaded from my-config.yaml.
        apple_scripts_dir (str): Directory containing AppleScript files.
        logger (logging.Logger): Logger for logging messages.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent AppleScript executions.
    """
    def __init__(self, config: dict, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        if not config.get("apple_scripts_dir"):
            raise ValueError("The 'apple_scripts_dir' key is missing in the configuration.")
        self.apple_scripts_dir = config.get("apple_scripts_dir")
        # Limit concurrent AppleScript calls; default value can be set in config under 'apple_script_concurrency'
        self.semaphore = asyncio.Semaphore(config.get("apple_script_concurrency", 5))
    
    async def run_script(self, script_name: str, args: Union[List[str], None] = None) -> Optional[str]:
        """
        Execute an AppleScript asynchronously and return its output.

        Args:
            script_name (str): Name of the AppleScript file.
            args (List[str], optional): List of arguments to pass to the script.

        Returns:
            Optional[str]: The output of the script, or None if an error occurred.

        Example:
            >>> result = await client.run_script("fetch_tracks.applescript", args=["Artist Name"])
            >>> print(result)
        """
        if not self.apple_scripts_dir:
            self.logger.error("AppleScript directory is not set.")
            return None
        script_path = os.path.join(self.apple_scripts_dir, script_name)
        if not os.path.exists(script_path):
            self.logger.error(f"AppleScript not found: {script_path}")
            return None
        
        # Build command list
        cmd = ["osascript", script_path]
        if args:
            cmd.extend(args)
        
        self.logger.info(f"Executing AppleScript: {' '.join(cmd)}")
        async with self.semaphore:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    self.logger.error(f"AppleScript failed: {stderr.decode().strip()}")
                    return None
                result = stdout.decode().strip()
                self.logger.debug(f"AppleScript result: {result}")
                return result
            except (OSError, asyncio.SubprocessError) as e:
                self.logger.error(f"Error running AppleScript '{script_name}': {e}", exc_info=True)
                return None