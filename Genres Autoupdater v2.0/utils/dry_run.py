#!/usr/bin/env python3

"""Dry Run Module.

This module provides a dry run simulation for cleaning and genre updates.
It defines classes that can be used by the main application to simulate
AppleScript interactions and processing logic without modifying the actual
music library.
"""

from __future__ import annotations

# Standard library imports
import asyncio
import logging

from typing import Any

from services.applescript_client import AppleScriptClient, AppleScriptClientProtocol

DRY_RUN_SUCCESS_MESSAGE = "Success (dry run)"


class DryRunAppleScriptClient(AppleScriptClientProtocol):
    """AppleScript client that logs actions instead of modifying the library."""

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
    ) -> None:
        """Initialize the DryRunAppleScriptClient with dependencies."""
        self._real_client: AppleScriptClient = AppleScriptClient(config, console_logger, error_logger)
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.config = config
        self.actions: list[dict[str, Any]] = []

    async def initialize(self) -> None:
        """Initialize the DryRunAppleScriptClient."""
        await self._real_client.initialize()

    async def run_script(
        self,
        script_name: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run an AppleScript by name in dry run mode."""
        if script_name.startswith("fetch"):
            result = await self._real_client.run_script(script_name, arguments, timeout)
            if not isinstance(result, str | None):
                self.error_logger.warning(
                    "Unexpected return type from _real_client.run_script: %s",
                    type(result).__name__,
                )
                return None
            return result
        self.console_logger.info(
            "DRY-RUN: Would run %s with args: %s",
            script_name,
            arguments or [],
        )
        self.actions.append({"script": script_name, "args": arguments or []})
        return DRY_RUN_SUCCESS_MESSAGE

    async def run_script_code(
        self,
        script_code: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run raw AppleScript code in dry run mode."""
        async def _execute() -> str:
            self.console_logger.info("DRY-RUN: Would execute inline AppleScript")
            self.actions.append({"code": script_code, "args": arguments or []})
            return DRY_RUN_SUCCESS_MESSAGE
        try:
            if timeout is not None:
                return await asyncio.wait_for(_execute(), timeout=timeout)
            return await _execute()
        except TimeoutError:
            self.error_logger.error(
                "Timeout after %s seconds while executing AppleScript", timeout
            )
            return None
        except Exception as e:
            self.error_logger.error(
                "Error executing AppleScript: %s", str(e), exc_info=True
            )
            return None

    def get_actions(self) -> list[dict[str, Any]]:
        """Get the list of actions performed during the dry run."""
        return self.actions
