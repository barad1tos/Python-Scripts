#!/usr/bin/env python3
"""
Dependency Injection Container Module

Manages the lifecycle and dependency relationships between application services.
Centralizes service initialization, configuration, and proper shutdown procedures.
Provides access to configured service instances including loggers, analytics,
API clients, and core application components. Handles circular imports and
asynchronous resource management.
"""

import asyncio
import logging

from typing import TYPE_CHECKING, Any, Dict  # Import TYPE_CHECKING for type hinting

# Import concrete service implementations
from services.applescript_client import AppleScriptClient
from services.cache_service import CacheService
from services.external_api_service import ExternalApiService
from services.pending_verification import PendingVerificationService

# Import Analytics class
from utils.analytics import Analytics

# Assuming these service and utility classes exist and are needed as dependencies
from utils.config import load_config
from utils.logger import get_loggers

# Use TYPE_CHECKING to avoid circular import issues during runtime
# The actual import of MusicUpdater is done inside __init__
if TYPE_CHECKING:
    from music_genre_updater import MusicUpdater


class DependencyContainer:
    """
    Central container for managing application dependencies.

    Responsible for initializing services and injecting their dependencies.
    Provides access to configured service instances.
    """

    def __init__(self, config_path: str):
        """
        Initializes the dependency container by creating and wiring up services.

        Args:
            config_path: The path to the application configuration file.
        """
        self._config_path = config_path

        # 1. Load configuration first, as it's needed by other services
        self.config: Dict[str, Any] = load_config(self._config_path)

        # 2. Initialize loggers, they are dependencies for many services
        # get_loggers might start a listener thread; its lifecycle should be managed
        self.console_logger, self.error_logger, self.analytics_logger, self._listener = get_loggers(self.config)

        # Log successful container initialization
        self.console_logger.info("DependencyContainer initialized.")

        # 3. Initialize core services that don't have complex dependencies or are needed early
        self.analytics = Analytics(self.config, self.console_logger, self.error_logger, self.analytics_logger)
        # Assuming AppleScriptClient needs config and loggers
        self.ap_client = AppleScriptClient(self.config, self.console_logger, self.error_logger)
        # CacheService now also requires loggers based on the latest error
        self.cache_service = CacheService(self.config, self.console_logger, self.error_logger)
        # Assuming PendingVerificationService needs config and loggers
        self.pending_verification_service = PendingVerificationService(self.config, self.console_logger, self.error_logger)

        # 4. Initialize services that depend on others
        # ExternalAPIService depends on CacheService and PendingVerificationService based on the error
        self.external_api_service = ExternalApiService(
            self.config,
            self.console_logger,
            self.error_logger,
            self.cache_service,  # Injecting CacheService instance
            self.pending_verification_service,  # Injecting PendingVerificationService instance
        )

        # Import MusicUpdater here to break the circular dependency at the top level
        try:
            from music_genre_updater import MusicUpdater
        except ImportError as import_err:
            # Handle import error gracefully
            self.console_logger.error("Import error: %s", import_err, exc_info=True)
            # In a real application, you might raise a specific error or handle differently
            raise  # Re-raise the exception after logging

        # 5. Initialize the main orchestrator class (MusicUpdater)
        # It depends on many of the services initialized above
        self._music_updater_instance = MusicUpdater(
            self.config,
            self.console_logger,
            self.error_logger,
            self.analytics,
            self.ap_client,
            self.cache_service,
            self.external_api_service,
            self.pending_verification_service,
            # Add other dependencies here if MusicUpdater requires them
        )

        self.console_logger.debug("All core services initialized and wired.")

    # Use a forward reference 'MusicUpdater' for the type hint
    def get_music_updater(self) -> 'MusicUpdater':
        """
        Provides access to the configured MusicUpdater instance.
        """
        return self._music_updater_instance

    def get_analytics(self) -> Analytics:
        """
        Provides access to the configured Analytics instance.
        """
        return self.analytics

    def get_error_logger(self) -> logging.Logger:
        """
        Provides access to the configured error logger.
        """
        return self.error_logger

    def get_console_logger(self) -> logging.Logger:
        """
        Provides access to the configured console logger.
        """
        return self.console_logger

    # You might add other getter methods for other services if needed

    def shutdown(self) -> None:
        """
        Performs necessary cleanup, such as stopping the logging listener.
        """
        self.console_logger.info("Shutting down DependencyContainer...")
        # Stop the logging listener if it was started
        if self._listener:
            self.console_logger.info("Stopping QueueListener from DependencyContainer...")
            self._listener.stop()

        # Add any other shutdown logic for services if they require it
        # For example, closing connections or saving state.
        if hasattr(self.external_api_service, 'close'):
            # Assuming ExternalApiService has a close method
            self.console_logger.debug("Closing ExternalApiService...")
            try:
                # Await if the close method is async
                # Use _async_run helper if calling from a sync context
                self._async_run(self.external_api_service.close())
            except Exception as e:
                self.error_logger.error(f"Error during ExternalApiService shutdown: {e}", exc_info=True)

        self.console_logger.info("DependencyContainer shutdown complete.")

    # Helper to run async shutdown methods from sync context if needed
    def _async_run(self, coro):
        """Helper to run a coroutine in the current event loop."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If no loop is currently set, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        # Check if the loop is already running
        if loop.is_running():
            # Submit the coroutine as a task
            return asyncio.create_task(coro)
        else:
            # Run the coroutine until completion
            return loop.run_until_complete(coro)
