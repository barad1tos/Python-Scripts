#!/usr/bin/env python3
"""Dependency Injection Container Module.

Manages the lifecycle and dependency relationships between application services.
Centralizes service initialization, configuration, and proper shutdown procedures.
Provides access to configured service instances including loggers, analytics,
API clients, and core application components. Handles circular imports and
asynchronous resource management.

Refactored: Added an async initialize method to handle asynchronous service setup
like loading data from disk for CacheService and PendingVerificationService,
and semaphore creation for AppleScriptClient.
__init__ now receives loggers and listener as arguments.
"""

import asyncio
import logging

from collections.abc import Coroutine
from logging.handlers import QueueListener

# Import necessary types for type hinting
from typing import TYPE_CHECKING, Any

import yaml  # Import yaml for configuration loading


# Import concrete service implementations
# Note: Services needing async init are now instantiated but not fully initialized in __init__
from services.applescript_client import AppleScriptClient
from services.cache_service import CacheService
from services.external_api_service import ExternalApiService
from services.pending_verification import PendingVerificationService

# Import Analytics class
from utils.analytics import Analytics

# Assuming these service and utility classes exist and are needed as dependencies
from utils.config import load_config

# Removed unused import: from utils.logger import get_loggers

# Use TYPE_CHECKING to avoid circular import issues during runtime
# The actual import of MusicUpdater is done inside __init__
if TYPE_CHECKING:
    from music_genre_updater import MusicUpdater


class DependencyContainer:
    """Central container for managing application dependencies.

    Responsible for initializing services and injecting their dependencies.
    Provides access to configured service instances.
    Includes an async initialize method for services requiring async setup.
    __init__ now receives loggers and listener as arguments.
    """

    def __init__(
        self,
        config_path: str,
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        analytics_logger: logging.Logger,
        logging_listener: QueueListener | None,
    ):
        """Initializes the dependency container, receiving loggers and config path.

        Does NOT perform asynchronous setup here. Use the async initialize method.

        Args:
            config_path: The path to the application configuration file.
            console_logger: The logger for console output.
            error_logger: The logger for error messages.
            analytics_logger: The logger for analytics data.
            logging_listener: The initialized QueueListener for logging.
        """
        self._config_path = config_path
        # Store the passed logger instances and the listener
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics_logger = analytics_logger
        self._listener = logging_listener  # Store the listener to stop it later

        # 1. Load configuration first, as it's needed by other services
        # Since loggers are available, we can log config loading errors properly
        try:
            self.config: dict[str, Any] = load_config(self._config_path)
            self.console_logger.info("Configuration loaded successfully.")
        except (
            FileNotFoundError,
            ValueError,
            yaml.YAMLError,
        ) as e:  # Import yaml not needed here as load_config handles it
            self.error_logger.critical(
                f"Failed to load or validate configuration from {self._config_path}: {e}"
            )
            # Store empty config on failure, but re-raise the exception
            self.config = {}
            raise  # Re-raise the exception after logging

        self.console_logger.info("DependencyContainer initialized (sync part).")

        # 3. Initialize core services that don't have complex async dependencies in their constructor
        # They receive config and already initialized loggers
        self.analytics = Analytics(
            self.config, self.console_logger, self.error_logger, self.analytics_logger
        )

        # Assuming AppleScriptClient needs config and loggers, async init is separate
        self.ap_client = AppleScriptClient(
            self.config, self.console_logger, self.error_logger
        )

        # CacheService now requires loggers and config, async init is separate
        self.cache_service = CacheService(
            self.config, self.console_logger, self.error_logger
        )

        # Assuming PendingVerificationService needs config and loggers, async init is separate
        self.pending_verification_service = PendingVerificationService(
            self.config, self.console_logger, self.error_logger
        )

        # 4. Initialize services that depend on others. Async init is separate.
        # ExternalAPIService depends on CacheService and PendingVerificationService
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
        except (
            ImportError
        ) as _:  # Renamed variable to _ as it's unused and satisfies linter
            del _  # Clean up the unused variable
            self.error_logger.critical(
                "Import error: Could not import MusicUpdater. Check file paths and names.",
                exc_info=True,
            )
            raise  # Re-raise the exception after logging

        # 5. Initialize the main orchestrator class (MusicUpdater)
        # It depends on many of the services initialized above
        # MusicUpdater does NOT need async init itself, its methods are async.
        self._music_updater_instance = MusicUpdater(
            self.config,
            self.console_logger,
            self.error_logger,
            self.analytics,
            self.ap_client,
            self.cache_service,  # Injecting CacheService instance
            self.external_api_service,  # Injecting ExternalAPIService instance
            self.pending_verification_service,  # Injecting PendingVerificationService instance
            # Add other dependencies here if MusicUpdater requires them
        )

        self.console_logger.debug(
            "All core services initialized and wired (sync part)."
        )

    async def initialize(self) -> None:
        """Asynchronously initializes services within the container that require async setup.

        This method must be called after the DependencyContainer is instantiated.
        """
        self.console_logger.info(
            "Asynchronously initializing DependencyContainer services..."
        )

        # Initialize services that require async setup

        # CacheService requires async initialization to load its persistent cache
        if hasattr(self.cache_service, "initialize") and callable(
            self.cache_service.initialize
        ):
            self.console_logger.debug("Calling CacheService async initialize...")
            await self.cache_service.initialize()
        else:
            self.error_logger.warning(
                "CacheService instance has no async initialize method or it's not callable."
            )

        # PendingVerificationService requires async initialization to load its data
        if hasattr(self.pending_verification_service, "initialize") and callable(
            self.pending_verification_service.initialize
        ):
            self.console_logger.debug(
                "Calling PendingVerificationService async initialize..."
            )
            await self.pending_verification_service.initialize()
        else:
            self.error_logger.warning(
                "PendingVerificationService instance has no async initialize method or it's not callable."
            )

        # ExternalAPIService requires async initialization (e.g. for aiohttp session)
        # Initialize it if it has the method
        if hasattr(self.external_api_service, "initialize") and callable(
            self.external_api_service.initialize
        ):
            self.console_logger.debug("Calling ExternalAPIService async initialize...")
            # ExternalAPIService.initialize takes optional force arg, pass False as default for DI init
            await self.external_api_service.initialize(force=False)
        else:
            self.error_logger.warning(
                "ExternalAPIService instance has no async initialize method or it's not callable."
            )

        # AppleScriptClient now requires async initialization to create its semaphore
        if hasattr(self.ap_client, "initialize") and callable(
            self.ap_client.initialize
        ):
            self.console_logger.debug("Calling AppleScriptClient async initialize...")
            await self.ap_client.initialize()
        else:
            self.error_logger.warning(
                "AppleScriptClient instance has no async initialize method or it's not callable."
            )

        # Add calls to async initialize methods of other services here if they are added later

        self.console_logger.info(
            "DependencyContainer services asynchronous initialization complete."
        )

    # Use a forward reference 'MusicUpdater' for the type hint
    def get_music_updater(self) -> "MusicUpdater":
        """Provides access to the configured MusicUpdater instance."""
        return self._music_updater_instance

    def get_analytics(self) -> Analytics:
        """Provides access to the configured Analytics instance."""
        return self.analytics

    def get_error_logger(self) -> logging.Logger:
        """Provides access to the configured error logger."""
        return self.error_logger

    def get_console_logger(self) -> logging.Logger:
        """Provides access to the configured console logger."""
        return self.console_logger

    # Add other getter methods for other services if needed by the main script

    def shutdown(self) -> None:
        """Performs necessary cleanup, such as stopping the logging listener and shutting down services that require cleanup.

        This method is called from a synchronous context (main function finally block).
        If service shutdown is async, need to handle it using _async_run helper.
        """
        self.console_logger.info("Shutting down DependencyContainer...")

        # Add shutdown logic for services that require it
        # ExternalAPIService needs its session closed, which is async.
        # Use the _async_run helper method.
        if hasattr(self.external_api_service, "close") and callable(
            self.external_api_service.close
        ):
            self.console_logger.debug("Calling ExternalApiService async close...")
            try:
                self._async_run(self.external_api_service.close())
            except Exception as e:
                self.error_logger.error(
                    f"Error during ExternalApiService shutdown: {e}", exc_info=True
                )
        else:
            self.console_logger.debug(
                "ExternalAPIService instance has no async close method or it's not callable."
            )

        # CacheService has a sync_cache method that uses run_in_executor, so it's async in practice.
        # Call it using _async_run to ensure final data is saved.
        if hasattr(self.cache_service, "sync_cache") and callable(
            self.cache_service.sync_cache
        ):
            self.console_logger.debug("Calling CacheService async sync_cache...")
            try:
                self._async_run(self.cache_service.sync_cache())
            except Exception as e:
                self.error_logger.error(
                    f"Error during CacheService sync_cache on shutdown: {e}",
                    exc_info=True,
                )
        else:
            self.console_logger.debug(
                "CacheService instance has no async sync_cache method or it's not callable."
            )

        # PendingVerificationService saves on mark/remove. A final save might be redundant
        # but could be added if it had a general save method and it used run_in_executor.
        # Let's rely on saves during modifications for now.

        # Stop the logging listener if it was started
        # The listener is now passed into the constructor and stored.
        if self._listener:
            self.console_logger.info(
                "Stopping QueueListener from DependencyContainer..."
            )
            # The listener.stop() method itself should be thread-safe and non-blocking sync.
            self._listener.stop()
        # Note: Closing individual handlers is typically done after stopping the listener
        # in the main script's finally block.

        self.console_logger.info("DependencyContainer shutdown complete.")

    # Helper to run async methods from sync context (like shutdown)
    # Correctly implemented helper
    def _async_run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Helper to run a coroutine in the current event loop or a new one if needed.

        This helper is called from the sync shutdown method.
        It needs to ensure the coroutine runs to completion.

        Args:
            coro: The coroutine to run.

        Returns:
            Any: The result of the coroutine.
        """
        try:
            # Get the current running loop or create a new one
            try:
                loop = asyncio.get_event_loop_policy().get_event_loop()
                if loop.is_closed():
                    raise RuntimeError(
                        "Event loop is closed"
                    )  # Treat closed loop as "no usable loop"
            except RuntimeError:
                # No running loop or loop is closed, create a new one
                loop = asyncio.new_event_loop()
                # asyncio.set_event_loop(loop) # Not necessary globally for this use case
                created_new_loop = True
                self.console_logger.debug("_async_run: Created new event loop.")
            else:
                created_new_loop = False
                self.console_logger.debug("_async_run: Using existing event loop.")

            # Run the coroutine until completion in the determined loop
            try:
                # Use asyncio.run_until_complete for running from a sync context
                return loop.run_until_complete(coro)
            except asyncio.CancelledError:
                self.console_logger.warning(
                    "_async_run: Async coroutine was cancelled."
                )
                # Do not re-raise, let the shutdown continue
            except Exception as run_e:
                self.error_logger.error(
                    f"_async_run: Error running async coroutine: {run_e}", exc_info=True
                )
                # Do not re-raise, let the shutdown continue

            finally:
                # Close the new loop if it was created
                if created_new_loop:
                    try:
                        # Clean up the new loop
                        loop.close()
                        self.console_logger.debug("_async_run: Closed new event loop.")
                    except Exception as close_e:
                        self.error_logger.error(
                            f"_async_run: Error closing new event loop: {close_e}",
                            exc_info=True,
                        )

        except Exception as outer_e:
            # Catch errors even before trying to get/create loop
            self.error_logger.error(
                f"_async_run: Unexpected error in helper logic: {outer_e}",
                exc_info=True,
            )
