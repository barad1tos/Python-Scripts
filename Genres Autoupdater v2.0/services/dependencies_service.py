#!/usr/bin/env python3

"""
Dependency Container Module

This module defines a DependencyContainer class that encapsulates all key dependencies
including configuration, loggers, analytics, AppleScriptClient, CacheService,
ExternalApiService, and PendingVerificationService. It implements a simple
dependency injection pattern for the application.
"""

import os

import yaml

from services.applescript_client import AppleScriptClient
from services.cache_service import CacheService
from services.external_api_service import ExternalApiService
from services.pending_verification import PendingVerificationService
from utils.analytics import Analytics
from utils.logger import get_loggers

_ANALYTICS_INSTANCE = None


def get_analytics_instance():
    """
    Accessor function for the analytics instance.

    Returns:
        Analytics: Current analytics instance or None if not initialized.

    Example:
        >>> analytics = get_analytics_instance()
        >>> if analytics:
        >>>     analytics.track_event("user_login")
    """
    return _ANALYTICS_INSTANCE


class DependencyContainer:
    """
    A container for all dependencies used in the application.
    This implements a simple dependency injection pattern.
    """

    def __init__(self, config_path: str):
        """
        Initializes the DependencyContainer with the configuration file path.

        Args:
            config_path: Path to the configuration YAML file.

        Example:
            >>> deps = DependencyContainer("my-config.yaml")
            >>> client = deps.ap_client
            >>> client.run_script("fetch_tracks.applescript")
        """
        # Load configuration first
        self.config = self.load_config(config_path)  # Updated to call the method with self

        # Initialize loggers
        self.console_logger, self.error_logger, self.analytics_logger, self.listener = get_loggers(self.config)

        # Initialize analytics
        self.analytics = Analytics(self.config, self.console_logger, self.error_logger, self.analytics_logger)

        # Make the analytics object available for external use
        global _ANALYTICS_INSTANCE
        _ANALYTICS_INSTANCE = self.analytics

        # Initialize the AppleScript client
        self.ap_client = AppleScriptClient(self.config, self.console_logger, self.error_logger)

        # Initialize the cache service
        self.cache_service = CacheService(self.config, self.console_logger, self.error_logger)

        # Initialize the external API service
        self.external_api_service = ExternalApiService(self.config, self.console_logger, self.error_logger)

        # Initialize the pending verification service
        self.pending_verification_service = PendingVerificationService(self.config, self.console_logger, self.error_logger)

        # Initialize API client session
        self.external_api_service.initialize_async = self.external_api_service.initialize

    @staticmethod
    def load_config(config_path: str):
        """
        Load the configuration from a YAML file.

        Args:
            config_path: Path to the configuration YAML file.

        Returns:
            dict: Dictionary containing the configuration.

        Raises:
            FileNotFoundError: If the config file does not exist.

        Example:
            >>> config = DependencyContainer.load_config("my-config.yaml")
            >>> apple_scripts_dir = config.get("apple_scripts_dir")
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file {config_path} does not exist.")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
