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
from utils.analytics import Analytics
from utils.logger import get_loggers


class DependencyContainer:
    """
    A container for all dependencies used in the application.
    This implements a simple dependency injection pattern.
    """

    def __init__(self, config_path: str):
        """
        Initializes the DependencyContainer with the configuration file path.

        :param config_path: Path to the configuration YAML file.
        """
        # Load configuration first
        from music_genre_updater import load_config

        self.config = load_config(config_path)

        # Initialize loggers
        self.console_logger, self.error_logger, self.analytics_logger = get_loggers(self.config)

        # Initialize analytics - this was missing!
        self.analytics = Analytics(self.config, self.console_logger, self.error_logger, self.analytics_logger)

        # Make the analytics object available for external use
        global analytics
        analytics = self.analytics

        # Initialize the AppleScript client
        self.ap_client = AppleScriptClient(self.config, self.console_logger, self.error_logger)

        # Initialize the cache service
        self.cache_service = CacheService(self.config, self.console_logger, self.error_logger)

        # Initialize the external API service
        self.external_api_service = ExternalApiService(self.config, self.console_logger, self.error_logger)

        # Initialize the pending verification service
        from services.pending_verification import PendingVerificationService

        self.pending_verification_service = PendingVerificationService(
            self.config, self.console_logger, self.error_logger
        )

        # Initialize API client session
        self.external_api_service.initialize_async = self.external_api_service.initialize

    @staticmethod
    def load_config(config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file {config_path} does not exist.")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
