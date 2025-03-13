#!/usr/bin/env python3

"""
Dependency Container Module

This module defines a DependencyContainer class that encapsulates key dependencies
such as configuration, AppleScriptClient, CacheService, and any other services.
"""

import os

import yaml

from services.applescript_client import AppleScriptClient
from services.cache_service import CacheService
from utils.analytics import Analytics
from utils.logger import get_loggers

class DependencyContainer:
    def __init__(self, config_path: str):
        # Load configuration from YAML
        self.config = self.load_config(config_path)
        
        # Initialize loggers
        self.console_logger, self.error_logger, self.analytics_logger = get_loggers(self.config)
        
        # Initialize AppleScript client
        self.ap_client = AppleScriptClient(self.config, logger=self.console_logger)
        
        # Initialize CacheService with TTL from configuration
        self.cache_service = CacheService(ttl=self.config.get("cache_ttl_seconds", 900))
        
        # Initialize Analytics service
        self.analytics = Analytics(self.config, self.console_logger, self.error_logger, self.analytics_logger)

    @staticmethod
    def load_config(config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file {config_path} does not exist.")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)