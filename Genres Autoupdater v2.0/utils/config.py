#!/usr/bin/env python3

"""
Configuration Loader Module

Provides functions for loading configuration from YAML files with validation.
"""

import os

import yaml


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
        >>> config = load_config("my-config.yaml")
        >>> apple_scripts_dir = config.get("apple_scripts_dir")
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file {config_path} does not exist.")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
