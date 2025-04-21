#!/usr/bin/env python3

"""
Configuration Management Module

Provides schema definition and validation for the application's configuration.
Includes a comprehensive schema for music library settings, performance parameters,
feature toggles, logging configuration, and music metadata retrieval settings.
Handles loading configuration from YAML files with strict validation.
"""

import os
import sys

import yaml

from cerberus import Validator

# Define the schema for the configuration file
# This schema describes the expected structure, data types, and constraints
CONFIG_SCHEMA = {
    # 1. MAIN PATHS AND ENVIRONMENT
    "music_library_path": {"type": "string", "required": True},
    "apple_scripts_dir": {"type": "string", "required": True},
    "logs_base_dir": {"type": "string", "required": True},
    "python_settings": {"type": "dict", "required": True, "schema": {"prevent_bytecode": {"type": "boolean", "required": True}}},
    # 2. EXECUTION AND PERFORMANCE
    "apple_script_concurrency": {"type": "integer", "required": True, "min": 1},
    "applescript_timeout_seconds": {"type": "integer", "required": True, "min": 1},
    "max_retries": {"type": "integer", "required": True, "min": 0},
    "retry_delay_seconds": {"type": "number", "required": True, "min": 0},  # Use 'number' for float or int
    "incremental_interval_minutes": {"type": "integer", "required": True, "min": 1},
    "cache_ttl_seconds": {"type": "integer", "required": True, "min": 0},
    "album_cache_sync_interval": {"type": "integer", "required": True, "min": 0},
    # 3. FEATURE TOGGLES AND SETTINGS
    "cleaning": {
        "type": "dict",
        "required": True,
        "schema": {
            "remaster_keywords": {"type": "list", "required": True, "schema": {"type": "string"}},
            "album_suffixes_to_remove": {"type": "list", "required": True, "schema": {"type": "string"}},
        },
    },
    "exceptions": {
        "type": "dict",
        "required": True,
        "schema": {
            "track_cleaning": {
                "type": "list",
                "required": True,
                "schema": {"type": "dict", "schema": {"artist": {"type": "string", "required": True}, "album": {"type": "string", "required": True}}},
            }
        },
    },
    "database_verification": {
        "type": "dict",
        "required": True,
        "schema": {
            "auto_verify_days": {"type": "integer", "required": True, "min": 0},
            "batch_size": {"type": "integer", "required": True, "min": 1},
        },
    },
    "development": {"type": "dict", "required": True, "schema": {"test_artists": {"type": "list", "required": True, "schema": {"type": "string"}}}},
    # 4. LOGGING AND ANALYTICS CONFIGURATION
    "logging": {
        "type": "dict",
        "required": True,
        "schema": {
            "max_runs": {"type": "integer", "required": True, "min": 0},
            "main_log_file": {"type": "string", "required": True},
            "analytics_log_file": {"type": "string", "required": True},
            "csv_output_file": {"type": "string", "required": True},
            "album_cache_csv": {"type": "string", "required": True},
            "changes_report_file": {"type": "string", "required": True},
            "dry_run_report_file": {"type": "string", "required": True},
            "last_incremental_run_file": {"type": "string", "required": True},
            "pending_verification_file": {"type": "string", "required": True},
            "last_db_verify_log": {"type": "string", "required": True},
            "levels": {
                "type": "dict",
                "required": True,
                "schema": {
                    "console": {"type": "string", "required": True, "allowed": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"]},
                    "main_file": {"type": "string", "required": True, "allowed": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"]},
                    "analytics_file": {"type": "string", "required": True, "allowed": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"]},
                    "year_updates_file": {"type": "string", "required": True, "allowed": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET"]},
                    # Add other log file levels here if you add more handlers in logger.py
                },
            },
        },
    },
    "analytics": {
        "type": "dict",
        "required": True,
        "schema": {
            "duration_thresholds": {
                "type": "dict",
                "required": True,
                "schema": {
                    "short_max": {"type": "number", "required": True, "min": 0},
                    "medium_max": {"type": "number", "required": True, "min": 0},
                    "long_max": {"type": "number", "required": True, "min": 0},
                },
            },
            "max_events": {"type": "integer", "required": True, "min": 0},
            "compact_time": {"type": "boolean", "required": True},
        },
    },
    # 5. YEAR RETRIEVAL CONFIGURATION
    "year_retrieval": {
        "type": "dict",
        "required": True,
        "schema": {
            "enabled": {"type": "boolean", "required": True},
            "preferred_api": {"type": "string", "required": True, "allowed": ["musicbrainz", "discogs", "lastfm"]},  # Add other APIs if used
            "api_auth": {
                "type": "dict",
                "required": True,
                "schema": {
                    "discogs_token": {"type": "string", "required": True},
                    "musicbrainz_app_name": {"type": "string", "required": True},
                    "contact_email": {"type": "string", "required": True},  # Could add regex validation for email format
                    "use_lastfm": {"type": "boolean", "required": True},
                    "lastfm_api_key": {"type": "string", "required": True},
                },
            },
            "rate_limits": {
                "type": "dict",
                "required": True,
                "schema": {
                    "discogs_requests_per_minute": {"type": "integer", "required": True, "min": 1},
                    "musicbrainz_requests_per_second": {"type": "number", "required": True, "min": 0},
                    "lastfm_requests_per_second": {"type": "number", "required": True, "min": 0},
                    "concurrent_api_calls": {"type": "integer", "required": True, "min": 1},
                },
            },
            "processing": {
                "type": "dict",
                "required": True,
                "schema": {
                    "batch_size": {"type": "integer", "required": True, "min": 1},
                    "delay_between_batches": {"type": "number", "required": True, "min": 0},
                    "adaptive_delay": {"type": "boolean", "required": True},
                    "cache_ttl_days": {"type": "integer", "required": True, "min": 0},
                    "pending_verification_interval_days": {"type": "integer", "required": True, "min": 0},
                },
            },
            "logic": {
                "type": "dict",
                "required": True,
                "schema": {
                    "min_valid_year": {"type": "integer", "required": True, "min": 1000},  # Assuming no music before 1000 AD
                    "definitive_score_threshold": {"type": "number", "required": True, "min": 0, "max": 100},
                    "definitive_score_diff": {"type": "number", "required": True, "min": 0},
                    "preferred_countries": {"type": "list", "required": True, "schema": {"type": "string"}},
                    "major_market_codes": {"type": "list", "required": True, "schema": {"type": "string"}},
                },
            },
            "reissue_detection": {
                "type": "dict",
                "required": True,
                "schema": {"reissue_keywords": {"type": "list", "required": True, "schema": {"type": "string"}}},
            },
            "scoring": {
                "type": "dict",
                "required": True,
                "schema": {
                    "base_score": {"type": "number", "required": True},
                    "artist_exact_match_bonus": {"type": "number", "required": True},
                    "album_exact_match_bonus": {"type": "number", "required": True},
                    "perfect_match_bonus": {"type": "number", "required": True},
                    "album_variation_bonus": {"type": "number", "required": True},
                    "album_substring_penalty": {"type": "number", "required": True, "max": 0},
                    "album_unrelated_penalty": {"type": "number", "required": True, "max": 0},
                    "mb_release_group_match_bonus": {"type": "number", "required": True},
                    "type_album_bonus": {"type": "number", "required": True},
                    "type_ep_single_penalty": {"type": "number", "required": True, "max": 0},
                    "type_compilation_live_penalty": {"type": "number", "required": True, "max": 0},
                    "status_official_bonus": {"type": "number", "required": True},
                    "status_bootleg_penalty": {"type": "number", "required": True, "max": 0},
                    "status_promo_penalty": {"type": "number", "required": True, "max": 0},
                    "reissue_penalty": {"type": "number", "required": True, "max": 0},
                    "year_diff_penalty_scale": {"type": "number", "required": True, "max": 0},
                    "year_diff_max_penalty": {"type": "number", "required": True, "max": 0},
                    "year_before_start_penalty": {"type": "number", "required": True, "max": 0},
                    "year_after_end_penalty": {"type": "number", "required": True, "max": 0},
                    "year_near_start_bonus": {"type": "number", "required": True},
                    "country_artist_match_bonus": {"type": "number", "required": True},
                    "country_major_market_bonus": {"type": "number", "required": True},
                    "source_mb_bonus": {"type": "number", "required": True},
                    "source_discogs_bonus": {"type": "number", "required": True},
                    "source_lastfm_penalty": {"type": "number", "required": True, "max": 0},
                },
            },
        },
    },
}


def load_config(config_path: str):
    """
    Load the configuration from a YAML file and validate it against a schema.

    Args:
        config_path: Path to the configuration YAML file.

    Returns:
        dict: Dictionary containing the validated configuration.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the configuration is invalid according to the schema.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file {config_path} does not exist.")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        # Re-raise YAML parsing errors
        print(f"ERROR: Failed to parse YAML config file {config_path}: {e}", file=sys.stderr)
        raise

    # Validate the loaded data against the schema
    validator = Validator(CONFIG_SCHEMA)

    if not validator.validate(config_data):
        # If validation fails, raise a ValueError with detailed errors
        error_messages = ["Configuration validation failed:"]
        for field, errors in validator.errors.items():
            error_messages.append(f"  Field '{field}': {', '.join(errors)}")
        full_error_message = "\n".join(error_messages)
        print(f"ERROR: {full_error_message}", file=sys.stderr)
        raise ValueError(full_error_message)

    # Return the validated data
    return config_data
