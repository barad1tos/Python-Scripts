#!/usr/bin/env python3

"""Music Genre Updater Script v3.0.

This script automatically manages your Music.app library by updating genres, cleaning track/album names, retrieving album years,
and verifying track database integrity. It uses dependency injection and asynchronous operations for improved performance.

Key Features:
- Genre harmonization: Assigns consistent genres to an artist's tracks based on their earliest releases
- Track/album name cleaning: Removes remaster tags, promotional text, and other clutter from titles
- Album year retrieval: Fetches and updates year information from external music databases with caching and rate limiting
- Database verification: Checks the internal track database against Music.app and removes entries for non-existent tracks
- Incremental processing: Updates only tracks added since the last run, respecting a configurable interval
- Analytics tracking: Monitors performance and operations with detailed logging
- Pending verification: Re-checks album years after a set period to improve accuracy

Architecture:
- MusicUpdater: Core orchestrator class that manages all library updates through injected dependencies
- DependencyContainer: Manages service instances and their lifecycle
- Services: AppleScriptClient, CacheService, ExternalApiService, PendingVerificationService
- Utility functions: For parsing tracks, cleaning names, determining genres, and handling reports

Commands:
- Default: Runs the main pipeline (clean, genres, years)
- clean_artist: Cleans names for a specific artist's tracks
- update_years: Updates album years, optionally filtered by artist
- verify_database: Validates the CSV database against Music.app
- verify_pending: Triggers verification for albums marked as pending

Options:
- --force: Overrides incremental checks and forces cache refresh
- --dry-run: Simulates changes without modifying the library
- --artist: Specifies target artist (for certain commands)

Configuration in my-config.yaml controls paths, API limits, cleaning rules, intervals, and logging preferences.
"""

import argparse
import asyncio
import logging
import os
import sys
import time

from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import yaml

from dotenv import load_dotenv

# Import necessary services and utilities
from utils import dry_run  # dry_run will be refactored later
from utils.analytics import Analytics  # Analytics is used in method decorators
from utils.config import load_config

# Import get_full_log_path from logger for use in main/methods, but NOT get_loggers here
from utils.logger import (  # get_loggers is only called in main
    get_full_log_path,
    get_loggers,
)

# Import utility functions from the metadata_helpers module
from utils.metadata import (
    clean_names,
    determine_dominant_genre_for_artist,
    group_tracks_by_artist,
    is_music_app_running,
    parse_tracks,
)

# Import the DependencyContainer for service management
from utils.reports import (
    load_track_list,
    save_changes_report,
    save_to_csv,
    sync_track_list_with_current,
)

# Use TYPE_CHECKING for services imported for type hints in method signatures/class definition
if TYPE_CHECKING:
    from services.applescript_client import AppleScriptClient
    from services.cache_service import CacheService
    from services.external_api_service import ExternalApiService
    from services.pending_verification import PendingVerificationService


# Define SCRIPT_DIR and CONFIG_PATH
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "my-config.yaml")

# Load environment variables from .env file
print("\n[ENV] Loading environment variables...")
load_dotenv()

# Debug: Print important environment variables
print(f"[ENV] Current working directory: {os.getcwd()}")
print(f"[ENV] .env file path: {os.path.join(os.getcwd(), '.env')}")
print(f"[ENV] DISCOGS_TOKEN: {'***set***' if os.getenv('DISCOGS_TOKEN') else 'MISSING'}")
print(f"[ENV] CONTACT_EMAIL: {os.getenv('CONTACT_EMAIL', 'MISSING')}")
print(f"[ENV] LASTFM_API_KEY: {'***set***' if os.getenv('LASTFM_API_KEY') else 'MISSING'}")


def resolve_env_vars(config: dict[str, Any] | list[Any] | Any) -> Any:
    """Recursively resolve environment variables in config values."""
    if isinstance(config, dict):
        return {k: resolve_env_vars(v) for k, v in config.items()}
    if isinstance(config, list):
        return [resolve_env_vars(item) for item in config]
    if isinstance(config, str) and config.startswith("${") and config.endswith("}"):
        env_var = config[2:-1]  # Remove ${ and }
        return os.getenv(env_var, "")
    return config


# Load environment variables from .env file if it exists
load_dotenv()

# Load and process CONFIG
try:
    # Load and validate the config (environment variables are resolved inside load_config)
    CONFIG = load_config(CONFIG_PATH)

    # Log important config values for debugging
    print("\n[CONFIG] Important configuration values:")
    print(f"- Music library path: {CONFIG.get('music_library_path')}")

    # Log API configuration
    if "year_retrieval" in CONFIG and CONFIG["year_retrieval"].get("enabled", False):
        api_auth = CONFIG["year_retrieval"].get("api_auth", {})
        print("\n[CONFIG] API Configuration:")
        print(
            f"- Discogs token: {'***set***' if api_auth.get('discogs_token') else 'MISSING'}",
        )
        print(f"- Contact email: {api_auth.get('contact_email') or 'MISSING'}")
        print(
            f"- Last.fm API key: {'***set***' if api_auth.get('lastfm_api_key') else 'MISSING'}",
        )
        print(f"- Use Last.fm: {api_auth.get('use_lastfm', False)}")

except (FileNotFoundError, ValueError, yaml.YAMLError) as e:
    print(
        f"\nFATAL ERROR: Failed to load or validate configuration: {e}",
        file=sys.stderr,
    )
    sys.exit(1)


# --- Utility functions (check_paths definition remains) ---
def check_paths(paths: list[str], logger: logging.Logger) -> None:
    """Check if the specified paths exist and are readable."""
    for path in paths:
        if not os.path.exists(path):
            logger.error(f"Path {path} does not exist.")
            raise FileNotFoundError(f"Path {path} does not exist.")
        if not os.access(path, os.R_OK):
            logger.error(f"No read access to {path}.")
            raise PermissionError(f"No read access to {path}.")


# --- MusicUpdater Class (Orchestrator) ---
# This class will contain the core logic previously in top-level async functions
class MusicUpdater:
    """Orchestrates the music library update process, using injected dependencies."""
    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        analytics: Analytics,
        ap_client: "AppleScriptClient",
        cache_service: "CacheService",
        external_api_service: "ExternalApiService",
        pending_verification_service: "PendingVerificationService",
        # Add other services here if needed in the future
    ):
        """Initializes the MusicUpdater with its dependencies."""
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics = analytics  # Store Analytics instance to use its decorator
        self.ap_client = ap_client
        self.cache_service = cache_service
        self.external_api_service = external_api_service
        self.pending_verification_service = pending_verification_service

        # The utility functions are now imported from metadata_helpers.py
        # They do not need to be stored as instance attributes unless specifically wrapped/modified

    # Checks if the given string is a valid 4-digit year.
    def _is_valid_year(self, year_str: str | int | float | None) -> bool:
        """Checks if the given string is a valid 4-digit year."""
        if not year_str:
            return False
        if isinstance(
            year_str,
            int | float,
        ):  # If it's already a number, convert to string
            year_str = str(int(year_str))
        return len(year_str) == 4 and year_str.isdigit()

    # Logic moved from old fetch_tracks_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Fetch Tracks Async")
    async def fetch_tracks_async(
        self,
        artist: str | None = None,
        force_refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Fetch tracks from the Music app, optionally filtered by artist."""
        self.console_logger.debug(
            f"fetch_tracks_async: Start. self.cache_service is {type(self.cache_service)}: {self.cache_service}",
        )

        try:
            cache_key = artist or "ALL"
            self.console_logger.info(
                "Fetching tracks with cache_key='%s', force_refresh=%s",
                cache_key,
                force_refresh,
            )

            # Check if Music app is running
            error_logger_for_music_app_check = self.error_logger
            if not is_music_app_running(error_logger_for_music_app_check):
                self.console_logger.error(
                    "Music app is not running! Please start Music.app before running this script.",
                )
                return []

            # Try to get data from cache if appropriate
            cached_tracks = []
            should_fetch_from_app = True

            # Check cache service availability
            if self.cache_service is None:
                self.error_logger.critical(
                    "fetch_tracks_async: ERROR: self.cache_service is None!",
                )
            elif force_refresh:
                # Handle force refresh vs normal cache check
                self.console_logger.info(
                    "Force refresh requested, ignoring cache for %s",
                    cache_key,
                )
                self.console_logger.debug(
                    "fetch_tracks_async: Calling cache_service.invalidate...",
                )
                self.cache_service.invalidate(cache_key)
            else:
                # Try to use cache
                    self.console_logger.debug(
                        "fetch_tracks_async: Calling cache_service.get_async...",
                    )
                    cached_tracks = await self.cache_service.get_async(cache_key)

                    # Check if we got valid cached data
                    if cached_tracks and isinstance(cached_tracks, list):
                        self.console_logger.info(
                            "Using cached data for %s, found %d tracks",
                            cache_key,
                            len(cached_tracks),
                        )
                        should_fetch_from_app = False
                    elif cached_tracks:
                        self.console_logger.warning(
                            "Cached data for %s has incorrect type. Expected list, got %s. Ignoring cache.",
                            cache_key,
                            type(cached_tracks).__name__
                        )
                    else:
                        self.console_logger.info(
                            "No cache found for %s, fetching from Music.app",
                            cache_key,
                        )

            # Initialize raw_data with empty string as default
            raw_data = ""

            # Define script_name outside the conditional block so it's always available
            script_name = "fetch_tracks.applescript"

            # If we shouldn't or couldn't use cached data, fetch from Music.app
            if should_fetch_from_app:
                script_args = [artist] if artist else []

                # Use timeout from config
                timeout = self.config.get("applescript_timeout_seconds", 900)

                # Use injected ap_client
                raw_data = await self.ap_client.run_script(
                    script_name,
                    script_args,
                    timeout=timeout,
                ) or ""  # Provide default empty string if None is returned

            if raw_data:
                self.console_logger.debug(
                    f"fetch_tracks_async: Raw AppleScript output (first 500 chars): {raw_data[:500]}...",
                )
                lines_count = raw_data.count("\n") + 1
                self.console_logger.info(
                    "AppleScript returned data: %d bytes, approximately %d lines",
                    len(raw_data.encode("utf-8")),
                    lines_count,
                )  # Use byte length
            else:
                self.error_logger.error(
                    "Empty response from AppleScript %s. Possible script error.",
                    script_name,
                )
                return []

            # parse_tracks is a simple parsing function, can remain a utility
            # Pass the error logger from the instance
            tracks = parse_tracks(raw_data, self.error_logger)
            self.console_logger.info(
                "Successfully parsed %d tracks from Music.app",
                len(tracks),
            )

            if tracks:
                # Use injected cache_service
                await self.cache_service.set_async(cache_key, tracks)
                self.console_logger.info(
                    "Cached %d tracks with key '%s'",
                    len(tracks),
                    cache_key,
                )

            return tracks
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error("Error in fetch_tracks_async: %s", e, exc_info=True)
            return []

    # Logic moved from old update_track_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Track")
    async def update_track_async(
        self,
        track_id: str,
        new_track_name: str | None = None,
        new_album_name: str | None = None,
        new_genre: str | None = None,
        new_year: str | None = None,
    ) -> bool:
        """Updates the track properties asynchronously via AppleScript using injected AppleScriptClient."""
        try:
            if not track_id:
                self.error_logger.error("No track_id provided.")
                return False

            success = True

            # Use injected ap_client
            if new_track_name:
                res = await self.ap_client.run_script(
                    "update_property.applescript",
                    [track_id, "name", new_track_name],
                )
                if not res or "Success" not in res:
                    self.error_logger.error(
                        "Failed to update track name for %s",
                        track_id,
                    )
                    success = False

            # Use injected ap_client
            if new_album_name:
                res = await self.ap_client.run_script(
                    "update_property.applescript",
                    [track_id, "album", new_album_name],
                )
                if not res or "Success" not in res:
                    self.error_logger.error(
                        "Failed to update album name for %s",
                        track_id,
                    )
                    success = False

            # Use injected ap_client
            if new_genre:
                max_retries = self.config.get("max_retries", 3)
                delay = self.config.get("retry_delay_seconds", 2)
                genre_updated = False
                for attempt in range(1, max_retries + 1):
                    res = await self.ap_client.run_script(
                        "update_property.applescript",
                        [track_id, "genre", new_genre],
                    )
                    if res and "Success" in res:
                        self.console_logger.info(
                            "Updated genre for %s to %s (attempt %s/%s)",
                            track_id,
                            new_genre,
                            attempt,
                            max_retries,
                        )
                        genre_updated = True
                        break
                    self.console_logger.warning(
                        "Attempt %s/%s failed. Retrying in %ss...",
                        attempt,
                        max_retries,
                        delay,
                    )
                    await asyncio.sleep(delay)
                if not genre_updated:
                    self.error_logger.error(
                        "Failed to update genre for track %s",
                        track_id,
                    )
                    success = False

            # Use injected ap_client
            if new_year:
                res = await self.ap_client.run_script(
                    "update_property.applescript",
                    [track_id, "year", new_year],
                )
                if not res or "Success" not in res:
                    self.error_logger.error("Failed to update year for %s", track_id)
                    success = False
                else:
                    self.console_logger.info(
                        "Updated year for %s to %s",
                        track_id,
                        new_year,
                    )

            return success
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error(
                "Error in update_track_async for track %s: %s",
                track_id,
                e,
                exc_info=True,
            )
            return False

    # Logic moved from old update_album_tracks_bulk_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Album Tracks Bulk Async")
    async def update_album_tracks_bulk_async(
        self,
        track_ids: list[str],
        year: str,
    ) -> bool:
        """Update the year property for multiple tracks in bulk using a batched approach.

        Uses injected AppleScriptClient.
        """
        try:
            if not track_ids or not year:
                self.console_logger.warning(
                    "No track IDs or year provided for bulk update.",
                )
                return False

            # Validation of track IDs and year before processing
            filtered_track_ids = []
            for track_id in track_ids:
                try:
                    # Ensure track_id is a valid number and not accidentally the year value
                    if track_id and track_id.isdigit() and track_id != year:
                        filtered_track_ids.append(track_id)
                    else:
                        self.console_logger.warning(
                            "Skipping invalid track ID: '%s'",
                            track_id,
                        )
                except (ValueError, TypeError):
                    self.console_logger.warning(
                        "Skipping non-numeric track ID: '%s'",
                        track_id,
                    )

            if not filtered_track_ids:
                self.console_logger.warning("No valid track IDs left after filtering")
                return False

            self.console_logger.info(
                "Updating year to %s for %d tracks (filtered from %d initial tracks)",
                year,
                len(filtered_track_ids),
                len(track_ids),
            )

            successful_updates = 0
            failed_updates = 0
            # Use batch_size from config, default to 20
            batch_size = min(20, self.config.get("batch_size", 20))

            for i in range(0, len(filtered_track_ids), batch_size):
                batch = filtered_track_ids[i : i + batch_size]
                tasks = []

                for track_id in batch:
                    self.console_logger.info(
                        "Adding update task for track ID %s to year %s",
                        track_id,
                        year,
                    )
                    # Use injected ap_client
                    tasks.append(
                        self.ap_client.run_script(
                            "update_property.applescript",
                            [track_id, "year", year],
                        ),
                    )

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for idx, result in enumerate(results):
                    track_id = batch[idx] if idx < len(batch) else "unknown"
                    if isinstance(result, Exception):
                        self.error_logger.error(
                            "Exception updating year for track %s: %s",
                            track_id,
                            result,
                        )
                        failed_updates += 1
                    elif (
                        result is None or result == ""
                    ):  # Handles None or empty string explicitly
                        self.error_logger.error(
                            "Empty or None result updating year for track %s",
                            track_id,
                        )
                        failed_updates += 1
                    elif isinstance(result, str):  # Result is a non-empty string
                        if "Success" in result:
                            successful_updates += 1
                            self.console_logger.info(
                                "Successfully updated year for track %s to %s",
                                track_id,
                                year,
                            )
                        else:  # "Success" not in result (string, but not success message)
                            if "not found" in result:
                                self.console_logger.warning(
                                    "Track %s not found, removing from processing",
                                    track_id,
                                )
                            else:
                                self.error_logger.error(
                                    "Failed to update year for track %s: %s",
                                    track_id,
                                    result,
                                )
                            failed_updates += 1
                    else:  # Fallback for unexpected truthy, non-exception, non-string types
                        self.error_logger.error(
                            f"Unexpected result type for track {track_id}: {type(result).__name__} - {result!r}",
                        )
                        failed_updates += 1

                # Add a small delay between batches if there are more batches to process
                if i + batch_size < len(filtered_track_ids):
                    await asyncio.sleep(0.5)

            self.console_logger.info(
                "Year update results: %s successful, %s failed",
                successful_updates,
                failed_updates,
            )
            return successful_updates > 0
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error(
                "Error in update_album_tracks_bulk_async: %s",
                e,
                exc_info=True,
            )
            return False

    # Logic moved from old process_album_years
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Process Album Years")
    async def process_album_years(
        self,
        tracks: list[dict[str, str]],
        albums_to_process: dict[tuple[str, str], str],
        force: bool = False,
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Manage album year updates for all tracks using external music databases.

        Uses injected ExternalAPIService, CacheService, and PendingVerificationService.
        Always returns a tuple of (updated_tracks, changes) even in error cases.
        """
        # Initialize default return values
        updated_tracks: list[dict[str, str]] = []
        changes: list[dict[str, str]] = []

        # Use config from self.config
        if not self.config.get("year_retrieval", {}).get("enabled", False):
            self.console_logger.info(
                "Album year updates are disabled in config. Skipping.",
            )
            return updated_tracks, changes

        if not tracks:
            self.console_logger.info("No tracks provided for year updates.")
            return updated_tracks, changes

        self.console_logger.info("Starting album year updates (force=%s)", force)

        # --- Filter tracks to include only those from albums marked for processing ---
        # Instead of filtering by last_run_time, filter by albums_to_process dictionary
        tracks_for_processing = []
        processed_album_keys = set(albums_to_process.keys())
        if processed_album_keys:
            self.console_logger.info(
                "Filtering tracks to process only those from %d identified albums.",
                len(processed_album_keys),
            )
            for track in tracks:
                artist = track.get("artist", "").strip()
                album = track.get("album", "").strip()
                album_key = (artist, album)
                if album_key in processed_album_keys:
                    tracks_for_processing.append(track)
        else:
            # If albums_to_process is empty, it means no albums were identified
            # for full processing based on the criteria (and not forced).
            # In this case, there are no tracks to process for year updates.
            self.console_logger.info("No albums identified for processing.")
            return [], []

        self.console_logger.info(
            "Found %d tracks belonging to identified albums for year updates",
            len(tracks_for_processing),
        )

        # If no tracks remain after filtering, return early
        if not tracks_for_processing:
            self.console_logger.info(
                "No tracks to process for album year updates after filtering by identified albums.",
            )
            return updated_tracks, changes

        try:
            # Use injected external_api_service
            await self.external_api_service.initialize()
            # Moved core logic to a helper method
            updated_tracks, changes = await self._update_album_years_logic(
                tracks_for_processing,
                force=force,
            )

            # Use config from self.config
            csv_output_file_path = get_full_log_path(
                self.config,
                "csv_output_file",
                "csv/track_list.csv",
            )
            changes_report_file_path = get_full_log_path(
                self.config,
                "changes_report_file",
                "csv/changes_report.csv",
            )

            if updated_tracks:
                # Use injected cache_service, console_logger, error_logger
                await sync_track_list_with_current(
                    updated_tracks,
                    csv_output_file_path,
                    self.cache_service,
                    self.console_logger,
                    self.error_logger,
                    partial_sync=True,
                )
                # Use injected console_logger, error_logger
                save_unified_changes_report(
                    changes,
                    changes_report_file_path,
                    self.console_logger,
                    self.error_logger,
                    force_mode=force,
                )
                self.console_logger.info(
                    "Updated %d tracks with album years.",
                    len(updated_tracks),
                )
            else:
                self.console_logger.info("No tracks needed album year updates.")

            return updated_tracks, changes
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error(
                "Error in process_album_years: %s",
                e,
                exc_info=True,
            )
            return updated_tracks, changes
        finally:
            # Use injected external_api_service
            await self.external_api_service.close()

    # Helper method containing the core album year update logic
    # Moved from the old process_album_years function to make it cleaner
    async def _update_album_years_logic(
        self,
        tracks: list[dict[str, str]],
        force: bool = False,
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Core logic for updating album years by querying external APIs.

        Separated from process_album_years for clarity.
        Uses injected services: external_api_service, cache_service, pending_verification_service, ap_client.
        """
        # Setup logging for year updates (logic remains the same, but uses self.loggers)
        year_changes_log_file = get_full_log_path(
            self.config,
            "year_changes_log_file",
            "main/year_changes.log",
        )
        os.makedirs(os.path.dirname(year_changes_log_file), exist_ok=True)
        year_logger = logging.getLogger("year_updates")
        # Check if handler is already added to year_logger to prevent duplicates
        if not any(
            isinstance(handler, logging.FileHandler)
            and handler.baseFilename == year_changes_log_file
            for handler in year_logger.handlers
        ):
            fh = logging.FileHandler(year_changes_log_file)
            # Use the same formatter as other file logs for consistency if possible,
            # but the RunTrackingHandler/Queue setup complicates direct FileHandler formatting here.
            # Sticking to a basic formatter for now, might need refactoring later.
            fh.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                ),
            )  # Added %(name)s
            year_logger.addHandler(fh)
        # Ensure logger level is set from config if it's not already set by QueueListener setup
        year_updates_file_level = logging.getLevelName(
            self.config.get("logging", {})
            .get("levels", {})
            .get("year_updates_file", "INFO")
            .upper(),
        )
        year_logger.setLevel(year_updates_file_level)

        self.console_logger.info(
            "Starting album year update logic for %d tracks",
            len(tracks),
        )
        # Group tracks by album to reduce API calls (logic remains the same)
        albums: dict[str, dict[str, Any]] = {}
        for track in tracks:
                    artist = track.get("artist", "Unknown")
                    album = track.get("album", "Unknown")
                    key = f"{artist}|{album}"
                    if key not in albums:
                        albums[key] = {"artist": artist, "album": album, "tracks": []}
                    # No conditional check needed - tracks is already initialized as a list above
                    # Just directly append the track to the tracks list
                    albums[key]["tracks"].append(track)

        self.console_logger.info("Processing %d unique albums", len(albums))
        year_logger.info("Starting year update for %d albums", len(albums))

        updated_tracks: list[dict[str, str]] = []
        changes_log: list[dict[str, str]] = []

        # Define the album processing function (now an inner async function or separate method)
        # Let's keep it as an inner async function for direct access to updated_tracks/changes_log
        async def process_album(album_data):
            nonlocal updated_tracks, changes_log
            artist = album_data["artist"]
            album = album_data["album"]
            album_tracks = album_data["tracks"]

            # Extract the current library year from the first track
            current_library_year = (
                album_tracks[0].get("new_year", "").strip() if album_tracks else ""
            )  # Use new_year as current state

            # New logic to determine if API fetch is needed based on cache and pending status
            needs_api_fetch = False
            year = None  # Variable to hold the determined year (from cache or API)
            is_definitive = False  # Variable to hold definitive status

            # Option 1: Force update - always fetch from API, bypass cache and pending logic
            if force:
                self.console_logger.info(
                    "Force fetching year for '%s - %s' from external API.",
                    artist,
                    album,
                )
                year, is_definitive = await self.external_api_service.get_album_year(
                    artist,
                    album,
                    current_library_year,
                )
                needs_api_fetch = True  # API call was made

            else:
                # Option 2: Check cache for a DEFINITIVE year
                # get_album_year_from_cache currently only stores definitive years, so a hit implies definitive
                cached_year = await self.cache_service.get_album_year_from_cache(
                    artist,
                    album,
                )

                if cached_year:
                    self.console_logger.info(
                        "Using cached definitive year %s for '%s - %s'",
                        cached_year,
                        artist,
                        album,
                    )
                    year = cached_year
                    is_definitive = True  # Cached year is considered definitive by definition of this cache
                    needs_api_fetch = False  # No API call needed

                else:
                    # Cache miss (album not definitively processed before, or cache invalidated)
                    # Option 3: Check if this album is pending verification and needs re-check
                    should_verify = False
                    if self.pending_verification_service:
                        # is_verification_needed is async and checks interval
                        should_verify = await self.pending_verification_service.is_verification_needed(
                            artist,
                            album,
                        )
                        if should_verify:
                            self.console_logger.info(
                                "Album '%s - %s' is due for verification",
                                artist,
                                album,
                            )

                    if should_verify:
                        # Album needs verification -> fetch from API
                        self.console_logger.info(
                            "Fetching year for '%s - %s' from external API (verification needed).",
                            artist,
                            album,
                        )
                        (
                            year,
                            is_definitive,
                        ) = await self.external_api_service.get_album_year(
                            artist,
                            album,
                            current_library_year,
                        )
                        needs_api_fetch = True  # API call was made

                        # Logic inside get_album_year already handles re-marking as pending if result is not definitive

                    else:
                        # Not forced, no definitive cache, and not due for pending verification -> skip API fetch
                        self.console_logger.debug(
                            "Skipping API fetch for '%s - %s': Not forced, no definitive cache, and not due for verification.",
                            artist,
                            album,
                        )
                        needs_api_fetch = False  # No API call needed
                        # year and is_definitive remain None/False

            # --- Process Result (whether from cache or API) ---
            # Now, 'year' holds the best year found (or None), and 'is_definitive' its status (if API called or cache hit)

            # If a year was determined (either from cache hit or successful API fetch)
            if year and self._is_valid_year(
                year,
            ):  # Use self._is_valid_year for consistency
                # Check if this determined year is different from the current library year
                if year != current_library_year:
                    # Filter tracks to update based on status and current year
                    tracks_to_update = []
                    for track in album_tracks:
                        current_year_on_track = track.get("new_year", "").strip()
                        track_status = track.get("trackStatus", "").lower()

                        # Skip tracks with statuses that can't be modified
                        if track_status in ("prerelease", "no longer available"):
                            continue

                        # Update if forced OR if current year is empty OR if current year is different from determined year
                        # Note: force logic is handled at the album level now, but keeping this condition ensures we update
                        # all tracks in the album batch if the determined year is different, regardless of *their* current year
                        # (unless status forbids). The primary force check is before API fetch.
                        # Simplified logic: if we successfully determined a year (from API or definitive cache)
                        # and it's different from the current library year for the album, update applicable tracks.
                        # This handles cases where only some tracks in the album got the year update previously.
                        if not current_year_on_track or current_year_on_track != year:
                            tracks_to_update.append(track)

                    if not tracks_to_update:
                        year_logger.info(
                            "No tracks need update for '%s - %s' (determined year: %s, current album year: %s)",
                            artist,
                            album,
                            year,
                            current_library_year,
                        )
                        # If no tracks need update but API call was made, return True for api_call_made
                        return needs_api_fetch  # Indicate if API call was made

                    year_logger.info(
                        "Updating %d of %d tracks for '%s - %s' to year %s (current album year: %s)",
                        len(tracks_to_update),
                        len(album_tracks),
                        artist,
                        album,
                        year,
                        current_library_year,
                    )

                    track_ids_to_update = [t.get("id", "") for t in tracks_to_update]
                    track_ids_to_update = [tid for tid in track_ids_to_update if tid]

                    # Use injected ap_client
                    success = await self.update_album_tracks_bulk_async(
                        track_ids_to_update,
                        year,
                    )
                    if success:
                        for track in tracks_to_update:
                            track_id = track.get("id", "")
                            if track_id:
                                # Update the track dictionary in memory
                                track["old_year"] = track.get(
                                    "new_year",
                                    "",
                                )  # Store previous 'new_year' as 'old_year'
                                track["new_year"] = year
                                # No need to append to updated_tracks here, the caller will collect all tracks
                                # updated_tracks.append(track)

                                # Log the change
                                changes_log.append(
                                    {
                                        "change_type": "year",
                                        "artist": artist,
                                        "album": album,
                                        "track_name": track.get("name", "Unknown"),
                                        "old_year": track.get(
                                            "old_year",
                                            "",
                                        ),  # Log the old year
                                        "new_year": year,
                                        "timestamp": datetime.now().strftime(
                                            "%Y-%m-%d %H:%M:%S",
                                        ),
                                    },
                                )
                        year_logger.info(
                            "Successfully updated %d tracks for '%s - %s' to year %s",
                            len(track_ids_to_update),
                            artist,
                            album,
                            year,
                        )
                    else:
                        self.error_logger.error(
                            "Failed to update year for '%s - %s'",
                            artist,
                            album,
                        )
                else:
                    # Year matches current library year, no update needed for tracks
                    year_logger.info(
                        "Determined year %s for '%s - %s' matches current library year, no track updates needed.",
                        year,
                        artist,
                        album,
                    )

                # Ensure definitive results are in cache (redundant if get_album_year_from_cache only stores definitive,
                # but good for clarity/safety if that logic changes)
                # Also store non-definitive years IF API call was made, so we know we tried (but don't consider them definitive cache hits)
                # The cache_service.store_album_year_in_cache currently only stores definitive results.
                # We need a way to record that we *attempted* to verify a pending album, regardless of result.
                # This seems to be handled by removing from pending *if definitive* in ExternalApiService.get_album_year.
                # If it's not definitive, it remains in pending.

                # Let's re-evaluate the caching and pending marking logic here.
                # 1. If API was called and result is definitive: cache year, remove from pending. (Handled in get_album_year and logic above)
                # 2. If API was called and result is NOT definitive: DO NOT cache year as definitive, MARK for pending. (Handled in get_album_year)
                # 3. If year was found in DEFINITIVE cache: use it, no API, no pending. (Handled above)
                # 4. If year was NOT found in DEFINITIVE cache and NOT forced and NOT due for pending: skip API, skip pending. (Handled above)

                # The existing logic for caching (only definitive) and pending marking (for non-definitive API results) seems correct.
                # We just needed to ensure we don't make redundant API calls for pending items unless their interval is met.

                # If no year was successfully determined from API and no definitive cache hit, it remains None/False
                # In this case, if API fetch was attempted, get_album_year would have marked for pending if a current_library_year existed.

                # Return whether an API call was actually made for this album
                return needs_api_fetch  # Indicate if API call was made for this album

        # Fetch optimal batch parameters from config using self.config
        batch_size = (
            self.config.get("year_retrieval", {})
            .get("processing", {})
            .get("batch_size", 20)
        )  # Get from processing subsection
        delay_between_batches = (
            self.config.get("year_retrieval", {})
            .get("processing", {})
            .get("delay_between_batches", 30)
        )  # Get from processing subsection
        concurrent_limit = (
            self.config.get("year_retrieval", {})
            .get("rate_limits", {})
            .get("concurrent_api_calls", 5)
        )  # Get from rate_limits subsection
        adaptive_delay = (
            self.config.get("year_retrieval", {})
            .get("processing", {})
            .get("adaptive_delay", False)
        )  # Get from processing subsection

        album_items = list(albums.items())

        # Create semaphore for concurrency control (specific to this method's batches)
        semaphore = asyncio.Semaphore(concurrent_limit)

        # Process albums in batches
        for i in range(0, len(album_items), batch_size):
            batch = album_items[i : i + batch_size]
            batch_tasks = []

            # Create tasks for each album in the batch
            for _, album_data in batch:
                # Use semaphore to limit concurrency
                async def process_with_semaphore(data: dict[str, Any]) -> bool:
                    async with semaphore:
                        result = await process_album(
                            data,
                        )  # Call the inner processing function
                        return bool(result)  # Ensure we always return a boolean

                batch_tasks.append(process_with_semaphore(album_data))

            # Execute batch with controlled concurrency
            api_calls_made_in_batch = await asyncio.gather(*batch_tasks)

            # Log progress
            batch_num = i // batch_size + 1
            total_batches = (len(album_items) + batch_size - 1) // batch_size
            self.console_logger.info("Processed batch %d/%d", batch_num, total_batches)

            # Calculate adaptive delay for next batch if needed
            if i + batch_size < len(album_items):
                api_calls_count = sum(
                    1 for call_made in api_calls_made_in_batch if call_made is True
                )  # Count how many albums in the batch made an API call

                if adaptive_delay and api_calls_count > 0:
                    # Scale delay based on API usage ratio
                    usage_ratio = api_calls_count / len(batch)
                    adjusted_delay = max(1, round(delay_between_batches * usage_ratio))

                    self.console_logger.info(
                        "Adaptive delay: %ds based on %d/%d API calls in batch",
                        adjusted_delay,
                        api_calls_count,
                        len(batch),
                    )
                    await asyncio.sleep(adjusted_delay)
                elif (
                    api_calls_count > 0
                ):  # If adaptive delay is off but API calls were made
                    self.console_logger.info(
                        "API calls were made. Waiting %ds before next batch",
                        delay_between_batches,
                    )
                    await asyncio.sleep(delay_between_batches)
                else:  # No API calls made in this batch
                    self.console_logger.info(
                        "No API calls made in this batch. Proceeding to next batch immediately.",
                    )

        self.console_logger.info(
            "Album year update logic complete. Processed %d albums.",
            len(albums),
        )  # Log albums processed, not just tracks
        year_logger.info(
            "Album year update logic complete. Processed %d albums.",
            len(albums),
        )

        # Return the list of updated tracks and the changes log
        # Note: updated_tracks list is populated inside the inner process_album function
        return updated_tracks, changes_log

    # Logic moved from old can_run_incremental
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Can Run Incremental")
    async def can_run_incremental(self, force_run: bool = False) -> bool:
        """Checks if the incremental interval has passed since the last run using injected CacheService."""
        if force_run:
            self.console_logger.info(
                "Force run requested. Bypassing incremental interval check.",
            )
            return True

        # Use config from self.config
        last_file_path = get_full_log_path(
            self.config,
            "last_incremental_run_file",
            "last_incremental_run.log",
            self.error_logger,
        )  # Pass error_logger
        interval = self.config.get("incremental_interval_minutes", 60)

        # If the file does not exist, allow it to run
        if not os.path.exists(last_file_path):
            self.console_logger.info(
                "Last incremental run file not found at %s. Proceeding.",
                last_file_path,
            )
            return True

        # Trying to read a file with repeated attempts
        max_retries = 3
        retry_delay = 0.5  # seconds

        for attempt in range(max_retries):
            try:
                # Trying to read a file with rollback on failure
                with open(last_file_path, encoding="utf-8") as f:
                    last_run_str = f.read().strip()

                try:
                    last_run_time = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    self.error_logger.error(
                        f"Invalid date format '{last_run_str}' in {last_file_path}. Proceeding with execution assuming first run.",
                    )  # Log invalid format
                    return True

                next_run_time = last_run_time + timedelta(minutes=interval)
                now = datetime.now()

                if now >= next_run_time:
                    self.console_logger.info(
                        "Incremental interval (%d mins) elapsed since last run (%s). Proceeding.",
                        interval,
                        last_run_time.strftime("%Y-%m-%d %H:%M:%S"),
                    )  # Log full timestamp
                    return True
                diff = next_run_time - now
                minutes_remaining = (
                    diff.total_seconds() // 60
                )  # Use total_seconds for more accurate difference
                self.console_logger.info(
                    "Last run: %s. Next run in %d mins.",
                    last_run_time.strftime("%Y-%m-%d %H:%M:%S"),
                    minutes_remaining,
                )  # Log full timestamp
                return False

            except OSError as e:
                if attempt < max_retries - 1:
                    # If this is not the last attempt, we wait and try again
                    self.console_logger.warning(
                        f"Error reading last run file (attempt {attempt + 1}/{max_retries}): "
                        f"{e}. Retrying in {retry_delay:.1f}s...",  # Use :.1f for float
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential increase in latency
                else:
                    # If all attempts fail, log and allow launch
                    self.error_logger.error(
                        f"Error accessing last run file at {last_file_path} after {max_retries} attempts: {e}. Allowing execution.",
                    )  # Log file path
                    return True

        # Should not be reached, but for safety:
        self.error_logger.error(
            "Unexpected logic path reached in can_run_incremental. Allowing execution.",
        )  # Log unexpected path
        return True

    # Logic moved from old update_last_incremental_run
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Last Incremental Run")
    async def update_last_incremental_run(self) -> None:
        """Update the timestamp of the last incremental run in a file.

        Uses config from self.config.
        """
        # Use config from self.config
        last_file_path = get_full_log_path(
            self.config,
            "last_incremental_run_file",
            "last_incremental_run.log",
            self.error_logger,
        )  # Pass error_logger
        try:
            # Ensure directory exists before writing
            os.makedirs(os.path.dirname(last_file_path), exist_ok=True)
            with open(last_file_path, "w", encoding="utf-8") as f:
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(now_str)
            self.console_logger.info(
                "Updated last incremental run timestamp to %s.",
                now_str,
            )
        except OSError as e:
            self.error_logger.error(
                f"Failed to write last incremental run timestamp to {last_file_path}: {e}",
            )

    # Logic moved from old verify_and_clean_track_database
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Database Verification")
    async def verify_and_clean_track_database(self, force: bool = False) -> int:
        """Verifies tracks in the CSV database against Music.app.

        Also removes entries for tracks that no longer exist.
        Respects the 'development.test_artists' config setting.
        Uses injected AppleScriptClient and config from self.config.
        """
        removed_count = 0
        try:
            self.console_logger.info(
                "Starting database verification process (force=%s)...",
                force,
            )  # Log force status
            # Use config from self.config
            csv_path = get_full_log_path(
                self.config,
                "csv_output_file",
                "csv/track_list.csv",
                self.error_logger,
            )  # Pass error_logger

            if not os.path.exists(csv_path):
                self.console_logger.info(
                    "Track database CSV file not found at %s, nothing to verify.",
                    csv_path,
                )
                return 0

            # Load the entire track list from CSV into a dictionary {track_id: track_dict}
            # load_track_list is a utility function, pass error logger if it logs internally
            csv_tracks_map = load_track_list(
                csv_path,
            )  # Assumes load_track_list uses module-level or passed loggers

            if not csv_tracks_map:
                self.console_logger.info(
                    "No tracks loaded from database CSV to verify.",
                )
                return 0

            # --- Check if verification is needed based on interval (unless forced) ---
            # Use config from self.config
            last_verify_file = get_full_log_path(
                self.config,
                "last_db_verify_log",
                "main/last_db_verify.log",
                self.error_logger,
            )  # Pass error_logger
            auto_verify_days = self.config.get("database_verification", {}).get(
                "auto_verify_days",
                7,
            )

            if not force and auto_verify_days > 0 and os.path.exists(last_verify_file):
                try:
                    with open(last_verify_file, encoding="utf-8") as f:
                        last_verify_str = f.read().strip()
                    last_date = datetime.strptime(last_verify_str, "%Y-%m-%d").date()
                    days_since_last = (datetime.now().date() - last_date).days
                    if days_since_last < auto_verify_days:
                        self.console_logger.info(
                            f"Database verified on {last_date} ({days_since_last} days ago). "
                            f"Skipping verification (interval: {auto_verify_days} days). Use --force to override.",
                        )
                        return 0  # Verification not needed yet
                except (OSError, FileNotFoundError, PermissionError, ValueError) as e:
                    self.error_logger.warning(
                        f"Could not read or parse last verification date ({last_verify_file}): {e}. Proceeding with verification.",
                    )
                    # Proceed if file is missing or corrupt

            # --- Apply test_artists filter if configured ---
            tracks_to_verify_map = csv_tracks_map  # Start with all tracks by default
            # Use config from self.config
            test_artists_config = self.config.get("development", {}).get(
                "test_artists",
                [],
            )

            if test_artists_config:
                self.console_logger.info(
                    "Limiting database verification to specified test_artists: %s",
                    test_artists_config,
                )
                # Filter the map to include only tracks from test artists
                tracks_to_verify_map = {
                    track_id: track
                    for track_id, track in csv_tracks_map.items()
                    if track.get("artist") in test_artists_config
                }
                self.console_logger.info(
                    "After filtering by test_artists, %d tracks will be verified.",
                    len(tracks_to_verify_map),
                )

            # --- Check if any tracks remain after filtering ---
            if not tracks_to_verify_map:
                self.console_logger.info(
                    "No tracks to verify (either DB is empty or after filtering by test_artists).",
                )
                # Update the verification timestamp even if nothing was checked
                try:
                    os.makedirs(
                        os.path.dirname(last_verify_file),
                        exist_ok=True,
                    )  # Ensure directory exists
                    with open(last_verify_file, "w", encoding="utf-8") as f:
                        f.write(datetime.now().strftime("%Y-%m-%d"))
                except OSError as e:
                    self.error_logger.error(
                        f"Failed to write last verification timestamp to {last_verify_file}: {e}",
                    )
                return 0  # Nothing removed

            # --- Log correct count and get IDs ---
            tracks_to_verify_count = len(tracks_to_verify_map)
            self.console_logger.info(
                "Verifying %d tracks against Music.app...",
                tracks_to_verify_count,
            )
            track_ids_to_check = list(
                tracks_to_verify_map.keys(),
            )  # Get IDs of tracks to check

            # --- Async verification helper ---
            # This helper needs access to the AppleScript client
            async def verify_track_exists(track_id: str) -> bool:
                """Checks if a track ID exists in the Music library using injected AppleScriptClient."""
                if not track_id or not track_id.isdigit():
                    self.error_logger.warning(
                        f"Invalid track ID '{track_id}' passed to verify_track_exists.",
                    )  # Log invalid ID
                    return False  # Basic validation
                script = f"""
                tell application "Music"
                    try
                        # Efficiently check existence by trying to get a property
                        # Using 'properties' to get a dictionary is slightly more robust than just 'id'
                        get properties of track id {track_id} of library playlist 1
                        return "exists"
                    on error errMsg number errNum
                        if errNum is -1728 then
                            # Error code for "item not found"
                            return "not_found"
                        else
                            # Log other errors but treat as potentially existing
                            log "Error verifying track {track_id}: " & errNum & " " & errMsg
                            return "error_assume_exists"
                        end if
                    end try
                end tell
                """
                try:
                    # Use injected ap_client
                    result = await self.ap_client.run_script_code(script)
                    # Return True only if AppleScript explicitly confirmed existence
                    if result == "exists":
                        return True
                    if result == "not_found":
                        self.console_logger.debug(
                            f"Track ID {track_id} not found in Music.app.",
                        )  # Log not found
                        return False
                    # Includes "error_assume_exists" case from AppleScript
                    self.error_logger.warning(
                        f"AppleScript verification for track ID {track_id} returned unexpected result: '{result}'. Assuming exists.",
                    )  # Log unexpected result
                    return True  # Assume exists on unknown result or error
                except (
                    Exception
                ) as e:  # Catch potential exceptions during script execution
                    self.error_logger.error(
                        f"Exception during AppleScript execution for track {track_id}: {e}",
                    )
                    return True  # Assume exists on error to prevent accidental deletion

            # --- Batch Processing ---
            # Use config from self.config
            batch_size = self.config.get("database_verification", {}).get(
                "batch_size",
                10,
            )
            invalid_track_ids: list[str] = []
            self.console_logger.info("Checking tracks in batches of %d...", batch_size)

            for i in range(0, len(track_ids_to_check), batch_size):
                batch_ids = track_ids_to_check[i : i + batch_size]
                # Create tasks for the current batch
                tasks = [verify_track_exists(track_id) for track_id in batch_ids]
                # Run tasks concurrently and get results (True if exists, False otherwise/error)
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results for the batch
                for idx, result in enumerate(results):
                    track_id = batch_ids[idx]
                    # If an exception occurred or verify_track_exists returned False (doesn't exist)
                    if (
                        isinstance(result, Exception) or result is False
                    ):  # Result is False means verify_track_exists returned False
                        if isinstance(result, Exception):
                            self.error_logger.warning(
                                f"Exception during verification task for track ID {track_id}: {result}. Treating as non-existent for cleanup.",
                            )  # Log exception and intent
                        invalid_track_ids.append(track_id)

                self.console_logger.info(
                    "Verified batch %d/%d",
                    (i // batch_size) + 1,
                    (tracks_to_verify_count + batch_size - 1) // batch_size,
                )
                # Optional short sleep between batches
                if i + batch_size < len(track_ids_to_check):
                    await asyncio.sleep(0.1)

            # --- Remove Invalid Tracks and Save ---
            if invalid_track_ids:
                self.console_logger.info(
                    "Found %d tracks that no longer exist in Music.app (within the verified subset).",
                    len(invalid_track_ids),
                )
                # Remove invalid tracks from the *original full map* loaded from CSV
                for track_id in invalid_track_ids:
                    if track_id in csv_tracks_map:
                        # Remove the track and log its details
                        removed_track_info = csv_tracks_map.pop(track_id)
                        artist_name = removed_track_info.get("artist", "Unknown Artist")
                        track_name = removed_track_info.get("name", "Unknown Track")
                        self.console_logger.info(
                            f"Removing track ID {track_id}: '{artist_name} - {track_name}'",
                        )
                        removed_count += 1
                    else:
                        # This case indicates a potential logic error if an ID is invalid but not in the map
                        self.console_logger.warning(
                            f"Attempted to remove track ID {track_id} which was not found in the loaded map. Skipping.",
                        )

                # Save the modified full track map back to the CSV
                final_list_to_save = list(csv_tracks_map.values())
                self.console_logger.info(
                    "Final CSV track count after verification: %d",
                    len(final_list_to_save),
                )
                # Use the dedicated save function (utility function)
                save_to_csv(
                    final_list_to_save,
                    csv_path,
                    self.console_logger,
                    self.error_logger,
                )
                self.console_logger.info(
                    "Removed %d invalid tracks from database.",
                    removed_count,
                )
            else:
                self.console_logger.info(
                    "All verified tracks (%d) exist in Music.app.",
                    tracks_to_verify_count - len(invalid_track_ids),
                )  # Log actual verified count
                removed_count = 0

            # --- Update Last Verification Timestamp ---
            try:
                os.makedirs(
                    os.path.dirname(last_verify_file),
                    exist_ok=True,
                )  # Ensure directory exists
                with open(last_verify_file, "w", encoding="utf-8") as f:
                    f.write(datetime.now().strftime("%Y-%m-%d"))
                self.console_logger.info(
                    "Updated last database verification timestamp.",
                )
            except OSError as e:
                self.error_logger.error(
                    f"Failed to write last verification timestamp to {last_verify_file}: {e}",
                )

            return removed_count

        except Exception as e:  # Catch broad exceptions during the verification process
            self.error_logger.error(
                "Critical error during database verification: %s",
                e,
                exc_info=True,
            )
            return 0  # Return 0 removed on error

    # Logic moved from old update_genres_by_artist_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Genres by Artist")
    async def update_genres_by_artist_async(
        self,
        tracks: list[dict[str, str]],
        last_run_time: datetime,
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Updates the genres for tracks based on the earliest genre of each artist.

        Uses injected AppleScriptClient and config from self.config.
        """
        try:
            # Use config from self.config
            csv_path = get_full_log_path(
                self.config,
                "csv_output_file",
                "csv/track_list.csv",
                self.error_logger,
            )  # Pass error_logger
            # load_track_list is a utility function, doesn't need self.
            load_track_list(
                csv_path,
            )  # This line seems redundant here, load_track_list result is not used. Can be removed if not needed.

            if last_run_time and last_run_time != datetime.min:
                self.console_logger.info(
                    "Filtering tracks added after %s for genre update",
                    last_run_time.strftime("%Y-%m-%d %H:%M:%S"),
                )  # Log full timestamp
                filtered_tracks = []
                for track in tracks:
                    date_added_str = track.get(
                        "dateAdded",
                        "1900-01-01 00:00:00",
                    )  # Use default date string
                    try:
                        date_added = datetime.strptime(
                            date_added_str,
                            "%Y-%m-%d %H:%M:%S",
                        )
                        if date_added > last_run_time:
                            filtered_tracks.append(track)
                    except ValueError:
                        self.error_logger.warning(
                            f"Invalid date format '{date_added_str}' for track ID {track.get('id', 'N/A')} "
                            f"during genre update filtering. Skipping track for incremental check.",
                        )  # Log invalid format
                        # Treat as not added since last run for safety in incremental mode.
                        # Track is not added to filtered_tracks

                self.console_logger.info(
                    "Found %d tracks added since last run for genre update",
                    len(filtered_tracks),
                )
                tracks_to_process = filtered_tracks  # Process only filtered tracks
            else:
                self.console_logger.info(
                    "Processing all %d tracks for genre update (force run or first run)",
                    len(tracks),
                )
                tracks_to_process = tracks  # Process all tracks

            # group_tracks_by_artist is a utility function, doesn't need self.
            grouped = group_tracks_by_artist(
                tracks_to_process,
            )  # Group the tracks selected for processing
            updated_tracks = (
                []
            )  # This list will contain tracks that were successfully updated
            changes_log = []

            # Inner async function to process a single track for genre update
            # It needs access to update_track_async method (which uses ap_client)
            async def process_track(track: dict[str, str], dom_genre: str) -> None:
                nonlocal updated_tracks, changes_log  # Access outer scope lists
                old_genre = track.get("genre", "Unknown")
                track_id = track.get("id", "")
                status = track.get(
                    "trackStatus",
                    "unknown",
                ).lower()  # Ensure status is lowercase for comparison

                # Check if update is needed and status allows modification
                # Only update if current genre is different from dominant genre AND status allows modification
                if (
                    track_id
                    and old_genre != dom_genre
                    and status in ("subscription", "downloaded")
                ):
                    self.console_logger.info(
                        "Updating track %s ('%s' - '%s') (Old Genre: %s, New Genre: %s)",
                        track_id,
                        track.get("artist", "Unknown"),
                        track.get("name", "Unknown"),
                        old_genre,
                        dom_genre,
                    )  # Log track details
                    # Use the method of this class
                    if await self.update_track_async(track_id, new_genre=dom_genre):
                        # If update successful, modify track dict and log changes
                        track["genre"] = dom_genre
                        # Append the modified track dictionary to the list of updated tracks
                        updated_tracks.append(track)
                        # Log the change
                        changes_log.append(
                            {
                                "change_type": "genre",
                                "artist": track.get("artist", "Unknown"),
                                "album": track.get("album", "Unknown"),
                                "track_name": track.get("name", "Unknown"),
                                "old_genre": old_genre,
                                "new_genre": dom_genre,
                                "timestamp": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S",
                                ),
                            },
                        )
                    else:
                        self.error_logger.error(
                            "Failed to update genre for track %s ('%s' - '%s')",
                            track_id,
                            track.get("artist", "Unknown"),
                            track.get("name", "Unknown"),
                        )  # Log track details
                # else: # Optional: Log why a track was skipped
                elif (
                    track_id
                ):  # If track_id exists but update wasn't needed or possible
                    if old_genre == dom_genre:
                        self.console_logger.debug(
                            f"Skipping track {track_id} "
                            f"('{track.get('artist', 'Unknown')}' - '{track.get('name', 'Unknown')}'): "
                            f"Genre already matches dominant ({dom_genre})",
                        )
                    elif status not in ("subscription", "downloaded"):
                        self.console_logger.debug(
                            f"Skipping track {track_id} "
                            f"('{track.get('artist', 'Unknown')}' - '{track.get('name', 'Unknown')}'): "
                            f"Status '{status}' does not allow modification",
                        )
                    else:
                        # Should not happen if logic is correct, but good for debugging
                        self.console_logger.debug(
                            f"Skipping track {track_id} "
                            f"('{track.get('artist', 'Unknown')}' - '{track.get('name', 'Unknown')}'): "
                            f"No update needed or condition not met.",
                        )

            async def process_tasks_in_batches(
                tasks: list[asyncio.Task[None]],
                batch_size: int,
            ) -> None:  # Pass batch_size
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i : i + batch_size]
                    self.console_logger.debug(
                        "Processing genre update batch %d/%d...",
                        (i // batch_size) + 1,
                        (len(tasks) + batch_size - 1) // batch_size,
                    )  # Log batch progress
                    await asyncio.gather(*batch, return_exceptions=True)

            tasks = []
            for artist, artist_tracks in grouped.items():
                if not artist_tracks:
                    continue
                try:
                    # determine_dominant_genre_for_artist is a utility function, doesn't need self.
                    # Pass the error logger from the instance
                    dom_genre = determine_dominant_genre_for_artist(
                        artist_tracks,
                        self.error_logger,
                    )
                except (
                    Exception
                ) as e:  # Catch potential exceptions from utility function
                    self.error_logger.error(
                        "Error determining dominant genre for artist '%s': %s",
                        artist,
                        e,
                        exc_info=True,
                    )
                    dom_genre = "Unknown"  # Default to Unknown on error

                self.console_logger.info(
                    "Artist: %s, Dominant Genre: %s (from %d tracks)",
                    artist,
                    dom_genre,
                    len(artist_tracks),
                )

                if (
                    dom_genre != "Unknown"
                ):  # Only process if a dominant genre was determined
                    for track in artist_tracks:
                        # Create a task for each track that might need a genre update
                        if track.get("genre", "Unknown") != dom_genre:
                            tasks.append(
                                asyncio.create_task(process_track(track, dom_genre)),
                            )

            if tasks:
                self.console_logger.info("Created %d genre update tasks.", len(tasks))
                # Use batch_size from config, default to 1000 for genre updates
                batch_size_genre = self.config.get("genre_update", {}).get(
                    "batch_size",
                    1000,
                )  # Assume genre_update batch_size config exists
                await process_tasks_in_batches(
                    tasks,
                    batch_size=batch_size_genre,
                )  # Pass batch_size

            else:
                self.console_logger.info("No genre update tasks needed.")

            # Return the list of tracks that were actually updated and the changes log
            # Note: updated_tracks list is populated inside the inner process_track function
            self.console_logger.info(
                "Genre update process complete. Updated %d tracks.",
                len(updated_tracks),
            )
            return updated_tracks, changes_log

        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error(
                "Error in update_genres_by_artist_async: %s",
                e,
                exc_info=True,
            )
            return [], []

    # Logic moved from old run_clean_artist
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Clean Artist")
    async def run_clean_artist(self, artist: str, force: bool) -> None:
        """Executes the cleaning process for a specific artist.

        Uses injected services and utility functions.
        """
        self.console_logger.info("Running 'clean_artist' mode for artist='%s'", artist)

        # Use injected fetch_tracks_async method
        # Pass force flag from arguments
        tracks = await self.fetch_tracks_async(artist=artist, force_refresh=force)

        if not tracks:
            self.console_logger.warning("No tracks found for artist: %s", artist)
            return

        # Make copies to avoid modifying the original list during iteration if it was cached
        all_tracks = [track.copy() for track in tracks]
        updated_tracks_cleaning = []  # Tracks modified by cleaning
        changes_log_cleaning = []  # Log of cleaning changes

        # Define async helper for cleaning a single track
        # This helper needs access to the update_track_async method and clean_names utility
        async def clean_track(track: dict[str, str]) -> None:
            nonlocal updated_tracks_cleaning, changes_log_cleaning  # Access outer scope lists
            orig_name = track.get("name", "")
            orig_album = track.get("album", "")
            track_id = track.get("id", "")
            artist_name = track.get(
                "artist",
                artist,
            )  # Use artist from track data or command arg

            if not track_id:
                return  # Skip if no ID

            # Use the utility function clean_names, pass config and loggers from self.config/self
            cleaned_nm, cleaned_al = clean_names(
                artist_name,
                orig_name,
                orig_album,
                self.config,
                self.console_logger,
                self.error_logger,
            )

            # Determine if changes were actually made
            new_tn = cleaned_nm if cleaned_nm != orig_name else None
            new_an = cleaned_al if cleaned_al != orig_album else None

            # If changes exist, attempt to update via AppleScript
            if new_tn or new_an:
                track_status = track.get("trackStatus", "").lower()
                # Check if track status allows modification
                if track_status in ("subscription", "downloaded"):
                    # Use the method of this class to update the track
                    if await self.update_track_async(
                        track_id,
                        new_track_name=new_tn,
                        new_album_name=new_an,
                    ):
                        # If update successful, modify track dict and log changes
                        if new_tn:
                            track["name"] = cleaned_nm
                        if new_an:
                            track["album"] = cleaned_al
                        # Append the modified track dictionary to the list
                        updated_tracks_cleaning.append(track)
                        # Log the change details
                        changes_log_cleaning.append(
                            {
                                "change_type": "name",
                                "artist": artist_name,
                                "album": track.get("album", "Unknown"),
                                "track_name": orig_name,  # Original name before cleaning
                                "old_track_name": orig_name,
                                "new_track_name": cleaned_nm,
                                "old_album_name": orig_album,
                                "new_album_name": cleaned_al,
                                "timestamp": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S",
                                ),
                            },
                        )
                    else:
                        self.error_logger.error(
                            "Failed to apply cleaning update for track ID %s",
                            track_id,
                        )
                else:
                    self.console_logger.debug(
                        f"Skipping track update for '{orig_name}' (ID: {track_id}) due to status '{track_status}'",
                    )

        # Run cleaning tasks concurrently
        clean_tasks_default = [asyncio.create_task(clean_track(t)) for t in all_tracks]
        await asyncio.gather(*clean_tasks_default)

        # --- Post-Processing for clean_artist ---
        # Note: Genre and Year updates are NOT run in clean_artist mode by default.
        # If desired, update_genres_by_artist_async and process_album_years
        # could be called here, operating on all_tracks.

        # Save results if any tracks were updated
        if updated_tracks_cleaning:
            self.console_logger.info(
                "Cleaned %d track/album names.",
                len(updated_tracks_cleaning),
            )
            # Sync changes with the main CSV database (utility function)
            # Use config from self.config, injected cache_service, loggers
            await sync_track_list_with_current(
                all_tracks,  # Pass the fully updated list of tracks, not just the cleaned ones, to preserve other metadata
                get_full_log_path(
                    self.config,
                    "csv_output_file",
                    "csv/track_list.csv",
                    self.error_logger,
                ),  # Pass error_logger
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=True,  # Partial sync is appropriate here
            )
            # Save a report of the specific changes made (utility function)
            # Use config from self.config, injected loggers
            save_unified_changes_report(
                changes_log_cleaning,
                get_full_log_path(
                    self.config,
                    "changes_report_file",
                    "csv/changes_report.csv",
                    self.error_logger,
                ),  # Pass error_logger
                self.console_logger,
                self.error_logger,
                force_mode=force,  # Use force flag from arguments for console output
            )
            self.console_logger.info(
                "Processed and logged %d cleaning changes for artist: %s",
                len(changes_log_cleaning),
                artist,
            )  # Log changes count
        else:
            self.console_logger.info(
                "No cleaning updates needed for artist: %s",
                artist,
            )

    # Logic moved from old update_years
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Update Years")
    async def run_update_years(self, artist: str | None, force: bool) -> None:
        """Executes the album year update process.

        Uses injected services and config from self.config.
        """
        force_year_update = force  # Use force flag specific to this command
        artist_msg = f" for artist={artist}" if artist else " for all artists"
        self.console_logger.info(
            f"Running in 'update_years' mode{artist_msg} (force={force_year_update})",
        )

        # Fetch tracks (filtered by artist if provided, force refresh if requested)
        # Use injected fetch_tracks_async method
        tracks = await self.fetch_tracks_async(
            artist=artist,
            force_refresh=force_year_update,
        )
        if not tracks:
            self.console_logger.warning(f"No tracks found{artist_msg}.")
            # Update last run timestamp even if no tracks processed, to respect interval
            if (
                not force_year_update and artist is None
            ):  # Only update if it was a full incremental scan attempt
                await self.update_last_incremental_run()  # Use method of this class
            return

        # Make copies AFTER filtering to avoid modifying cached data if applicable
        all_tracks = [track.copy() for track in tracks]

        # Load previous track list data for comparison
        tracklist_csv_path = get_full_log_path(
            self.config,
            "csv_output_file",
            "csv/track_list.csv",
            self.error_logger,
        )
        previous_track_data = load_track_list(
            tracklist_csv_path,
        )  # This returns a dictionary {track_id: track_data}

        # Determine whether to use incremental or full processing
        effective_last_run = datetime.min  # Default to beginning of time for full run
        if not force_year_update:  # Only check last run time if not forcing
            try:
                # Use injected cache_service
                last_run_time = await self.cache_service.get_last_run_timestamp()
                self.console_logger.info(
                    "Last incremental run timestamp: %s",
                    (
                        last_run_time.strftime("%Y-%m-%d %H:%M:%S")
                        if last_run_time != datetime.min
                        else "Never or Failed"
                    ),
                )  # Log full timestamp
                effective_last_run = (
                    last_run_time  # Use actual last run time for incremental
                )
            except Exception as e:
                self.error_logger.warning(
                    f"Could not get last run timestamp from cache service: {e}. Assuming full run.",
                )
                effective_last_run = (
                    datetime.min
                )  # Fallback to full run if timestamp retrieval fails

        # --- Determine which albums require full processing ---
        # Collect albums that need full processing due to various criteria
        albums_for_full_processing: dict[
            tuple[str, str],
            str,
        ] = {}  # Key: (artist, album), Value: Reason

        self.console_logger.info("Identifying albums for full processing...")

        if not all_tracks:
            self.console_logger.warning(
                "No tracks to process for full processing check.",
            )
            return

        try:
            for track in all_tracks:
                track_id = track.get("id")
                artist = track.get("artist", "").strip()
                album = track.get("album", "").strip()
                current_status = track.get("trackStatus", "").strip().lower()
                date_added_str = track.get("dateAdded", "1900-01-01 00:00:00")

                if not artist or not album or not track_id:
                    self.console_logger.debug(
                        f"Skipping track with missing metadata for full processing check: ID={track_id}, Artist='{artist}', Album='{album}'",
                    )
                    continue  # Skip tracks that lack essential metadata

                # Create album_key only after validation when we know artist and album are non-empty strings
                album_key = (artist, album)

                # 1. Check if added since last run (basic incremental filter)
                try:
                    date_added = datetime.strptime(date_added_str, "%Y-%m-%d %H:%M:%S")
                    if date_added > effective_last_run:
                        if album_key not in albums_for_full_processing:
                            albums_for_full_processing[album_key] = (
                                "Added since last run"
                            )
                            self.console_logger.debug(
                                f"Album '{artist} - {album}' marked for full processing: Added since last run.",
                            )
                            # If album is already marked, no need to check other criteria for this track in this album
                            continue
                except ValueError:
                    self.error_logger.warning(
                        f"Invalid date format '{date_added_str}' for track ID {track_id}. Cannot check date added criterion.",
                    )
                    # Cannot determine if added since last run, proceed with other checks

                # 2. Check for status change (e.g., prerelease -> subscription)
                previous_track = previous_track_data.get(track_id)
                if previous_track:
                    previous_status = (
                        previous_track.get("trackStatus", "").strip().lower()
                    )
                    # Condition: current is 'subscription' AND previous was not 'subscription'
                    if (
                        current_status == "subscription"
                        and previous_status != "subscription"
                    ):
                        if album_key not in albums_for_full_processing:
                            albums_for_full_processing[album_key] = (
                                "Status changed to subscription"
                            )
                            self.console_logger.debug(
                                f"Album '{artist} - {album}' marked for full processing:"
                                f"Track status changed from '{previous_status}' to '{current_status}'",
                            )
                            # If album is already marked, no need to check other criteria for this track in this album
                            continue

                # 3. Check if album is pending verification
                # Use injected pending_verification_service if available
                if self.pending_verification_service:
                    # Check if the *album* (artist, album) is in the pending list
                    is_pending = (
                        await self.pending_verification_service.is_verification_needed(
                            artist,
                            album,
                        )
                    )  # Check if album needs verification
                    if is_pending:
                        if album_key not in albums_for_full_processing:
                            albums_for_full_processing[album_key] = (
                                "Pending verification"
                            )
                            self.console_logger.debug(
                                f"Album '{artist} - {album}' marked for full processing: Pending verification.",
                            )
                            # If album is already marked, no need to check other criteria for this track in this album
                            continue

        except Exception as e:
            self.error_logger.error(
                f"Error processing tracks for full processing check: {e}",
            )
            albums_for_full_processing = (
                {}
            )  # Ensure we pass an empty dict if there's an error

        # Log the identified albums for debugging
        self.console_logger.info(
            f"Identified {len(albums_for_full_processing)} albums for full processing",
        )
        if albums_for_full_processing:
            self.console_logger.debug("Albums for full processing:")
            for (artist, album), reason in albums_for_full_processing.items():
                self.console_logger.debug(f"- {artist} - {album}: {reason}")

        # If force is true, all albums need full processing regardless of criteria
        if force_year_update:
            self.console_logger.info(
                "Force mode is ON. All %d unique albums will be fully processed.",
                len(
                    {
                        (t.get("artist"), t.get("album"))
                        for t in all_tracks
                        if t.get("artist") and t.get("album")
                    },
                ),
            )
            # In force mode, override the identified albums to include all unique albums in all_tracks
            albums_for_full_processing = {
                (t.get("artist", "Unknown"), t.get("album", "Unknown")): "Force mode"
                for t in all_tracks
                if t.get("artist") and t.get("album")
            }

        self.console_logger.info(
            "Identified %d unique albums for full processing.",
            len(albums_for_full_processing),
        )

        # At this point, albums_for_full_processing contains the (artist, album) keys
        # that need full processing. The next step (Subtask 3) will be to modify
        # how the processing functions (like process_album_years) filter tracks
        # to only process tracks belonging to these identified albums.

        # For now, the existing filtering logic by date added below will still apply
        # until we implement Subtask 3.
        # This means currently, we IDENTIFY albums for full processing, but the actual
        # processing steps (genre, year) still only work on tracks added since the last run.
        # We will address this discrepancy in the next step.albums_to_verify = [(artist, album, timestamp) for timestamp, artist, album in all_pending_raw] # noqa: E501

        # Pass effective_last_run and albums_for_full_processing to processing functions
        # (This will require modifying their signatures in the next step)

        # Use injected process_album_years method
        updated_y, changes_y = await self.process_album_years(
            all_tracks,
            albums_for_full_processing,
            force=force_year_update,
        )  # Pass all_tracks

        # Save results if updates occurred
        if updated_y:
            self.console_logger.info(
                "Year updates resulted in %d tracks needing DB sync.",
                len(updated_y),
            )
            # Sync updated tracks (containing new years) with the main CSV (utility function)
            # Use config from self.config, injected cache_service, loggers
            # We should sync ALL tracks after potentially updating some, to reflect the new years
            # This ensures the CSV accurately represents the state after the run.
            # sync_track_list_with_current reads the existing CSV, updates based on the provided list (all_tracks)
            # and saves the merged result. So passing `all_tracks` which contains the updated years is correct.
            await sync_track_list_with_current(
                all_tracks,  # Pass the fully updated list of tracks
                get_full_log_path(
                    self.config,
                    "csv_output_file",
                    "csv/track_list.csv",
                    self.error_logger,
                ),  # Pass error_logger
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=not force_year_update,  # Use partial sync for incremental, full sync for force
            )
            # Save a report of the year changes (utility function)
            # Use config from self.config, injected loggers
            save_unified_changes_report(
                changes_y,
                get_full_log_path(
                    self.config,
                    "changes_report_file",
                    "csv/changes_report.csv",
                    self.error_logger,
                ),  # Pass error_logger
                self.console_logger,
                self.error_logger,
                force_mode=force_year_update,  # Use force flag for console output
            )
            self.console_logger.info(
                "Processed and logged %d year changes.",
                len(changes_y),
            )  # Log changes count
            # Update last run time only if it was an incremental run (not forced) AND was for all artists
            if not force_year_update and artist is None:
                await self.update_last_incremental_run()  # Use method of this class

        else:
            self.console_logger.info(
                "No year updates needed or year retrieval disabled.",
            )
            # Update last run time only if it was an incremental run (not forced) AND was for all artists,
            # even if no updates were made, to respect the interval.
            if not force_year_update and artist is None:
                await self.update_last_incremental_run()  # Use method of this class

    # Logic moved from old run_verify_database
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Verify Database")
    async def run_verify_database(self, force: bool) -> None:
        """Executes the database verification process.

        Uses injected verify_and_clean_track_database method.
        """
        self.console_logger.info(
            f"Executing verify_database command with force={force}",
        )
        # Use the method of this class
        removed_count = await self.verify_and_clean_track_database(force=force)
        if removed_count > 0:
            self.console_logger.info(
                "Database cleanup removed %d non-existent tracks",
                removed_count,
            )
        else:
            self.console_logger.info(
                "Database verification completed. No non-existent tracks found.",
            )

        # Update last verification timestamp regardless of whether tracks were removed
        # This logic is inside verify_and_clean_track_database method now.

    # Logic moved from old run_verify_pending
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Verify Pending")
    async def run_verify_pending(self, force: bool) -> None:
        """Executes verification for all pending albums regardless of timeframe if force is True.

        Or only those due for verification based on the pending interval if force is False.
        Uses injected services and config from self.config.
        """
        self.console_logger.info(f"Executing verify_pending command with force={force}")

        # Use injected pending_verification_service
        if not self.pending_verification_service:
            self.error_logger.error(
                "PendingVerificationService is not available. Cannot verify pending albums.",
            )
            return

        albums_to_verify: list[tuple[str, str, datetime]] = (
            []
        )  # List to store (artist, album, timestamp) tuples

        if force:
            # Get all pending albums if force is true
            # get_all_pending_albums is async
            all_pending_raw = (
                await self.pending_verification_service.get_all_pending_albums()
            )
            albums_to_verify = [
                (artist, album, timestamp)
                for timestamp, artist, album in all_pending_raw
            ]
            self.console_logger.info(
                f"Force verification for all {len(albums_to_verify)} pending albums.",
            )
        else:
            # Get album keys needing verification based on interval
            # get_verified_album_keys is async
            verified_keys = (
                await self.pending_verification_service.get_verified_album_keys()
            )
            self.console_logger.info(
                f"Found {len(verified_keys)} album keys needing verification based on interval.",
            )

            # Retrieve artist/album names for the verified keys from the pending service cache
            # Accessing the cache directly for names is less ideal, a method in PendingVerificationService
            # to get details by key would be better, or iterate the result of get_all_pending_albums
            # and filter by keys. Let's iterate get_all_pending_albums and filter.
            if verified_keys:
                all_pending_raw = (
                    await self.pending_verification_service.get_all_pending_albums()
                )
                # Use the same key generation as PendingVerificationService to filter
                for (
                    timestamp,
                    artist,
                    album,
                ) in (
                    all_pending_raw
                ):  # Note: tuple is (timestamp, artist, album) in service
                    album_key = self.pending_verification_service._generate_album_key(
                        artist,
                        album,
                    )  # Use service's internal key generation
                    if album_key in verified_keys:
                        albums_to_verify.append((artist, album, timestamp))

        if not albums_to_verify:
            self.console_logger.info("No albums currently need verification.")
            return

        verification_updated_tracks = []
        verification_changes = []

        # Fetch all tracks to find tracks belonging to pending albums
        # Use injected fetch_tracks_async method
        # We need track objects for `process_album_years`, don't force refresh unless strictly necessary
        # to avoid unnecessary AppleScript calls, relying on cache if available.
        all_tracks_fetched = await self.fetch_tracks_async(force_refresh=False)

        if not all_tracks_fetched:
            self.console_logger.warning(
                "Could not fetch tracks to verify pending albums.",
            )
            return

        # Group tracks by album (artist, album) for easy lookup
        all_tracks_grouped_by_album: dict[tuple[str, str], list[dict[str, str]]] = (
            defaultdict(list)
        )
        for track in all_tracks_fetched:
            artist = track.get("artist", "").strip()
            album = track.get("album", "").strip()
            if artist and album:
                all_tracks_grouped_by_album[artist, album].append(track)

        # Process each album that needs verification
        for (
            artist,
            album,
            _,
        ) in albums_to_verify:  # Iterate through (artist, album, timestamp) tuples
            try:
                self.console_logger.info("Verifying album '%s - %s'", artist, album)

                # Find tracks for this album using the grouped map
                album_tracks = all_tracks_grouped_by_album.get((artist, album), [])

                if album_tracks:
                    # Force verification regardless of current year values for pending albums
                    # Create a dictionary with the current album to process
                    albums_to_process = {
                        (artist, album): "",
                    }  # Empty string as placeholder value
                    # Use the method of this class. Pass force=True to process_album_years
                    # to ensure it fetches from API/re-scores.
                    (
                        verified_tracks_for_album,
                        verified_changes_for_album,
                    ) = await self.process_album_years(
                        album_tracks,
                        albums_to_process,
                        force=True,
                    )

                    if verified_tracks_for_album:
                        verification_updated_tracks.extend(verified_tracks_for_album)
                        verification_changes.extend(verified_changes_for_album)
                        # remove_from_pending is now called inside process_album_years if a definitive year is found
                        # If process_album_years didn't find a definitive year, it re-marked as pending.
                        self.console_logger.info(
                            "Successfully processed and potentially updated album '%s - %s' during verification.",
                            artist,
                            album,
                        )
                    else:
                        self.console_logger.info(
                            "Processing album '%s - %s' during verification did not result in updates.",
                            artist,
                            album,
                        )
                        # If process_album_years didn't result in updates or definitive year, it's still pending.
                        # No need to call remove_from_pending here.

                else:
                    self.console_logger.warning(
                        "No tracks found in fetched data for album '%s - %s' needing verification. Removing from pending list.",
                        artist,
                        album,
                    )
                    # If no tracks found in the library for this album, remove it from pending list to avoid endless checks.
                    if self.pending_verification_service:
                        await self.pending_verification_service.remove_from_pending(
                            artist,
                            album,
                        )  # remove_from_pending is async

            except Exception as e:
                self.error_logger.error(
                    f"Error verifying album '{artist} - {album}': {e}",
                    exc_info=True,
                )

        # Add verification results to other changes (these will be saved by the main process if run_full_process calls this)
        # If run_verify_pending is a standalone command, we need to save the changes here.
        if verification_updated_tracks or verification_changes:
            self.console_logger.info(
                "Verification of pending albums resulted in %d track updates and %d changes.",
                len(verification_updated_tracks),
                len(verification_changes),
            )
            # Save the results if this is a standalone command
            # Need to decide how updates from verify_pending integrate into the main track_list.csv
            # The process_album_years call within the loop already updates the in-memory state
            # and saves the *year* changes to the report file via `save_unified_changes_report`.
            # The main `sync_track_list_with_current` should handle saving the updated track list.
            # So, if this is a standalone command, we should trigger the main sync after the loop.

            # Use config from self.config
            csv_output_file_path = get_full_log_path(
                self.config,
                "csv_output_file",
                "csv/track_list.csv",
                self.error_logger,
            )  # Pass error_logger
            changes_report_file_path = get_full_log_path(
                self.config,
                "changes_report_file",
                "csv/changes_report.csv",
                self.error_logger,
            )  # Pass error_logger

            # Need to ensure all_tracks_fetched list is updated with changes before syncing.
            # The tracks updated within process_album_years (called from verify_pending) are
            # modified in-place if they are part of the `album_tracks` list passed to it.
            # Since `album_tracks` is a subset of `all_tracks_fetched`, `all_tracks_fetched`
            # should contain the updated tracks.
            await sync_track_list_with_current(
                all_tracks_fetched,  # Pass the list that potentially contains updated tracks
                csv_output_file_path,
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=True,  # Verification is always a partial sync of existing entries
            )

            # Append the verification changes to the changes report file
            # save_unified_changes_report appends if file exists, or creates new.
            save_unified_changes_report(
                verification_changes,
                changes_report_file_path,
                self.console_logger,
                self.error_logger,
                force_mode=force,  # Use force flag for console output
            )

        else:
            self.console_logger.info(
                "No updates resulted from verifying pending albums.",
            )

    # Logic for the main execution methods of MusicUpdater
    # These methods orchestrate the process using the injected services (self.)
    # and the utility functions defined above (or imported).

    # Example of a method that uses utility functions and other methods

    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Full Process")
    async def run_full_process(self, args: argparse.Namespace) -> None:
        """Runs the default full update process (cleaning, genres, years, pending verification).

        Uses injected services and config from self.config.
        """
        self.console_logger.info(
            "Running in default mode (incremental or full processing).",
        )

        # Check if we can run incrementally using the method of this class
        can_run = await self.can_run_incremental(force_run=args.force)

        if not can_run:
            self.console_logger.info(
                "Incremental interval not reached. Skipping main processing.",
            )
            return  # Exit if interval not met and not forced

        # Determine last run time for incremental filtering
        effective_last_run = datetime.min  # Default to beginning of time for full run
        if not args.force:  # Only check last run time if not forcing
            try:
                # Use injected cache_service
                last_run_time = await self.cache_service.get_last_run_timestamp()
                self.console_logger.info(
                    "Last incremental run timestamp: %s",
                    (
                        last_run_time.strftime("%Y-%m-%d %H:%M:%S")
                        if last_run_time != datetime.min
                        else "Never or Failed"
                    ),
                )  # Log full timestamp
                effective_last_run = (
                    last_run_time  # Use actual last run time for incremental
                )
            except Exception as e:
                self.error_logger.warning(
                    f"Could not get last run timestamp from cache service: {e}. Assuming full run.",
                )
                effective_last_run = (
                    datetime.min
                )  # Fallback to full run if timestamp retrieval fails

        # --- Fetch Tracks (respecting test_artists if set globally) ---
        all_tracks: list[dict[str, str]] = []
        # Use config from self.config
        test_artists_config = self.config.get("development", {}).get("test_artists", [])

        if test_artists_config:
            # Fetch tracks only for test artists if the list is populated
            self.console_logger.info(
                f"Development mode: Fetching tracks only for test_artists: {test_artists_config}",
            )
            fetched_track_map = (
                {}
            )  # Use map to avoid duplicates if artist listed multiple times
            for art in test_artists_config:
                # Use injected fetch_tracks_async method
                art_tracks = await self.fetch_tracks_async(
                    artist=art,
                    force_refresh=args.force,
                )
                for track in art_tracks:
                    if track.get("id"):
                        fetched_track_map[track["id"]] = track
            all_tracks = list(fetched_track_map.values())
            self.console_logger.info(
                "Loaded %d tracks total for test artists.",
                len(all_tracks),
            )
        else:
            # Fetch all tracks if test_artists list is empty
            # Use injected fetch_tracks_async method
            all_tracks = await self.fetch_tracks_async(force_refresh=args.force)
            self.console_logger.info(
                "Loaded %d tracks from Music.app.",
                len(all_tracks),
            )

        if not all_tracks:
            self.console_logger.warning("No tracks fetched or found for processing.")
            # Update last run time even if no tracks processed, to respect interval
            if not args.force:
                # Use injected update_last_incremental_run method
                await self.update_last_incremental_run()
            return

        # Make copies AFTER filtering to avoid modifying cached data if applicable
        all_tracks = [track.copy() for track in all_tracks]
        updated_tracks_cleaning = []
        changes_log_cleaning = []

        # --- Step 1: Clean Track/Album Names ---
        self.console_logger.info("Starting track name cleaning process...")

        # Define async helper for cleaning a single track
        # This helper needs access to the update_track_async method and clean_names utility
        async def clean_track_default(track: dict[str, str]) -> None:
            nonlocal updated_tracks_cleaning, changes_log_cleaning  # Access outer scope lists
            orig_name = track.get("name", "")
            orig_album = track.get("album", "")
            track_id = track.get("id", "")
            artist_name = track.get("artist", "Unknown")  # Use artist from track data

            if not track_id:
                return  # Skip if no ID

            # Use the utility function clean_names, pass config and loggers from self.config/self
            cleaned_nm, cleaned_al = clean_names(
                artist_name,
                orig_name,
                orig_album,
                self.config,
                self.console_logger,
                self.error_logger,
            )

            # Determine if changes were actually made
            new_tn = cleaned_nm if cleaned_nm != orig_name else None
            new_an = cleaned_al if cleaned_al != orig_album else None

            if new_tn or new_an:
                track_status = track.get("trackStatus", "").lower()
                if track_status in ("subscription", "downloaded"):
                    # Use the method of this class to update the track
                    if await self.update_track_async(
                        track_id,
                        new_track_name=new_tn,
                        new_album_name=new_an,
                    ):
                        # If update successful, modify track dict and log changes
                        if new_tn:
                            track["name"] = cleaned_nm
                        if new_an:
                            track["album"] = cleaned_al
                        # Append the modified track dictionary to the list
                        updated_tracks_cleaning.append(track)
                        # Log the change details
                        changes_log_cleaning.append(
                            {
                                "change_type": "name",
                                "artist": artist_name,
                                "album": track.get("album", "Unknown"),
                                "track_name": orig_name,  # Original name before cleaning
                                "old_track_name": orig_name,
                                "new_track_name": cleaned_nm,
                                "old_album_name": orig_album,
                                "new_album_name": cleaned_al,
                                "timestamp": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S",
                                ),
                            },
                        )
                    else:
                        self.error_logger.error(
                            "Failed to apply cleaning update for track ID %s ('%s' - '%s')",
                            track_id,
                            artist_name,
                            orig_name,
                        )  # Log track details
                else:
                    self.console_logger.debug(
                        f"Skipping track update for '{orig_name}' (ID: {track_id}) due to status '{track_status}'",
                    )

        # Run cleaning tasks concurrently
        clean_tasks_default = [
            asyncio.create_task(clean_track_default(t)) for t in all_tracks
        ]
        await asyncio.gather(*clean_tasks_default)
        if updated_tracks_cleaning:
            self.console_logger.info(
                "Cleaned %d track/album names.",
                len(updated_tracks_cleaning),
            )
        else:
            self.console_logger.info("No track names or album names needed cleaning.")

        # --- Step 2: Update Genres ---
        self.console_logger.info("Starting genre update process...")
        # Pass the effective last run time (min if forced, actual otherwise)
        # Use the method of this class
        updated_g, changes_g = await self.update_genres_by_artist_async(
            all_tracks,
            effective_last_run,
        )  # Pass all_tracks
        if updated_g:
            self.console_logger.info("Updated genres for %d tracks.", len(updated_g))
        else:
            self.console_logger.info(
                "No genre updates needed based on incremental check or existing genres.",
            )

        # --- Step 3: Update Album Years ---
        self.console_logger.info("Starting album year update process...")
        # In full process mode, we'll process all albums by passing an empty dictionary
        # This will make process_album_years process all albums in all_tracks
        updated_y, changes_y = await self.process_album_years(
            all_tracks,
            {},
            force=args.force,
        )
        if updated_y:
            self.console_logger.info("Updated years for %d tracks.", len(updated_y))
        else:
            self.console_logger.info(
                "No year updates needed or year retrieval disabled.",
            )

        # --- Consolidate Changes and Save ---
        # Combine all change logs
        all_changes = changes_log_cleaning + changes_g + changes_y

        # The all_tracks list now contains all modifications from cleaning, genres, and years
        if all_changes:  # Check if any changes were logged
            self.console_logger.info("Consolidating and saving results...")
            # Use utility function sync_track_list_with_current
            # Use config from self.config, injected cache_service, loggers
            await sync_track_list_with_current(
                all_tracks,  # Pass the fully updated list of tracks
                get_full_log_path(
                    self.config,
                    "csv_output_file",
                    "csv/track_list.csv",
                    self.error_logger,
                ),  # Pass error_logger
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=not args.force,  # Use partial sync for incremental, full sync for force
            )
            # Use utility function save_unified_changes_report
            # Use config from self.config, injected loggers
            save_unified_changes_report(
                all_changes,
                get_full_log_path(
                    self.config,
                    "changes_report_file",
                    "csv/changes_report.csv",
                    self.error_logger,
                ),  # Pass error_logger
                self.console_logger,
                self.error_logger,
                force_mode=args.force,  # Use force flag for console output if needed
            )
            self.console_logger.info(
                "Processing complete. Logged %d changes.",
                len(all_changes),
            )
        else:
            self.console_logger.info("No changes detected in this run.")

        # Update last run time only if it was an incremental run (not forced)
        if not args.force:
            # Use injected update_last_incremental_run method
            await self.update_last_incremental_run()

        # --- Step 4: Verify Pending Albums (after 30 days) ---
        # This step is part of the default process
        # Use the method of this class. This will call process_album_years internally
        # for pending albums and handle updating the pending list and reporting changes.
        await self.run_verify_pending(
            force=False,
        )  # Run pending verification based on interval


# --- Argument Parsing ---
def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments using argparse."""
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force run, bypassing incremental checks and cache.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate changes without applying them.",
    )
    subparsers = parser.add_subparsers(dest="command")

    clean_artist_parser = subparsers.add_parser(
        "clean_artist",
        help="Clean track/album names for a given artist.",
    )
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name.")
    # Removed --force from subcommand, use global --force

    update_years_parser = subparsers.add_parser(
        "update_years",
        help="Update album years from external APIs.",
    )
    update_years_parser.add_argument("--artist", help="Artist name (optional).")
    # Removed --force from subcommand, use global --force

    args = parser.parse_args()

    # If a subcommand was used, ensure the global --force flag is transferred
    # to the subcommand's namespace for consistency if needed, although
    # the logic now primarily checks args.force directly.
    # if args.command and hasattr(args, 'force') and args.force:
    #     # This is handled by argparse automatically if global --force is before subcommand
    #     # but explicit transfer might be needed depending on exact argparse usage/version
    #     pass # No change needed here with standard argparse

    return args


def main() -> None:
    """Main synchronous function to parse arguments and run the async main logic.

    Handles top-level setup, teardown, exception handling, and listener shutdown.
    Initializes loggers and DependencyContainer, orchestrates command execution.
    """
    # Import DependencyContainer here to break the circular import and make it available to main's scope
    # Ensure DependencyContainer is imported at the function level if needed only here,
    # or at the top level if used elsewhere in this file outside of methods.
    # It's used in the main function only, so importing here is acceptable for breaking the cycle.
    from services.dependencies_service import DependencyContainer

    start_all = time.time()
    args = parse_arguments()

    # Initialize loggers and the QueueListener *before* creating DependencyContainer
    # These are needed by the DependencyContainer constructor and for early logging
    # Ensure CONFIG is loaded at the module level or before this point
    # CONFIG is loaded at the module level in this file, needed by get_loggers.
    # If CONFIG loading fails at module level, script exits early.
    # Assuming this is handled correctly.

    # Initialize loggers and the QueueListener using the module-level CONFIG
    # This is the ONLY place get_loggers should be called
    local_console_logger = None  # Initialize to None for error handling in finally
    local_error_logger = None
    local_analytics_logger = None
    local_listener = None  # Initialize listener to None
    deps = None  # Initialize deps to None for finally block

    try:
        # Check if CONFIG exists (it should if module level load passed)
        if "CONFIG" not in globals() or not isinstance(CONFIG, dict):
            # This case should ideally not be reached if module level load works
            raise ValueError("CONFIG not loaded or invalid at module level.")

        # Initialize loggers and the QueueListener using the module-level CONFIG
        # Store results in local variables
        (
            local_console_logger,
            local_error_logger,
            local_analytics_logger,
            local_listener,
        ) = get_loggers(CONFIG)

        # Log that loggers are initialized (should see this only ONCE)
        local_console_logger.info(
            "Main function: Loggers and QueueListener initialized.",
        )
        # Check if listener is None (get_loggers might fail)
        if local_listener is None:
            local_console_logger.error(
                "Main function: QueueListener failed to initialize. File logs might be missing.",
            )

        # --- Add safety check for listener (redundant if checked above, but harmless) ---
        # if local_listener is None:
        #     local_console_logger.error("Failed to initialize QueueListener for logging. File logs might be missing.")

        # --- Perform initial path checks ---
        # Call check_paths here, after loggers are initialized
        # Pass CONFIG and the local error logger
        check_paths(
            [CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]],
            local_error_logger,
        )

        # --- Handle Dry Run mode ---
        # Dry run logic is now handled by calling dry_run.main() which has its own DI setup
        if args.dry_run:
            local_console_logger.info(
                "Running in dry-run mode. No changes will be applied.",
            )
            try:
                # dry_run.main() is an async function, need asyncio.run here
                # dry_run.main() now gets its own loggers and config internally.
                # Ideally, dry_run.main should receive loggers/listener from here to share
                # the same logging setup, but for now, we keep it separate as refactored earlier.
                # This part needs careful consideration for shared logging/DI in dry run.
                # For now, stick to the current call, acknowledging dry_run might have its own logger setup.
                dry_run.main()
            except Exception as dry_run_err:
                local_error_logger.error(
                    f"Error during dry run: {dry_run_err}",
                    exc_info=True,
                )
                sys.exit(1)  # Exit with error code
            # After dry run completes, exit the script. Cleanup is in finally.
            sys.exit(0)

        # --- Dependency Injection Container Setup ---
        deps = None  # Initialize to None to prevent 'possibly unbound' warning
        try:
            # Create the DependencyContainer instance, passing config path and the initialized loggers/listener
            # Pass the local logger and listener variables
            deps = DependencyContainer(
                CONFIG_PATH,
                local_console_logger,
                local_error_logger,
                local_analytics_logger,
                local_listener,
            )

            # --- Asynchronously Initialize Dependencies ---
            # IMPORTANT: Call the async initialize method before using services
            # This must be awaited, so we run it using asyncio.run
            local_console_logger.info(
                "Starting DependencyContainer asynchronous initialization...",
            )
            try:
                # Need a wrapper async function to await deps.initialize()
                async def initialize_dependencies_async(deps_instance: DependencyContainer) -> None:
                    await deps_instance.initialize()

                asyncio.run(initialize_dependencies_async(deps))
                local_console_logger.info(
                    "DependencyContainer asynchronous initialization completed successfully.",
                )
            except Exception as init_err:
                local_error_logger.critical(
                    f"Critical error during DependencyContainer asynchronous initialization: {init_err}",
                    exc_info=True,
                )
                # If async initialization fails, the application likely cannot proceed
                sys.exit(1)
        except Exception as e:
            if "local_error_logger" in locals() and local_error_logger:
                local_error_logger.critical(
                    f"Unexpected error during initialization: {e}",
                    exc_info=True,
                )
            else:
                print(
                    f"CRITICAL ERROR: Unexpected error during initialization: {e}",
                    file=sys.stderr,
                )
                import traceback

                traceback.print_exc(file=sys.stderr)
            sys.exit(1)

        # --- Run Main Commands ---
        # Wrap the main command execution in an async function
        async def run_main_commands(
            deps: DependencyContainer,
            args: argparse.Namespace,
        ) -> None:
            # Get the main orchestrator instance from the container
            music_updater = deps.get_music_updater()

            # Always check if Music.app is running first (utility function)
            # Pass the error logger from deps
            error_logger_for_check = deps.get_error_logger()
            # is_music_app_running requires error_logger
            if not is_music_app_running(error_logger_for_check):
                deps.get_console_logger().error(
                    "Music app is not running! Please start Music.app before running this script.",
                )
                sys.exit(1)  # Exit if Music app is not running

            # --- Command-Specific Execution Paths ---
            # Call the corresponding methods on the music_updater instance
            # These methods are async and need to be awaited
            if hasattr(args, "command") and args.command == "verify_database":
                await music_updater.run_verify_database(force=args.force)

            elif args.command == "clean_artist":
                await music_updater.run_clean_artist(
                    artist=args.artist,
                    force=args.force,
                )

            elif args.command == "update_years":
                await music_updater.run_update_years(
                    artist=args.artist,
                    force=args.force,
                )

            elif args.command == "verify_pending":
                await music_updater.run_verify_pending(
                    force=args.force,
                )  # Pass force argument

            else:  # Default mode
                await music_updater.run_full_process(args)

        # Run the async command execution wrapper function
        # This runs the main logic after successful async initialization
        try:
            # This asyncio.run starts the main event loop for the command execution
            asyncio.run(run_main_commands(deps, args))
        except KeyboardInterrupt:
            # Log interruption using loggers from deps
            deps.get_console_logger().info(
                "Script interrupted by user (main commands).",
            )
            sys.exit(1)  # Exit with error code upon interruption
        except Exception as command_exec_err:
            # Log critical error using loggers from deps
            deps.get_error_logger().critical(
                "Critical error during main command execution: %s",
                command_exec_err,
                exc_info=True,
            )
            sys.exit(1)  # Exit with error code

    except FileNotFoundError as e:
        # Catch config loading error (re-raised by DependencyContainer __init__)
        # Loggers are initialized *before* deps, so local loggers should be available.
        if "local_error_logger" in locals() and local_error_logger:
            local_error_logger.critical(f"Configuration file not found: {e}")
        else:
            # Fallback print if loggers weren't initialized
            print(f"CRITICAL ERROR: Configuration file not found: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        # Catch config validation error (re-raised by DependencyContainer __init__)
        if "local_error_logger" in locals() and local_error_logger:
            local_error_logger.critical(f"Configuration validation error: {e}")
        else:
            print(
                f"CRITICAL ERROR: Configuration validation error: {e}",
                file=sys.stderr,
            )
        sys.exit(1)
    except yaml.YAMLError as e:
        # Catch config parsing error (re-raised by DependencyContainer __init__)
        if "local_error_logger" in locals() and local_error_logger:
            local_error_logger.critical(f"Configuration parsing error: {e}")
        else:
            print(f"CRITICAL ERROR: Configuration parsing error: {e}", file=sys.stderr)
        sys.exit(1)
    # Other exceptions from DependencyContainer __init__ (e.g. Import Error) will also be caught here.
    except Exception as deps_init_err:
        if "local_error_logger" in locals() and local_error_logger:
            local_error_logger.critical(
                "Critical error during DependencyContainer initialization: %s",
                deps_init_err,
                exc_info=True,
            )
        else:
            print(
                f"CRITICAL ERROR: Critical error during DependencyContainer initialization: {deps_init_err}",
                file=sys.stderr,
            )
            import traceback

            traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    finally:
        # --- Final Cleanup ---
        # Shutdown DependencyContainer if it was successfully created
        # This handles closing services and stopping the listener
        if deps:
            try:
                deps.shutdown()
            except Exception as shutdown_err:
                # Use error logger from deps if available, otherwise local error logger
                error_logger_for_shutdown = (
                    deps.get_error_logger()
                    if deps and hasattr(deps, "get_error_logger")
                    else ("local_error_logger" in locals() and local_error_logger)
                    or None
                )
                if error_logger_for_shutdown:
                    error_logger_for_shutdown.error(
                        f"Error during DependencyContainer shutdown: {shutdown_err}",
                        exc_info=True,
                    )
                else:
                    print(
                        f"ERROR: Error during DependencyContainer shutdown: {shutdown_err}",
                        file=sys.stderr,
                    )

        # Explicitly close handlers for robustness.
        # This is handled by deps.shutdown() if deps was created.
        # If deps was NOT created due to an error before its initialization (e.g. config error),
        # the local loggers might still need explicit handler closing if they had file handlers added by get_loggers.
        # The get_loggers implementation in utils/logger.py adds RunTrackingHandler and QueueHandler.
        # RunTrackingHandler's close method calls super().close() and trim_log_to_max_runs.
        # QueueListener's stop() method calls close() on its handlers.
        # So if deps.shutdown() is called, the listener is stopped and handlers are closed.
        # If deps is NOT created, the listener started by get_loggers needs to be stopped,
        # and its handlers closed.
        # Let's make sure the listener is stopped directly if deps is not created.
        # And rely on listener stopping to close handlers.

        # Simplified cleanup in finally
        # If deps exists, it calls deps.shutdown() which stops the listener and closes handlers.
        # If deps does NOT exist (error before deps = ...), the local listener was created
        # by get_loggers and needs to be stopped. Its handlers will be closed by listener.stop().
        # This case handles errors *before* deps = DependencyContainer(...).
        # In case of errors *within* the try block *after* deps is created, the `if deps:` block above runs.
        # This structure seems correct for cleanup regardless of where the error occurs.
        # The local_listener is only stopped here if deps is not created.
        elif "local_listener" in locals() and local_listener:
            try:
                # Use the console logger if available, otherwise print
                logger_for_listener_stop = (
                    "local_console_logger" in locals() and local_console_logger
                ) or None
                if logger_for_listener_stop:
                    logger_for_listener_stop.info(
                        "Stopping QueueListener (deps not created)...",
                    )
                else:
                    print(
                        "INFO: Stopping QueueListener (deps not created)...",
                        file=sys.stderr,
                    )

                local_listener.stop()
            except Exception as listener_stop_err:
                if "local_error_logger" in locals() and local_error_logger:
                    local_error_logger.error(
                        f"Error stopping QueueListener (deps not created): {listener_stop_err}",
                        exc_info=True,
                    )
                else:
                    print(
                        f"ERROR: Error stopping QueueListener (deps not created): {listener_stop_err}",
                        file=sys.stderr,
                    )

        # Final timing log
        end_all = time.time()
        # Use console logger from deps if available, otherwise local console logger
        console_logger_for_final = (
            deps.get_console_logger()
            if deps and hasattr(deps, "get_console_logger")
            else ("local_console_logger" in locals() and local_console_logger) or None
        )
        if console_logger_for_final:
            console_logger_for_final.info(
                f"\nTotal script execution time: {end_all - start_all:.2f} seconds",
            )
        else:
            # Fallback print if no loggers were successfully initialized at all
            print(
                f"\nTotal script execution time: {end_all - start_all:.2f} seconds",
                file=sys.stderr,
            )

        # sys.exit(0) # Exit status is handled by explicit sys.exit calls above


# --- Initial checks and entry point ---
# CONFIG is loaded at module level.
# check_paths is called in main now.
# The main function is the entry point.

if __name__ == "__main__":
    main()
