#!/usr/bin/env python3

"""
Music Genre Updater Script v3.0

This script automatically manages your Music.app library by updating genres, cleaning track/album names,
retrieving album years, and verifying the integrity of its track database. It operates asynchronously
for improved performance and uses caching to minimize AppleScript calls and API usage.

Key Features:
    - Genre harmonization: Assigns consistent genres to an artist's tracks based on their earliest releases.
    - Track/album name cleaning: Removes remaster tags, promotional text, and other clutter from titles based on configurable rules.
    - Album year retrieval: Fetches and updates year information from external music databases (e.g., MusicBrainz) with caching and rate limiting.
    - Database verification: Checks the script's internal track list (CSV) against Music.app and removes entries for tracks that no longer exist.
    - Incremental processing: Efficiently updates only tracks added since the last run, respecting a configurable interval.
    - Analytics tracking: Monitors performance and operations with detailed logging and reports.
    - Pending verification system: Tracks album years from APIs that might need re-verification after a set period (e.g., N days) to improve accuracy.
    - Test mode: Allows limiting processing to specific artists defined in the configuration for easier testing.
    - Dry run mode: Simulates all operations without modifying the Music library or saving reports/CSV updates.

Architecture:
    - MusicUpdater class: Orchestrates the music library, including cleaning names, updating genres, retrieving years, and database verification.
    - DependencyContainer: Manages service instances and dependencies, ensuring proper initialization and resource management.
    - Utility functions: Handle parsing tracks, cleaning names, determining genres, and other helper operations.

Commands:
    - Default (no arguments): Runs the main processing pipeline (clean names, update genres, update years). Operates incrementally unless forced.
    - clean_artist: Cleans track/album names only for tracks by a specific artist.
    - update_years: Updates album years from external APIs, optionally filtered by artist.
    - verify_database: Checks the internal track database (CSV) and removes entries for tracks that no longer exist. Respects interval unless forced.
    - verify_pending: Triggers verification for all albums marked as pending, regardless of their individual timers.

Options:
    - --force: Overrides incremental interval checks, forces cache refresh for fetches, database verification, and processing of all relevant items.
    - --dry-run: Simulates changes without modifying the Music library or saving reports/CSV updates.
    - --artist: Specify target artist (used with `clean_artist` or `update_years` commands).

Services injected via DependencyContainer:
    - AppleScriptClient: For interactions with Music.app via `osascript`.
    - CacheService: For storing fetched track data, album years, and run timestamps between executions.
    - ExternalAPIService: For querying album years from external music databases.
    - PendingVerificationService: For tracking album years that require future re-verification.
    - Analytics: For tracking performance metrics and operation counts.

Configuration (`my-config.yaml`) includes settings for:
    - Paths: `music_library_path`, `apple_scripts_dir`, `logs_base_dir`.
    - Year Retrieval: `year_retrieval` (API limits, batch sizes, adaptive delays, enabling/disabling).
    - Cleaning: `cleaning` (remaster_keywords, album_suffixes_to_remove).
    - Incremental Updates: `incremental_interval_minutes`.
    - Batching: `batch_size` (general), specific batch sizes within modules.
    - Database Verification: `database_verification` (auto_verify_days, batch_size).
    - Exceptions: `exceptions` (rules to skip cleaning for specific artists/albums).
    - Development: `development` (`test_artists` list to limit processing scope).
    - Logging: Log file paths and levels.

Example usage:
    python music_genre_updater.py --force --dry-run
    python music_genre_updater.py clean_artist --artist "Artist Name"
    python music_genre_updater.py update_years --artist "Artist Name"
    python music_genre_updater.py verify_database
    python music_genre_updater.py verify_pending
"""

import argparse
import asyncio
import logging
import os
import re
import subprocess
import sys
import time

from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

# Import necessary services and utilities that are NOT part of the dependency cycle
# DependencyContainer is imported inside main()
from utils import dry_run  # dry_run will be refactored later
from utils.analytics import Analytics  # Analytics is imported here as it's used in method decorators
from utils.config import load_config
from utils.logger import get_full_log_path, get_loggers
from utils.reports import save_changes_report  # This function might be removed or refactored later
from utils.reports import (
    load_track_list,
    save_to_csv,
    save_unified_changes_report,
    sync_track_list_with_current,
)

# Use TYPE_CHECKING to avoid circular import issues during runtime for type hints
if TYPE_CHECKING:
    from services.applescript_client import AppleScriptClient
    from services.cache_service import CacheService
    from services.external_api_service import ExternalApiService
    from services.pending_verification import PendingVerificationService


# Load configuration from YAML early
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "my-config.yaml")

# Global variable for config - kept for functions that still need it before full refactor
# or for utility functions that don't belong to a class instance.
# This should be reviewed later.
CONFIG = load_config(CONFIG_PATH)


# Check essential paths early
def check_paths(paths: List[str], logger: logging.Logger) -> None:
    """
    Check if the specified paths exist and are readable.
    """
    for path in paths:
        if not os.path.exists(path):
            logger.error(f"Path {path} does not exist.")
            raise FileNotFoundError(f"Path {path} does not exist.")
        if not os.access(path, os.R_OK):
            logger.error(f"No read access to {path}.")
            raise PermissionError(f"No read access to {path}.")


# Initialize loggers early
# The listener needs to be started and stopped in the main execution block
console_logger, error_logger, analytics_logger, listener = get_loggers(CONFIG)

# Perform path checks after loggers are initialized
try:
    check_paths([CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]], error_logger)
except (FileNotFoundError, PermissionError) as e:
    error_logger.critical(f"Initial path check failed: {e}. Exiting.")
    # In a real application, you might exit here or handle differently
    # For this refactoring, we'll let the main execution handle potential exits.
    pass  # Allow script to continue for now, main() will handle exit

# --- Utility Functions (can remain here or be moved to a separate utils module) ---
# These functions do not need access to self or injected services


# Logic moved from old parse_tracks
# Logic moved from old parse_tracks
def parse_tracks(raw_data: str) -> List[Dict[str, str]]:
    """
    Parses the raw data from AppleScript into a list of track dictionaries.
    Uses the Record Separator (U+001E) as the field delimiter.
    """
    # Note: This function no longer uses self.error_logger directly
    # If detailed error logging is needed here, consider passing a logger instance
    # or relying on exceptions being caught by the caller.
    # For now, basic print to stderr or using a module-level logger fallback is implied
    # if called in a context without the main loggers being fully set up.

    # Define the new field separator (Record Separator U+001E)
    field_separator = '\x1e'

    if not raw_data:
        # Use module-level error_logger if available, otherwise print
        if 'error_logger' in globals() and error_logger:
            error_logger.error("No data fetched from AppleScript.")
        else:
            print("ERROR: No data fetched from AppleScript.", file=sys.stderr)
        return []

    tracks = []
    # Split raw data into rows using newline character
    rows = raw_data.strip().split("\n")
    for row in rows:
        # Skip empty rows that might result from trailing newline
        if not row:
            continue

        # Split each row into fields using the new field separator
        fields = row.split(field_separator)

        # The AppleScript is assumed to output 9 fields:
        # id, name, artist, album, genre, dateAdded, trackStatus, standardYear, empty_field
        # The Python parsing logic expects up to 9 fields, mapping index 7 to old_year
        # and index 8 to new_year.
        # We need at least 7 fields for the standard properties.
        if len(fields) >= 7:
            track = {
                "id": fields[0].strip(),
                "name": fields[1].strip(),
                "artist": fields[2].strip(),
                "album": fields[3].strip(),
                "genre": fields[4].strip(),
                "dateAdded": fields[5].strip(),
                "trackStatus": fields[6].strip(),
            }
            # Adding old_year and new_year fields based on the structure
            # provided in the original code, assuming they are read from fields 7 and 8.
            # The AppleScript now outputs standardYear at index 7 and an empty string at index 8.
            if len(fields) > 7:  # Check if field at index 7 exists (the 8th field)
                # Assuming the 8th field from AppleScript is the standard year
                # and it should potentially be treated as old_year initially
                track["old_year"] = fields[7].strip()
            else:
                track["old_year"] = ""  # Default to empty if field is missing

            if len(fields) > 8:  # Check if field at index 8 exists (the 9th field)
                # Assuming the 9th field from AppleScript is intended for new_year
                track["new_year"] = fields[8].strip()
            else:
                track["new_year"] = ""  # Initialize as an empty string if not present

            tracks.append(track)
        else:
            # Log malformed row for debugging
            # Use module-level error_logger if available, otherwise print
            if 'error_logger' in globals() and error_logger:
                error_logger.warning("Malformed track data row skipped: %s", row)
            else:
                print(f"WARNING: Malformed track data row skipped: {row}", file=sys.stderr)

    return tracks


# Logic moved from old group_tracks_by_artist
def group_tracks_by_artist(tracks: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Group tracks by artist name into a dictionary for efficient processing.
    Uses standard Python collections, no injected services needed.
    """
    # Use defaultdict for efficient grouping without checking for key existence
    artists = defaultdict(list)
    for track in tracks:
        # Ensure artist key exists, default to "Unknown" if missing
        artist = track.get("artist", "Unknown")
        artists[artist].append(track)
    # Return defaultdict directly without converting to dict for better performance
    return artists


# Logic moved from old determine_dominant_genre_for_artist
def determine_dominant_genre_for_artist(artist_tracks: List[Dict[str, str]]) -> str:
    """
    Determine the dominant genre for an artist based on the earliest genre of their track.
    Does not require injected services.
    """
    if not artist_tracks:
        return "Unknown"
    try:
        # Find the earliest track for each album
        album_earliest: Dict[str, Dict[str, str]] = {}
        for track in artist_tracks:
            # Ensure album and dateAdded keys exist
            album = track.get("album", "Unknown")
            date_added_str = track.get("dateAdded", "1900-01-01 00:00:00")  # Provide a default date string
            try:
                track_date = datetime.strptime(date_added_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # Handle cases with invalid date format by using a very old date
                track_date = datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                # Optionally log a warning here, but requires a logger instance

            if album not in album_earliest:
                album_earliest[album] = track
            else:
                existing_date_str = album_earliest[album].get("dateAdded", "1900-01-01 00:00:00")
                try:
                    existing_date = datetime.strptime(existing_date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    existing_date = datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

                if track_date < existing_date:
                    album_earliest[album] = track

        # From these tracks, find the earliest album (by the date of addition of its earliest track)
        # Ensure album_earliest is not empty before finding min
        if not album_earliest:
            return "Unknown"

        earliest_album_track_in_album_earliest = min(
            album_earliest.values(),
            key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"),
        )

        # In the earliest album, find the earliest track (should be the same as earliest_album_track_in_album_earliest)
        # This step is redundant if we already found the earliest track across all albums via album_earliest
        # Let's simplify and just take the genre from the earliest track found in the album_earliest step
        dominant_genre = earliest_album_track_in_album_earliest.get("genre")

        return dominant_genre or "Unknown"  # Return genre or "Unknown" if genre is None/empty
    except Exception as e:  # Catch broader exceptions during processing
        # Use module-level error_logger if available, otherwise print
        if 'error_logger' in globals() and error_logger:
            error_logger.error("Error in determine_dominant_genre_for_artist: %s", e, exc_info=True)
        else:
            print(f"Error in determine_dominant_genre_for_artist: {e}", file=sys.stderr)
        return "Unknown"


# Logic moved from old remove_parentheses_with_keywords
def remove_parentheses_with_keywords(name: str, keywords: List[str]) -> str:
    """
    Remove parentheses and their content if they contain any of the specified keywords.
    Handles nested parentheses correctly. Does not require injected services.
    """
    try:
        # Use module-level console_logger if available, otherwise print
        if 'console_logger' in globals() and console_logger:
            console_logger.debug("remove_parentheses_with_keywords called with name='%s' and keywords=%s", name, keywords)
        else:
            print(f"DEBUG: remove_parentheses_with_keywords called with name='{name}' and keywords={keywords}", file=sys.stderr)

        if not name or not keywords:
            return name

        # Convert keywords to lowercase for case-insensitive comparison
        keyword_set = set(k.lower() for k in keywords)

        # Iteratively clean brackets until no more matches are found
        prev_name = ""
        current_name = name

        while prev_name != current_name:
            prev_name = current_name

            # Find all bracket pairs in the current version of the string
            stack = []
            pairs = []

            for i, char in enumerate(current_name):
                if char in "([":
                    stack.append((char, i))
                elif char in ")]":
                    if stack:
                        start_char, start_idx = stack.pop()
                        if (start_char == "(" and char == ")") or (start_char == "[" and char == "]"):
                            pairs.append((start_idx, i))
                        # Handle mismatched brackets by clearing stack if top doesn't match
                        elif (start_char == "(" and char == "]") or (start_char == "[" and char == ")"):
                            stack.clear()  # Clear stack on mismatch to avoid incorrect pairing

            # Sort pairs by start index
            pairs.sort()

            # Find which pairs to remove based on keywords
            to_remove = set()
            for start, end in pairs:
                content = current_name[start + 1 : end]  # noqa: ignore=E203
                if any(keyword.lower() in content.lower() for keyword in keyword_set):
                    to_remove.add((start, end))

            # If nothing found to remove, we're done
            if not to_remove:
                break

            # Remove brackets (from right to left to maintain indices)
            # Convert set to list and sort in reverse order of end index
            sorted_to_remove = sorted(list(to_remove), key=lambda item: item[1], reverse=True)
            for start, end in sorted_to_remove:
                # Ensure start and end are valid indices for current_name
                if 0 <= start < end < len(current_name):
                    current_name = current_name[:start] + current_name[end + 1 :]  # noqa: ignore=E203
                else:
                    # Should not happen with correct pair finding, but safety check
                    if 'error_logger' in globals() and error_logger:
                        error_logger.warning(f"Invalid indices ({start}, {end}) found during bracket removal for '{prev_name}'")
                    else:
                        print(f"WARNING: Invalid indices ({start}, {end}) found during bracket removal for '{prev_name}'", file=sys.stderr)

        # Clean up multiple spaces
        result = re.sub(r"\s+", " ", current_name).strip()

        # Use module-level console_logger if available, otherwise print
        if 'console_logger' in globals() and console_logger:
            console_logger.debug("Cleaned result: '%s'", result)
        else:
            print(f"DEBUG: Cleaned result: '{result}'", file=sys.stderr)

        return result

    except Exception as e:  # Catch broader exceptions during processing
        # Use module-level error_logger if available, otherwise print
        if 'error_logger' in globals() and error_logger:
            error_logger.error("Error in remove_parentheses_with_keywords: %s", e, exc_info=True)
        else:
            print(f"Error in remove_parentheses_with_keywords: {e}", file=sys.stderr)
        return name  # Return original name on error


# Logic moved from old clean_names
def clean_names(artist: str, track_name: str, album_name: str, config: Dict[str, Any]) -> Tuple[str, str]:
    """
    Cleans the track name and album name based on the configuration settings.
    Requires config, does not require other injected services directly.
    """
    # Use module-level console_logger if available, otherwise print
    if 'console_logger' in globals() and console_logger:
        console_logger.info("clean_names called with: artist='%s', track_name='%s', album_name='%s'", artist, track_name, album_name)
    else:
        print(f"INFO: clean_names called with: artist='{artist}', track_name='{track_name}', album_name='{album_name}'", file=sys.stderr)

    # Use config passed as argument
    exceptions = config.get("exceptions", {}).get("track_cleaning", [])
    # Check if the current artist/album pair is in the exceptions list
    is_exception = any(exc.get("artist", "").lower() == artist.lower() and exc.get("album", "").lower() == album_name.lower() for exc in exceptions)

    if is_exception:
        if 'console_logger' in globals() and console_logger:
            console_logger.info("No cleaning applied due to exceptions for artist '%s', album '%s'.", artist, album_name)
        else:
            print(f"INFO: No cleaning applied due to exceptions for artist '{artist}', album '{album_name}'.", file=sys.stderr)
        return track_name.strip(), album_name.strip()  # Return original names, stripped

    # Get cleaning config
    cleaning_config = config.get("cleaning", {})
    remaster_keywords = cleaning_config.get("remaster_keywords", ["remaster", "remastered"])
    album_suffixes = set(cleaning_config.get("album_suffixes_to_remove", []))

    # Helper function for cleaning strings using remove_parentheses_with_keywords
    def clean_string(val: str, keywords: List[str]) -> str:
        # Use the utility function defined above
        new_val = remove_parentheses_with_keywords(val, keywords)
        # Clean up multiple spaces and strip whitespace
        new_val = re.sub(r"\s+", " ", new_val).strip()
        return new_val if new_val else ""  # Return empty string if result is empty after cleaning

    original_track = track_name
    original_album = album_name

    # Apply cleaning to track name and album name
    cleaned_track = clean_string(track_name, remaster_keywords)
    cleaned_album = clean_string(album_name, remaster_keywords)

    # Remove specified album suffixes
    for suffix in album_suffixes:
        if cleaned_album.endswith(suffix):
            cleaned_album = cleaned_album[: -len(suffix)].strip()
            # Log the removal
            if 'console_logger' in globals() and console_logger:
                console_logger.info("Removed suffix '%s' from album. New album name: '%s'", suffix, cleaned_album)
            else:
                print(f"INFO: Removed suffix '{suffix}' from album. New album name: '{cleaned_album}'", file=sys.stderr)

    # Log the cleaning results
    if 'console_logger' in globals() and console_logger:
        console_logger.info("Original track name: '%s' -> '%s'", original_track, cleaned_track)
        console_logger.info("Original album name: '%s' -> '%s'", original_album, cleaned_album)
    else:
        print(f"INFO: Original track name: '{original_track}' -> '{cleaned_track}'", file=sys.stderr)
        print(f"INFO: Original album name: '{original_album}' -> '{cleaned_album}'", file=sys.stderr)

    return cleaned_track, cleaned_album


# Logic moved from old is_music_app_running
def is_music_app_running() -> bool:
    """
    Checks if the Music.app is currently running.
    Uses subprocess, no injected services needed.
    """
    try:
        # Use module-level error_logger if available, otherwise print
        script = 'tell application "System Events" to (name of processes) contains "Music"'
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
        return result.stdout.strip().lower() == "true"
    except (subprocess.SubprocessError, OSError) as e:
        if 'error_logger' in globals() and error_logger:
            error_logger.error("Unable to check Music.app status: %s", e, exc_info=True)
        else:
            print(f"ERROR: Unable to check Music.app status: {e}", file=sys.stderr)
        return False  # Assume not running on error


# --- New MusicUpdater Class (Orchestrator) ---
# This class will contain the core logic previously in top-level async functions
class MusicUpdater:
    """
    Orchestrates the music library update process, using injected dependencies.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        analytics: Analytics,
        ap_client: 'AppleScriptClient',  # Use forward reference as AppleScriptClient is imported by DependencyContainer
        cache_service: 'CacheService',  # Use forward reference
        external_api_service: 'ExternalApiService',  # Use forward reference
        pending_verification_service: 'PendingVerificationService',  # Use forward reference
        # Add other services here if needed in the future
    ):
        """
        Initializes the MusicUpdater with its dependencies.
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.analytics = analytics  # Store Analytics instance to use its decorator
        self.ap_client = ap_client
        self.cache_service = cache_service
        self.external_api_service = external_api_service
        self.pending_verification_service = pending_verification_service

        # Store utility functions or move their logic into methods if appropriate
        # For now, we will assume clean_names and determine_dominant_genre_for_artist
        # are either imported or their logic is moved into this class.
        # If they are moved into this class, they might need access to self.config etc.
        # If they are imported from a utils module, they don't need self.

    # --- Methods containing logic moved from old top-level async functions ---

    # Logic moved from old fetch_tracks_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Fetch Tracks Async")
    async def fetch_tracks_async(self, artist: Optional[str] = None, force_refresh: bool = False) -> List[Dict[str, str]]:
        """
        Fetches tracks asynchronously from Music.app via AppleScript and manages caching.
        Uses injected AppleScriptClient and CacheService.
        """
        try:
            cache_key = artist if artist else "ALL"
            self.console_logger.info("Fetching tracks with cache_key='%s', force_refresh=%s", cache_key, force_refresh)

            # is_music_app_running is a simple check, can remain a utility or be moved
            # For now, let's assume it's a utility function available in the module scope.
            if not is_music_app_running():
                self.console_logger.error("Music app is not running! Please start Music.app before running this script.")
                return []

            if not force_refresh:
                # Use injected cache_service
                cached_tracks = await self.cache_service.get_async(cache_key)
                if cached_tracks:
                    self.console_logger.info("Using cached data for %s, found %d tracks", cache_key, len(cached_tracks))
                    return cached_tracks
                else:
                    self.console_logger.info("No cache found for %s, fetching from Music.app", cache_key)
            elif force_refresh:
                self.console_logger.info("Force refresh requested, ignoring cache for %s", cache_key)
                # Use injected cache_service
                await self.cache_service.invalidate(cache_key)

            script_name = "fetch_tracks.applescript"
            script_args = [artist] if artist else []

            # Use timeout from config
            timeout = self.config.get("applescript_timeout_seconds", 900)

            # Use injected ap_client
            raw_data = await self.ap_client.run_script(script_name, script_args, timeout=timeout)

            if raw_data:
                lines_count = raw_data.count('\n') + 1
                self.console_logger.info("AppleScript returned data: %d bytes, approximately %d lines", len(raw_data), lines_count)
            else:
                self.error_logger.error("Empty response from AppleScript %s. Possible script error.", script_name)
                return []

            # parse_tracks is a simple parsing function, can remain a utility
            tracks = parse_tracks(raw_data)
            self.console_logger.info("Successfully parsed %d tracks from Music.app", len(tracks))

            if tracks:
                # Use injected cache_service
                await self.cache_service.set_async(cache_key, tracks)
                self.console_logger.info("Cached %d tracks with key '%s'", len(tracks), cache_key)

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
        new_track_name: Optional[str] = None,
        new_album_name: Optional[str] = None,
        new_genre: Optional[str] = None,
        new_year: Optional[str] = None,
    ) -> bool:
        """
        Updates the track properties asynchronously via AppleScript using injected AppleScriptClient.
        """
        try:
            if not track_id:
                self.error_logger.error("No track_id provided.")
                return False

            success = True

            # Use injected ap_client
            if new_track_name:
                res = await self.ap_client.run_script("update_property.applescript", [track_id, "name", new_track_name])
                if not res or "Success" not in res:
                    self.error_logger.error("Failed to update track name for %s", track_id)
                    success = False

            # Use injected ap_client
            if new_album_name:
                res = await self.ap_client.run_script("update_property.applescript", [track_id, "album", new_album_name])
                if not res or "Success" not in res:
                    self.error_logger.error("Failed to update album name for %s", track_id)
                    success = False

            # Use injected ap_client
            if new_genre:
                max_retries = self.config.get("max_retries", 3)
                delay = self.config.get("retry_delay_seconds", 2)
                genre_updated = False
                for attempt in range(1, max_retries + 1):
                    res = await self.ap_client.run_script("update_property.applescript", [track_id, "genre", new_genre])
                    if res and "Success" in res:
                        self.console_logger.info("Updated genre for %s to %s (attempt %s/%s)", track_id, new_genre, attempt, max_retries)
                        genre_updated = True
                        break
                    else:
                        self.console_logger.warning("Attempt %s/%s failed. Retrying in %ss...", attempt, max_retries, delay)
                        await asyncio.sleep(delay)
                if not genre_updated:
                    self.error_logger.error("Failed to update genre for track %s", track_id)
                    success = False

            # Use injected ap_client
            if new_year:
                res = await self.ap_client.run_script("update_property.applescript", [track_id, "year", new_year])
                if not res or "Success" not in res:
                    self.error_logger.error("Failed to update year for %s", track_id)
                    success = False
                else:
                    self.console_logger.info("Updated year for %s to %s", track_id, new_year)

            return success
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error("Error in update_track_async for track %s: %s", track_id, e, exc_info=True)
            return False

    # Logic moved from old update_album_tracks_bulk_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Album Tracks Bulk Async")
    async def update_album_tracks_bulk_async(self, track_ids: List[str], year: str) -> bool:
        """
        Updates the year property for multiple tracks in bulk using a batched approach.
        Uses injected AppleScriptClient.
        """
        try:
            if not track_ids or not year:
                self.console_logger.warning("No track IDs or year provided for bulk update.")
                return False

            # Validation of track IDs and year before processing
            filtered_track_ids = []
            for track_id in track_ids:
                try:
                    # Ensure track_id is a valid number and not accidentally the year value
                    if track_id and track_id.isdigit() and track_id != year:
                        filtered_track_ids.append(track_id)
                    else:
                        self.console_logger.warning("Skipping invalid track ID: '%s'", track_id)
                except (ValueError, TypeError):
                    self.console_logger.warning("Skipping non-numeric track ID: '%s'", track_id)

            if not filtered_track_ids:
                self.console_logger.warning("No valid track IDs left after filtering")
                return False

            self.console_logger.info(
                "Updating year to %s for %d tracks (filtered from %d initial tracks)", year, len(filtered_track_ids), len(track_ids)
            )

            successful_updates = 0
            failed_updates = 0
            # Use batch_size from config, default to 20
            batch_size = min(20, self.config.get("batch_size", 20))

            for i in range(0, len(filtered_track_ids), batch_size):
                batch = filtered_track_ids[i : i + batch_size]  # noqa: ignore=E203
                tasks = []

                for track_id in batch:
                    self.console_logger.info("Adding update task for track ID %s to year %s", track_id, year)
                    # Use injected ap_client
                    tasks.append(self.ap_client.run_script("update_property.applescript", [track_id, "year", year]))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for idx, result in enumerate(results):
                    track_id = batch[idx] if idx < len(batch) else "unknown"
                    if isinstance(result, Exception):
                        self.error_logger.error("Exception updating year for track %s: %s", track_id, result)
                        failed_updates += 1
                    elif not result:
                        self.error_logger.error("Empty result updating year for track %s", track_id)
                        failed_updates += 1
                    elif "Success" not in result:
                        if "not found" in result:
                            self.console_logger.warning("Track %s not found, removing from processing", track_id)
                        else:
                            self.error_logger.error("Failed to update year for track %s: %s", track_id, result)
                        failed_updates += 1
                    else:
                        successful_updates += 1
                        self.console_logger.info("Successfully updated year for track %s to %s", track_id, year)

                # Add a small delay between batches if there are more batches to process
                if i + batch_size < len(filtered_track_ids):
                    await asyncio.sleep(0.5)

            self.console_logger.info("Year update results: %s successful, %s failed", successful_updates, failed_updates)
            return successful_updates > 0
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error("Error in update_album_tracks_bulk_async: %s", e, exc_info=True)
            return False

    # Logic moved from old process_album_years
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Process Album Years")
    async def process_album_years(
        self, tracks: List[Dict[str, str]], last_run_time: datetime = datetime.min, force: bool = False
    ) -> Optional[tuple[list[Any], list[Any]]]:
        """
        Manages album year updates for all tracks using external music databases.
        Uses injected ExternalAPIService, CacheService, and PendingVerificationService.
        """
        # Use config from self.config
        if not self.config.get("year_retrieval", {}).get("enabled", False):
            self.console_logger.info("Album year updates are disabled in config. Skipping.")
            return [], []

        if not tracks:
            self.console_logger.info("No tracks provided for year updates.")
            return [], []

        self.console_logger.info("Starting album year updates (force=%s)", force)

        # Skip prerelease tracks (logic remains the same)
        filtered_tracks = []
        for track in tracks:
            track_status = track.get("trackStatus", "").lower()
            if track_status == "prerelease":
                self.console_logger.debug(
                    "Skipping prerelease track: %s - %s - %s", track.get('name', ''), track.get('artist', ''), track.get('album', '')
                )
                continue
            elif track_status == "no longer available":
                self.console_logger.debug(
                    "Skipping unavailable track: %s - %s - %s", track.get('name', ''), track.get('artist', ''), track.get('album', '')
                )
                continue
            else:
                filtered_tracks.append(track)

        self.console_logger.info("Using %d/%d tracks (skipped prerelease/unavailable)", len(filtered_tracks), len(tracks))

        # Filter by last_run_time for incremental updates if not forced (logic remains the same)
        if last_run_time and last_run_time != datetime.min and not force:
            self.console_logger.info("Filtering tracks added after %s for album year updates", last_run_time)
            filtered_tracks = [
                track
                for track in filtered_tracks
                if datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") > last_run_time
            ]
            self.console_logger.info("Found %d tracks added since last run for album year updates", len(filtered_tracks))

        # If no tracks remain after filtering, return early
        if not filtered_tracks:
            self.console_logger.info("No tracks to process for album year updates after filtering.")
            return [], []

        try:
            # Use injected external_api_service
            await self.external_api_service.initialize()
            # Moved core logic to a helper method
            updated_y, changes_y = await self._update_album_years_logic(filtered_tracks, force=force)

            # Use config from self.config
            csv_output_file_path = get_full_log_path(self.config, "csv_output_file", "csv/track_list.csv")
            changes_report_file_path = get_full_log_path(self.config, "changes_report_file", "csv/changes_report.csv")

            if updated_y:
                # Use injected cache_service, console_logger, error_logger
                await sync_track_list_with_current(
                    updated_y,
                    csv_output_file_path,
                    self.cache_service,
                    self.console_logger,
                    self.error_logger,
                    partial_sync=True,
                )
                # Use injected console_logger, error_logger
                save_changes_report(
                    changes_y,
                    changes_report_file_path,
                    self.console_logger,
                    self.error_logger,
                    force_mode=force,
                )
                self.console_logger.info("Updated %d tracks with album years.", len(updated_y))
            else:
                self.console_logger.info("No tracks needed album year updates.")

            return updated_y, changes_y
        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error("Error in process_album_years: %s", e, exc_info=True)
            return [], []
        finally:
            # Use injected external_api_service
            await self.external_api_service.close()

    # Helper method containing the core album year update logic
    # Moved from the old process_album_years function to make it cleaner
    async def _update_album_years_logic(self, tracks: List[Dict[str, str]], force: bool = False) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """
        Core logic for updating album years by querying external APIs.
        Separated from process_album_years for clarity.
        Uses injected services: external_api_service, cache_service, pending_verification_service, ap_client.
        """
        # Setup logging for year updates (logic remains the same, but uses self.loggers)
        year_changes_log_file = get_full_log_path(self.config, "year_changes_log_file", "main/year_changes.log")
        os.makedirs(os.path.dirname(year_changes_log_file), exist_ok=True)
        year_logger = logging.getLogger("year_updates")
        if not year_logger.handlers:  # Configure only once
            fh = logging.FileHandler(year_changes_log_file)
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            year_logger.addHandler(fh)

        self.console_logger.info("Starting album year update logic for %d tracks", len(tracks))
        # Group tracks by album to reduce API calls (logic remains the same)
        albums = {}
        for track in tracks:
            artist = track.get("artist", "Unknown")
            album = track.get("album", "Unknown")
            key = f"{artist}|{album}"
            if key not in albums:
                albums[key] = {"artist": artist, "album": album, "tracks": []}
            albums[key]["tracks"].append(track)

        self.console_logger.info("Processing %d unique albums", len(albums))
        year_logger.info("Starting year update for %d albums", len(albums))

        updated_tracks = []
        changes_log = []

        # Define the album processing function (now an inner async function or separate method)
        # Let's keep it as an inner async function for direct access to updated_tracks/changes_log
        async def process_album(album_data):
            nonlocal updated_tracks, changes_log
            artist = album_data["artist"]
            album = album_data["album"]
            album_tracks = album_data["tracks"]

            if not album_tracks:
                return False  # Indicate no API call needed

            # Extract the current library year from the first track
            current_library_year = album_tracks[0].get("new_year", "") if album_tracks else ""  # Use new_year as current state

            # Check if this album is pending verification using injected service
            should_verify = False
            if self.pending_verification_service:
                should_verify = self.pending_verification_service.is_verification_needed(artist, album)
                if should_verify:
                    self.console_logger.info("Album '%s - %s' is due for verification", artist, album)

            # Try to get cached year first, unless forcing or verification is due
            year = None
            is_definitive = True
            api_call_made = False  # Flag to track if an API call was made for this album

            if force or should_verify:
                # Bypass cache when forcing or verification is due
                year_logger.info("Fetching year for '%s - %s' from external API (force=%s, verify=%s)", artist, album, force, should_verify)
                # Use injected external_api_service and pending_verification_service
                year, is_definitive = await self.external_api_service.get_album_year(
                    artist, album, current_library_year  # pending_verification_service is now an instance attribute
                )
                api_call_made = True

                if year and is_definitive:
                    # Use injected cache_service
                    await self.cache_service.store_album_year_in_cache(artist, album, year)
                    year_logger.info("Stored year %s for '%s - %s' in cache", year, artist, album)

                    # Remove from pending if we got a definitive result using injected service
                    if self.pending_verification_service:
                        self.pending_verification_service.remove_from_pending(artist, album)
                # If not definitive, get_album_year already marked it for pending verification

            else:
                # Try to get cached year first using injected cache_service
                year = await self.cache_service.get_album_year_from_cache(artist, album)

                if not year:
                    year_logger.info("No cached year found for '%s - %s', fetching from API", artist, album)
                    # Use injected external_api_service and pending_verification_service
                    year, is_definitive = await self.external_api_service.get_album_year(
                        artist, album, current_library_year  # pending_verification_service is now an instance attribute
                    )
                    api_call_made = True

                    if year and is_definitive:
                        # Use injected cache_service
                        await self.cache_service.store_album_year_in_cache(artist, album, year)
                        year_logger.info("Stored year %s for '%s - %s' in cache", year, artist, album)

                    # If not definitive, get_album_year already marked it for pending verification
                else:
                    year_logger.info("Using cached year %s for '%s - %s'", year, artist, album)
                    api_call_made = False  # No API call was made in this path

            if not year:
                return api_call_made  # Indicate if API call was made, but no year found

            # Skip update if year is the same as current library year
            if year == current_library_year:
                year_logger.info("Year %s for '%s - %s' matches current library year, no update needed", year, artist, album)
                return api_call_made  # Indicate if API call was made

            # Filter tracks to update based on status and current year
            tracks_to_update = []
            for track in album_tracks:
                current_year_on_track = track.get("new_year", "").strip()
                track_status = track.get("trackStatus", "").lower()

                # Skip tracks with statuses that can't be modified
                if track_status in ("prerelease", "no longer available"):
                    continue

                # Update if forced OR if current year is empty OR if current year is different from determined year
                if force or not current_year_on_track or current_year_on_track != year:
                    tracks_to_update.append(track)

            if not tracks_to_update:
                year_logger.info("No tracks need update for '%s - %s' (year: %s, force=%s)", artist, album, year, force)
                return api_call_made  # Indicate if API call was made

            year_logger.info(
                "Updating %d of %d tracks for '%s - %s' to year %s (force=%s)",
                len(tracks_to_update),
                len(album_tracks),
                artist,
                album,
                year,
                force,
            )

            track_ids_to_update = [t.get("id", "") for t in tracks_to_update]
            track_ids_to_update = [tid for tid in track_ids_to_update if tid]

            # Use injected ap_client
            success = await self.update_album_tracks_bulk_async(track_ids_to_update, year)
            if success:
                for track in tracks_to_update:
                    track_id = track.get("id", "")
                    if track_id:
                        # Update the track dictionary in memory
                        track["old_year"] = track.get("new_year", "")  # Store previous 'new_year' as 'old_year'
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
                                "old_year": track.get("old_year", ""),  # Log the old year
                                "new_year": year,
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                year_logger.info("Successfully updated %d tracks for '%s - %s' to year %s", len(track_ids_to_update), artist, album, year)
            else:
                year_logger.error("Failed to update year for '%s - %s'", artist, album)

            return api_call_made  # Indicate if API call was made

        # Fetch optimal batch parameters from config using self.config
        batch_size = self.config.get("year_retrieval", {}).get("batch_size", 20)
        delay_between_batches = self.config.get("year_retrieval", {}).get("delay_between_batches", 30)
        concurrent_limit = self.config.get("year_retrieval", {}).get("concurrent_api_calls", 5)
        adaptive_delay = self.config.get("year_retrieval", {}).get("adaptive_delay", False)

        album_items = list(albums.items())

        # Create semaphore for concurrency control (specific to this method's batches)
        semaphore = asyncio.Semaphore(concurrent_limit)

        # Process albums in batches
        for i in range(0, len(album_items), batch_size):
            batch = album_items[i : i + batch_size]  # noqa: ignore=E203
            batch_tasks = []

            # Create tasks for each album in the batch
            for _, album_data in batch:
                # Use semaphore to limit concurrency
                async def process_with_semaphore(data):
                    async with semaphore:
                        return await process_album(data)  # Call the inner processing function

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

                    self.console_logger.info("Adaptive delay: %ds based on %d/%d API calls in batch", adjusted_delay, api_calls_count, len(batch))
                    await asyncio.sleep(adjusted_delay)
                elif api_calls_count > 0:  # If adaptive delay is off but API calls were made
                    self.console_logger.info("API calls were made. Waiting %ds before next batch", delay_between_batches)
                    await asyncio.sleep(delay_between_batches)
                else:  # No API calls made in this batch
                    self.console_logger.info("No API calls made in this batch. Proceeding to next batch immediately.")

        self.console_logger.info("Album year update logic complete. Updated %d tracks.", len(updated_tracks))
        year_logger.info("Album year update logic complete. Updated %d tracks.", len(updated_tracks))

        # Return the list of updated tracks and the changes log
        # Note: updated_tracks list is populated inside the inner process_album function
        return updated_tracks, changes_log

    # Logic moved from old can_run_incremental
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Can Run Incremental")
    async def can_run_incremental(self, force_run: bool = False) -> bool:
        """
        Checks if the incremental interval has passed since the last run using injected CacheService.
        """
        if force_run:
            return True

        # Use config from self.config
        last_file_path = get_full_log_path(self.config, "last_incremental_run_file", "last_incremental_run.log")
        interval = self.config.get("incremental_interval_minutes", 60)

        # If the file does not exist, allow it to run
        if not os.path.exists(last_file_path):
            self.console_logger.info("Last incremental run file not found. Proceeding.")
            return True

        # Trying to read a file with repeated attempts
        max_retries = 3
        retry_delay = 0.5  # seconds

        for attempt in range(max_retries):
            try:
                # Trying to read a file with rollback on failure
                with open(last_file_path, "r", encoding="utf-8") as f:
                    last_run_str = f.read().strip()

                try:
                    last_run_time = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    self.error_logger.error("Invalid date in %s. Proceeding with execution.", last_file_path)
                    return True

                next_run_time = last_run_time + timedelta(minutes=interval)
                now = datetime.now()

                if now >= next_run_time:
                    return True
                else:
                    diff = next_run_time - now
                    minutes_remaining = diff.total_seconds() // 60  # Use total_seconds for more accurate difference
                    self.console_logger.info("Last run: %s. Next run in %d mins.", last_run_time.strftime('%Y-%m-%d %H:%M'), minutes_remaining)
                    return False

            except (OSError, IOError) as e:
                if attempt < max_retries - 1:
                    # If this is not the last attempt, we wait and try again
                    self.console_logger.warning(
                        f"Error reading last run file (attempt {attempt+1}/{max_retries}): {e}. Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential increase in latency
                else:
                    # If all attempts fail, log and allow launch
                    self.error_logger.error(f"Error accessing last run file after {max_retries} attempts: {e}")
                    return True

        # Should not be reached, but for safety:
        return True

    # Logic moved from old update_last_incremental_run
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Last Incremental Run")
    async def update_last_incremental_run(self) -> None:
        """
        Update the timestamp of the last incremental run in a file.
        Uses config from self.config.
        """
        # Use config from self.config
        last_file_path = get_full_log_path(self.config, "last_incremental_run_file", "last_incremental_run.log")
        try:
            # Ensure directory exists before writing
            os.makedirs(os.path.dirname(last_file_path), exist_ok=True)
            with open(last_file_path, "w", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.console_logger.info("Updated last incremental run timestamp.")
        except IOError as e:
            self.error_logger.error(f"Failed to write last incremental run timestamp to {last_file_path}: {e}")

    # Logic moved from old verify_and_clean_track_database
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Database Verification")
    async def verify_and_clean_track_database(self, force: bool = False) -> int:
        """
        Verifies tracks in the CSV database against Music.app and removes entries for tracks
        that no longer exist. Respects the 'development.test_artists' config setting.
        Uses injected AppleScriptClient and config from self.config.
        """
        removed_count = 0
        try:
            self.console_logger.info("Starting database verification process...")
            # Use config from self.config
            csv_path = get_full_log_path(self.config, "csv_output_file", "csv/track_list.csv")

            if not os.path.exists(csv_path):
                self.console_logger.info("Track database not found at %s, nothing to verify.", csv_path)
                return 0

            # Load the entire track list from CSV into a dictionary {track_id: track_dict}
            # load_track_list is a utility function, doesn't need self.
            csv_tracks_map = load_track_list(csv_path)

            if not csv_tracks_map:
                self.console_logger.info("No tracks loaded from database to verify.")
                return 0

            # --- Check if verification is needed based on interval (unless forced) ---
            # Use config from self.config
            last_verify_file = get_full_log_path(self.config, "last_db_verify_log", "main/last_db_verify.log")
            auto_verify_days = self.config.get("database_verification", {}).get("auto_verify_days", 7)

            if not force and auto_verify_days > 0 and os.path.exists(last_verify_file):
                try:
                    with open(last_verify_file, "r", encoding="utf-8") as f:
                        last_verify_str = f.read().strip()
                    last_date = datetime.strptime(last_verify_str, "%Y-%m-%d").date()
                    days_since_last = (datetime.now().date() - last_date).days
                    if days_since_last < auto_verify_days:
                        self.console_logger.info(
                            f"Database verified {days_since_last} days ago ({last_date}). "
                            f"Skipping verification (interval: {auto_verify_days} days). Use --force to override."
                        )
                        return 0  # Verification not needed yet
                except (FileNotFoundError, PermissionError, IOError, ValueError) as e:
                    self.error_logger.warning(
                        f"Could not read or parse last verification date ({last_verify_file}): {e}. Proceeding with verification."
                    )
                    pass  # Proceed if file is missing or corrupt

            # --- Apply test_artists filter if configured ---
            tracks_to_verify_map = csv_tracks_map  # Start with all tracks by default
            # Use config from self.config
            test_artists_config = self.config.get("development", {}).get("test_artists", [])

            if test_artists_config:
                self.console_logger.info("Limiting database verification to specified test_artists: %s", test_artists_config)
                # Filter the map to include only tracks from test artists
                tracks_to_verify_map = {track_id: track for track_id, track in csv_tracks_map.items() if track.get("artist") in test_artists_config}
                self.console_logger.info("After filtering by test_artists, %d tracks will be verified.", len(tracks_to_verify_map))

            # --- Check if any tracks remain after filtering ---
            if not tracks_to_verify_map:
                self.console_logger.info("No tracks to verify (either DB is empty or after filtering by test_artists).")
                # Update the verification timestamp even if nothing was checked
                try:
                    os.makedirs(os.path.dirname(last_verify_file), exist_ok=True)
                    with open(last_verify_file, "w", encoding="utf-8") as f:
                        f.write(datetime.now().strftime("%Y-%m-%d"))
                except IOError as e:
                    self.error_logger.error(f"Failed to write last verification timestamp: {e}")
                return 0  # Nothing removed

            # --- Log correct count and get IDs ---
            tracks_to_verify_count = len(tracks_to_verify_map)
            self.console_logger.info("Verifying %d tracks against Music.app...", tracks_to_verify_count)
            track_ids_to_check = list(tracks_to_verify_map.keys())  # Get IDs of tracks to check

            # --- Async verification helper ---
            # This helper needs access to the AppleScript client
            async def verify_track_exists(track_id: str) -> bool:
                """Checks if a track ID exists in the Music library using injected AppleScriptClient."""
                if not track_id or not track_id.isdigit():
                    return False  # Basic validation
                script = f'''
                tell application "Music"
                    try
                        # Efficiently check existence by trying to get a property
                        get id of track id {track_id} of library playlist 1
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
                '''
                try:
                    # Use injected ap_client
                    result = await self.ap_client.run_script_code(script)
                    # Return True only if AppleScript explicitly confirmed existence
                    return result == "exists"
                except Exception as e:  # Catch potential exceptions during script execution
                    self.error_logger.error(f"Exception during AppleScript execution for track {track_id}: {e}")
                    return True  # Assume exists on error to prevent accidental deletion

            # --- Batch Processing ---
            # Use config from self.config
            batch_size = self.config.get("database_verification", {}).get("batch_size", 10)
            invalid_track_ids: List[str] = []
            self.console_logger.info("Checking tracks in batches of %d...", batch_size)

            for i in range(0, len(track_ids_to_check), batch_size):
                batch_ids = track_ids_to_check[i : i + batch_size]  # noqa: ignore=E203
                # Create tasks for the current batch
                tasks = [verify_track_exists(track_id) for track_id in batch_ids]
                # Run tasks concurrently and get results (True if exists, False otherwise/error)
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results for the batch
                for idx, result in enumerate(results):
                    track_id = batch_ids[idx]
                    # If an exception occurred or verify_track_exists returned False (doesn't exist)
                    if isinstance(result, Exception) or result is False:
                        if isinstance(result, Exception):
                            self.error_logger.warning(f"Exception during verification task for track ID {track_id}: {result}")
                        # Add track ID to the list of invalid ones
                        invalid_track_ids.append(track_id)

                self.console_logger.info("Verified batch %d/%d", (i // batch_size) + 1, (tracks_to_verify_count + batch_size - 1) // batch_size)
                # Optional short sleep between batches
                if i + batch_size < len(track_ids_to_check):
                    await asyncio.sleep(0.1)

            # --- Remove Invalid Tracks and Save ---
            if invalid_track_ids:
                self.console_logger.info("Found %d tracks that no longer exist in Music.app (within the verified subset).", len(invalid_track_ids))
                # Remove invalid tracks from the *original full map* loaded from CSV
                for track_id in invalid_track_ids:
                    if track_id in csv_tracks_map:
                        # Remove the track and log its details
                        removed_track_info = csv_tracks_map.pop(track_id)
                        artist_name = removed_track_info.get('artist', 'Unknown Artist')
                        track_name = removed_track_info.get('name', 'Unknown Track')
                        self.console_logger.info(f"Removing track ID {track_id}: '{artist_name} - {track_name}'")
                        removed_count += 1
                    else:
                        # This case indicates a potential logic error if an ID is invalid but not in the map
                        self.console_logger.warning(f"Attempted to remove track ID {track_id} which was not found in the loaded map. Skipping.")

                # Save the modified full track map back to the CSV
                final_list_to_save = list(csv_tracks_map.values())
                self.console_logger.info("Final CSV track count after verification: %d", len(final_list_to_save))
                # Use the dedicated save function (utility function)
                save_to_csv(final_list_to_save, csv_path, self.console_logger, self.error_logger)
                self.console_logger.info("Removed %d invalid tracks from database.", removed_count)
            else:
                self.console_logger.info("All verified tracks (%d) exist in Music.app.", tracks_to_verify_count)
                removed_count = 0

            # --- Update Last Verification Timestamp ---
            try:
                os.makedirs(os.path.dirname(last_verify_file), exist_ok=True)  # Ensure directory exists
                with open(last_verify_file, "w", encoding="utf-8") as f:
                    f.write(datetime.now().strftime("%Y-%m-%d"))
                self.console_logger.info("Updated last database verification timestamp.")
            except IOError as e:
                self.error_logger.error(f"Failed to write last verification timestamp: {e}")

            return removed_count

        except Exception as e:  # Catch broad exceptions during the verification process
            self.error_logger.error("Critical error during database verification: %s", e, exc_info=True)
            return 0  # Return 0 removed on error

    # Logic moved from old update_genres_by_artist_async
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Update Genres by Artist")
    async def update_genres_by_artist_async(
        self, tracks: List[Dict[str, str]], last_run_time: datetime
    ) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """
        Updates the genres for tracks based on the earliest genre of each artist.
        Uses injected AppleScriptClient and config from self.config.
        """
        try:
            # Use config from self.config
            csv_path = get_full_log_path(self.config, "csv_output_file", "csv/track_list.csv")
            # load_track_list is a utility function, doesn't need self.
            load_track_list(csv_path)  # This line seems redundant here, load_track_list result is not used

            if last_run_time and last_run_time != datetime.min:
                self.console_logger.info("Filtering tracks added after %s", last_run_time)
                tracks = [
                    track for track in tracks if datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") > last_run_time
                ]
                self.console_logger.info("Found %d tracks added since last run", len(tracks))

            # group_tracks_by_artist is a utility function, doesn't need self.
            grouped = group_tracks_by_artist(tracks)
            updated_tracks = []  # This list will contain tracks that were successfully updated
            changes_log = []

            # Inner async function to process a single track for genre update
            # It needs access to update_track_async method (which uses ap_client)
            async def process_track(track: Dict[str, str], dom_genre: str) -> None:
                nonlocal updated_tracks, changes_log
                old_genre = track.get("genre", "Unknown")
                track_id = track.get("id", "")
                status = track.get("trackStatus", "unknown")

                # Check if update is needed and status allows modification
                if track_id and old_genre != dom_genre and status in ("subscription", "downloaded"):
                    self.console_logger.info("Updating track %s (Old Genre: %s, New Genre: %s)", track_id, old_genre, dom_genre)
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
                                # "new_track_name": track.get("name", "Unknown"), # Removed - not a genre change
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                    else:
                        self.error_logger.error("Failed to update genre for track %s", track_id)
                # else: # Optional: Log why a track was skipped
                if track_id and old_genre == dom_genre:
                    self.console_logger.debug(f"Skipping track {track_id}: Genre already matches dominant ({dom_genre})")
                elif track_id and status not in ("subscription", "downloaded"):
                    self.console_logger.debug(f"Skipping track {track_id}: Status '{status}' does not allow modification")

            async def process_tasks_in_batches(tasks: List[asyncio.Task], batch_size: int = 1000) -> None:
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i : i + batch_size]  # noqa: ignore=E203
                    await asyncio.gather(*batch, return_exceptions=True)

            tasks = []
            for artist, artist_tracks in grouped.items():
                if not artist_tracks:
                    continue
                try:
                    # determine_dominant_genre_for_artist is a utility function, doesn't need self.
                    dom_genre = determine_dominant_genre_for_artist(artist_tracks)
                except Exception as e:  # Catch potential exceptions from utility function
                    self.error_logger.error("Error determining dominant genre for artist '%s': %s", artist, e, exc_info=True)
                    dom_genre = "Unknown"  # Default to Unknown on error

                self.console_logger.info("Artist: %s, Dominant Genre: %s (from %d tracks)", artist, dom_genre, len(artist_tracks))

                if dom_genre != "Unknown":  # Only process if a dominant genre was determined
                    for track in artist_tracks:
                        # Create a task for each track that might need a genre update
                        if track.get("genre", "Unknown") != dom_genre:
                            tasks.append(asyncio.create_task(process_track(track, dom_genre)))

            if tasks:
                self.console_logger.info("Created %d genre update tasks.", len(tasks))
                # Use batch_size from config, default to 1000 for genre updates
                batch_size_genre = self.config.get("genre_update", {}).get("batch_size", 1000)  # Assume genre_update batch_size config exists
                await process_tasks_in_batches(tasks, batch_size=batch_size_genre)
            else:
                self.console_logger.info("No genre update tasks needed.")

            # Return the list of tracks that were actually updated and the changes log
            return updated_tracks, changes_log

        except Exception as e:  # Catch potential exceptions during the process
            self.error_logger.error("Error in update_genres_by_artist_async: %s", e, exc_info=True)
            return [], []

    # Logic moved from old run_clean_artist
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Clean Artist")
    async def run_clean_artist(self, artist: str, force: bool) -> None:
        """
        Executes the cleaning process for a specific artist.
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
        # This helper needs access to the update_track_async method
        async def clean_track(track: Dict[str, str]) -> None:
            nonlocal updated_tracks_cleaning, changes_log_cleaning  # Access outer scope lists
            orig_name = track.get("name", "")
            orig_album = track.get("album", "")
            track_id = track.get("id", "")
            artist_name = track.get("artist", artist)  # Use artist from track data or command arg

            if not track_id:
                return  # Skip if no ID

            # Use the utility function clean_names, pass config from self.config
            cleaned_nm, cleaned_al = clean_names(artist_name, orig_name, orig_album, self.config)

            # Determine if changes were actually made
            new_tn = cleaned_nm if cleaned_nm != orig_name else None
            new_an = cleaned_al if cleaned_al != orig_album else None

            # If changes exist, attempt to update via AppleScript
            if new_tn or new_an:
                track_status = track.get("trackStatus", "").lower()
                # Check if track status allows modification
                if track_status in ("subscription", "downloaded"):
                    # Use the method of this class to update the track
                    if await self.update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
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
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                    else:
                        self.error_logger.error("Failed to apply cleaning update for track ID %s", track_id)
                else:
                    self.console_logger.debug(f"Skipping track update for '{orig_name}' (ID: {track_id}) due to status '{track_status}'")

        # Run cleaning tasks concurrently
        clean_tasks = [asyncio.create_task(clean_track(t)) for t in all_tracks]
        await asyncio.gather(*clean_tasks)

        # --- Post-Processing for clean_artist ---
        # Note: Genre and Year updates are NOT run in clean_artist mode by default.
        # If desired, update_genres_by_artist_async and process_album_years
        # could be called here, operating on all_tracks.

        # Save results if any tracks were updated
        if updated_tracks_cleaning:
            # Sync changes with the main CSV database (utility function)
            # Use config from self.config, injected cache_service, loggers
            await sync_track_list_with_current(
                updated_tracks_cleaning,  # Pass only the list of tracks that were updated by cleaning
                get_full_log_path(self.config, "csv_output_file", "csv/track_list.csv"),
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=True,  # Partial sync is appropriate here
            )
            # Save a report of the specific changes made (utility function)
            # Use config from self.config, injected loggers
            save_unified_changes_report(
                changes_log_cleaning,
                get_full_log_path(self.config, "changes_report_file", "csv/changes_report.csv"),
                self.console_logger,
                self.error_logger,
                force_mode=force,  # Use force flag from arguments for console output
            )
            self.console_logger.info("Processed and cleaned %d tracks for artist: %s", len(updated_tracks_cleaning), artist)
        else:
            self.console_logger.info("No cleaning updates needed for artist: %s", artist)

    # Logic moved from old update_years
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Update Years")
    async def run_update_years(self, artist: Optional[str], force: bool) -> None:
        """
        Executes the album year update process.
        Uses injected services and config from self.config.
        """
        force_year_update = force  # Use force flag specific to this command
        artist_msg = f' for artist={artist}' if artist else ' for all artists'
        self.console_logger.info(f"Running in 'update_years' mode{artist_msg} (force={force_year_update})")

        # Fetch tracks (filtered by artist if provided, force refresh if requested)
        # Use injected fetch_tracks_async method
        tracks = await self.fetch_tracks_async(artist=artist, force_refresh=force_year_update)
        if not tracks:
            self.console_logger.warning(f"No tracks found{artist_msg}.")
            return

        # Determine whether to use incremental or full processing
        effective_last_run = datetime.min  # Default to beginning of time for full run
        if not force_year_update:  # Only check last run time if not forcing
            try:
                # Use injected cache_service
                last_run_time = await self.cache_service.get_last_run_timestamp()
                self.console_logger.info("Last incremental run timestamp: %s", last_run_time if last_run_time != datetime.min else "Never or Failed")
                effective_last_run = last_run_time  # Use actual last run time for incremental
            except Exception as e:
                self.error_logger.warning(f"Could not get last run timestamp from cache service: {e}. Assuming full run.")
                effective_last_run = datetime.min  # Fallback to full run if timestamp retrieval fails

        # Use injected process_album_years method
        updated_y, changes_y = await self.process_album_years(tracks, effective_last_run, force=force_year_update)

        # Save results if updates occurred
        if updated_y:
            # Sync updated tracks (containing new years) with the main CSV (utility function)
            # Use config from self.config, injected cache_service, loggers
            await sync_track_list_with_current(
                updated_y,
                get_full_log_path(self.config, "csv_output_file", "csv/track_list.csv"),
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=True,  # Partial sync is appropriate for year updates
            )
            # Save a report of the year changes (utility function)
            # Use config from self.config, injected loggers
            save_unified_changes_report(
                changes_y,
                get_full_log_path(self.config, "changes_report_file", "csv/changes_report.csv"),
                self.console_logger,
                self.error_logger,
                force_mode=force_year_update,  # Use force flag for console output
            )
            # Update last run time only if it was an incremental run (not forced)
            if not force_year_update:
                # Use injected update_last_incremental_run method
                await self.update_last_incremental_run()

        else:
            self.console_logger.info("No year updates needed or year retrieval disabled.")

    # Logic moved from old run_verify_database
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Verify Database")
    async def run_verify_database(self, force: bool) -> None:
        """
        Executes the database verification process.
        Uses injected verify_and_clean_track_database method.
        """
        self.console_logger.info(f"Executing verify_database command with force={force}")
        # Use the method of this class
        removed_count = await self.verify_and_clean_track_database(force=force)
        if removed_count > 0:
            self.console_logger.info("Database cleanup removed %d non-existent tracks", removed_count)
        else:
            self.console_logger.info("Database verification completed. No non-existent tracks found.")

        # Update last verification timestamp regardless of whether tracks were removed
        # This logic is inside verify_and_clean_track_database method now.

    # Logic moved from old run_verify_pending
    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Verify Pending")
    async def run_verify_pending(self, force: bool) -> None:
        """
        Executes verification for all pending albums regardless of timeframe.
        Uses injected services and config from self.config.
        """
        self.console_logger.info(f"Executing verify_pending command with force={force}")

        # Use injected pending_verification_service
        if not self.pending_verification_service:
            self.error_logger.error("PendingVerificationService is not available. Cannot verify pending albums.")
            return

        # Get all pending albums regardless of interval if force is True,
        # otherwise get only those needing verification now.
        # The get_verified_album_keys method already checks the interval.
        # If force is True for this command, we might need a way to bypass the interval check
        # within get_verified_album_keys or get all pending albums.
        # Let's assume get_verified_album_keys respects force=True or add a new method to PendingVerificationService.
        # If force is False, get_verified_album_keys already filters by interval.

        albums_to_verify_keys = set()
        if force:
            # Get all pending albums if force is true
            all_pending = self.pending_verification_service.get_all_pending_albums()
            albums_to_verify_keys = {f"{artist}|||{album}" for artist, album, _ in all_pending}
            self.console_logger.info(f"Force verification for all {len(albums_to_verify_keys)} pending albums.")
        else:
            # Get only albums needing verification based on interval
            albums_to_verify_keys = self.pending_verification_service.get_verified_album_keys()
            self.console_logger.info(f"Found {len(albums_to_verify_keys)} albums needing verification based on interval.")

        if not albums_to_verify_keys:
            self.console_logger.info("No albums currently need verification.")
            return

        verification_updated_tracks = []
        verification_changes = []

        # Fetch all tracks to find tracks belonging to pending albums
        # Use injected fetch_tracks_async method
        all_tracks = await self.fetch_tracks_async(force_refresh=False)  # Don't force refresh here unless necessary

        if not all_tracks:
            self.console_logger.warning("Could not fetch tracks to verify pending albums.")
            return

        # Group tracks by album key for easy lookup
        all_tracks_by_album_key = defaultdict(list)
        for track in all_tracks:
            artist = track.get("artist", "")
            album = track.get("album", "")
            if artist and album:
                album_key = f"{artist}|||{album}"
                all_tracks_by_album_key[album_key].append(track)

        # Process each album that needs verification
        for album_key in albums_to_verify_keys:
            try:
                artist, album = album_key.split("|||", 1)
                self.console_logger.info("Verifying album '%s - %s'", artist, album)

                # Find tracks for this album using the grouped map
                album_tracks = all_tracks_by_album_key.get(album_key, [])

                if album_tracks:
                    # Force verification regardless of current year values
                    # Use the method of this class
                    verified_tracks, verified_changes = await self.process_album_years(album_tracks, datetime.min, force=True)

                    if verified_tracks:
                        verification_updated_tracks.extend(verified_tracks)
                        verification_changes.extend(verified_changes)
                        # Remove from pending verification as we've processed it using injected service
                        self.pending_verification_service.remove_from_pending(artist, album)
                        self.console_logger.info("Successfully verified and updated album '%s - %s'", artist, album)
                    else:
                        self.console_logger.warning("Verification didn't result in updates for '%s - %s'", artist, album)
                        # If verification didn't result in updates, should it remain pending?
                        # The current logic in process_album_years marks as pending if not definitive.
                        # So, if process_album_years didn't find a definitive year, it will re-mark it.
                        # If it found a definitive year but no tracks needed updating (e.g., year was already correct),
                        # it won't be re-marked, and remove_from_pending is called above.
                        pass  # No action needed here, marking is handled in process_album_years
                else:
                    self.console_logger.warning("No tracks found for album '%s - %s' needing verification", artist, album)
                    # If no tracks found, still remove from verification list to avoid endless processing
                    # Use injected pending_verification_service
                    self.pending_verification_service.remove_from_pending(artist, album)

            except Exception as e:
                self.error_logger.error(f"Error verifying album {album_key}: {e}", exc_info=True)

        # Add verification results to other changes (these will be saved by the main process)
        if verification_updated_tracks:
            self.console_logger.info("Verified and updated %d tracks from %d albums", len(verification_updated_tracks), len(albums_to_verify_keys))
            # Note: These updated tracks and changes need to be returned or added to a shared list
            # to be saved by the main execution flow.
            # For now, we just log, but the main orchestration logic needs to handle collecting these results.
            pass  # The results handling needs to be integrated into the main run logic

    # Logic for the main execution methods of MusicUpdater
    # These methods orchestrate the process using the injected services (self.)
    # and the utility functions defined above (or imported).

    # Example of a method that uses utility functions and other methods

    # Correctly using the classmethod decorator from Analytics
    @Analytics.track_instance_method("Run Full Process")
    async def run_full_process(self, args: argparse.Namespace) -> None:
        """
        Runs the default full update process (cleaning, genres, years, pending verification).
        Uses injected services and config from self.config.
        """
        self.console_logger.info("Running in default mode (incremental or full processing).")

        # Check if we can run incrementally using the method of this class
        can_run = await self.can_run_incremental(force_run=args.force)

        if not can_run:
            self.console_logger.info("Incremental interval not reached. Skipping main processing.")
            return  # Exit if interval not met and not forced

        # Determine last run time for incremental filtering
        effective_last_run = datetime.min  # Default to beginning of time for full run
        if not args.force:  # Only check last run time if not forcing
            try:
                # Use injected cache_service
                last_run_time = await self.cache_service.get_last_run_timestamp()
                self.console_logger.info("Last incremental run timestamp: %s", last_run_time if last_run_time != datetime.min else "Never or Failed")
                effective_last_run = last_run_time  # Use actual last run time for incremental
            except Exception as e:
                self.error_logger.warning(f"Could not get last run timestamp from cache service: {e}. Assuming full run.")
                effective_last_run = datetime.min  # Fallback to full run if timestamp retrieval fails

        # --- Fetch Tracks (respecting test_artists if set globally) ---
        all_tracks: List[Dict[str, str]] = []
        # Use config from self.config
        test_artists_global = self.config.get("development", {}).get("test_artists", [])

        if test_artists_global:
            # Fetch tracks only for test artists if the list is populated
            self.console_logger.info(f"Development mode: Fetching tracks only for test_artists: {test_artists_global}")
            fetched_track_map = {}  # Use map to avoid duplicates if artist listed multiple times
            for art in test_artists_global:
                # Use injected fetch_tracks_async method
                art_tracks = await self.fetch_tracks_async(artist=art, force_refresh=args.force)
                for track in art_tracks:
                    if track.get("id"):
                        fetched_track_map[track["id"]] = track
            all_tracks = list(fetched_track_map.values())
            self.console_logger.info("Loaded %d tracks total for test artists.", len(all_tracks))
        else:
            # Fetch all tracks if test_artists list is empty
            # Use injected fetch_tracks_async method
            all_tracks = await self.fetch_tracks_async(force_refresh=args.force)
            self.console_logger.info("Loaded %d tracks from Music.app.", len(all_tracks))

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
        async def clean_track_default(track: Dict[str, str]) -> None:
            nonlocal updated_tracks_cleaning, changes_log_cleaning  # Access outer scope lists
            orig_name = track.get("name", "")
            orig_album = track.get("album", "")
            track_id = track.get("id", "")
            artist_name = track.get("artist", "Unknown")
            if not track_id:
                return

            # Use the utility function clean_names, pass config from self.config
            cleaned_nm, cleaned_al = clean_names(artist_name, orig_name, orig_album, self.config)

            # Determine if changes were actually made
            new_tn = cleaned_nm if cleaned_nm != orig_name else None
            new_an = cleaned_al if cleaned_al != orig_album else None

            if new_tn or new_an:
                track_status = track.get("trackStatus", "").lower()
                if track_status in ("subscription", "downloaded"):
                    # Use the method of this class to update the track
                    if await self.update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
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
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                    else:
                        self.error_logger.error("Failed to apply cleaning update for track ID %s", track_id)
                else:
                    self.console_logger.debug(f"Skipping track update for '{orig_name}' (ID: {track_id}) due to status '{track_status}'")

        # Run cleaning tasks concurrently
        clean_tasks_default = [asyncio.create_task(clean_track_default(t)) for t in all_tracks]
        await asyncio.gather(*clean_tasks_default)
        if updated_tracks_cleaning:
            self.console_logger.info("Cleaned %d track/album names.", len(updated_tracks_cleaning))
        else:
            self.console_logger.info("No track names or album names needed cleaning.")

        # --- Step 2: Update Genres ---
        self.console_logger.info("Starting genre update process...")
        # Pass the effective last run time (min if forced, actual otherwise)
        # Use the method of this class
        updated_g, changes_g = await self.update_genres_by_artist_async(all_tracks, effective_last_run)
        if updated_g:
            self.console_logger.info("Updated genres for %d tracks.", len(updated_g))
        else:
            self.console_logger.info("No genre updates needed based on incremental check or existing genres.")

        # --- Step 3: Update Album Years ---
        self.console_logger.info("Starting album year update process...")
        # process_album_years also needs last_run_time for incremental updates
        # Use the method of this class
        updated_y, changes_y = await self.process_album_years(all_tracks, effective_last_run, force=args.force)
        if updated_y:
            self.console_logger.info("Updated years for %d tracks.", len(updated_y))
        else:
            self.console_logger.info("No year updates needed or year retrieval disabled.")

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
                get_full_log_path(self.config, "csv_output_file", "csv/track_list.csv"),
                self.cache_service,
                self.console_logger,
                self.error_logger,
                partial_sync=not args.force,  # Use partial sync for incremental, full sync for force
            )
            # Use utility function save_unified_changes_report
            # Use config from self.config, injected loggers
            save_unified_changes_report(
                all_changes,
                get_full_log_path(self.config, "changes_report_file", "csv/changes_report.csv"),
                self.console_logger,
                self.error_logger,
                force_mode=args.force,  # Use force flag for console output if needed
            )
            self.console_logger.info("Processing complete. Logged %d changes.", len(all_changes))
        else:
            self.console_logger.info("No changes detected in this run.")

        # Update last run time only if it was an incremental run (not forced)
        if not args.force:
            # Use injected update_last_incremental_run method
            await self.update_last_incremental_run()

        # --- Step 4: Verify Pending Albums (after 30 days) ---
        # This step is part of the default process
        # Use the method of this class
        await self.run_verify_pending(force=False)  # Run pending verification based on interval


# --- Argument Parsing ---
def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments using argparse.
    """
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    parser.add_argument("--force", action="store_true", help="Force run, bypassing incremental checks and cache.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without applying them.")
    subparsers = parser.add_subparsers(dest="command")

    clean_artist_parser = subparsers.add_parser("clean_artist", help="Clean track/album names for a given artist.")
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name.")
    # Removed --force from subcommand, use global --force

    update_years_parser = subparsers.add_parser("update_years", help="Update album years from external APIs.")
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


# --- Main Execution Block ---
# --- Main Execution Block ---
def main() -> None:
    """
    Main synchronous function to parse arguments and run the async main logic.
    Handles top-level setup, teardown, exception handling, and listener shutdown.
    """
    # Import DependencyContainer here to break the circular import and make it available to main's scope
    from services.dependencies_service import DependencyContainer

    start_all = time.time()
    args = parse_arguments()

    # Initialize loggers and listener *early*
    # These are module-level variables
    # console_logger, error_logger, analytics_logger, listener = get_loggers(CONFIG) # Already initialized at module level

    # --- Add safety check for listener ---
    # Check the module-level listener
    if listener is None:
        console_logger.error("Failed to initialize QueueListener for logging. File logs might be missing.")

    # --- Handle Dry Run mode ---
    if args.dry_run:
        console_logger.info("Running in dry-run mode. No changes will be applied.")
        try:
            # dry_run.main() needs access to config and potentially loggers
            # It should ideally obtain these via dependency injection or arguments
            # For now, we call the existing dry_run.main() which uses global CONFIG and loggers
            # This part will be refactored in Step 3.2
            asyncio.run(dry_run.main())
        except Exception as dry_run_err:
            error_logger.error(f"Error during dry run: {dry_run_err}", exc_info=True)
            sys.exit(1)  # Exit with error code
        finally:
            # Stop listener after dry run
            if listener:
                console_logger.info("Stopping QueueListener (after dry run)...")
                listener.stop()
            # Explicitly close handlers (might be redundant if listener closes them)
            console_logger.info("Explicitly closing logger handlers (after dry run)...")
            # Use module-level loggers or root logger to get handlers
            all_handlers = []
            # Close root handlers first
            for handler in logging.root.handlers[:]:
                all_handlers.append(handler)
                logging.root.removeHandler(handler)
            # Close handlers attached to specific loggers (console, error, analytics)
            for name in ['console_logger', 'main_logger', 'analytics_logger', 'year_updates']:  # Include names of loggers used
                logger = logging.getLogger(name)
                for handler in logger.handlers[:]:
                    all_handlers.append(handler)
                    logger.removeHandler(handler)

            # Close all collected handlers
            for handler in all_handlers:
                try:
                    handler.close()
                except Exception as e:
                    print(f"Error closing handler {handler}: {e}", file=sys.stderr)

            end_all = time.time()
            print(f"\nTotal script execution time (dry run): {end_all - start_all:.2f} seconds")
            sys.exit(0)  # Exit after dry run

    # --- Initialize Dependency Container ---
    # The import is now at the top of the main function
    deps = None
    try:
        # Create the DependencyContainer instance
        deps = DependencyContainer(CONFIG_PATH)

        # Get the main orchestrator instance from the container
        music_updater = deps.get_music_updater()

        # Always check if Music.app is running first (utility function)
        if not is_music_app_running():
            console_logger.error("Music app is not running! Please start Music.app before running this script.")
            sys.exit(1)  # Exit if Music app is not running

        # --- Command-Specific Execution Paths ---
        # Use asyncio.run for the main entry point
        if hasattr(args, 'command') and args.command == "verify_database":
            # Call the corresponding method on the music_updater instance
            asyncio.run(music_updater.run_verify_database(force=args.force))

        elif args.command == "clean_artist":
            # Call the corresponding method on the music_updater instance
            asyncio.run(music_updater.run_clean_artist(artist=args.artist, force=args.force))

        elif args.command == "update_years":
            # Call the corresponding method on the music_updater instance
            asyncio.run(music_updater.run_update_years(artist=args.artist, force=args.force))

        elif args.command == "verify_pending":
            # Call the corresponding method on the music_updater instance
            asyncio.run(music_updater.run_verify_pending(force=args.force))

        else:  # Default mode
            # Call the main processing method on the music_updater instance
            asyncio.run(music_updater.run_full_process(args))

    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user (main level).")
        sys.exit(1)  # Exit with error code upon interruption
    except Exception as exc:
        console_logger.error("Critical error during main execution: %s", exc, exc_info=True)
        # Try logging to error logger from deps if available
        if deps and hasattr(deps, 'get_error_logger'):
            deps.get_error_logger().error("Critical error during main execution: %s", exc, exc_info=True)
        sys.exit(1)  # Exit with error code upon any other critical exception
    finally:
        # --- Final Cleanup ---
        # Analytics report is generated within the MusicUpdater methods now,
        # or can be called here if needed, accessing analytics via deps.analytics
        if deps and hasattr(deps, 'get_analytics'):
            try:
                # Generate report using the analytics instance from deps
                deps.get_analytics().generate_reports(force_mode=args.force)
            except Exception as report_err:
                console_logger.error(f"Error generating report: {report_err}", exc_info=True)

        # --- Stop Listener and Close Handlers ---
        # Use the module-level listener variable
        if listener:
            console_logger.info("Stopping QueueListener...")
            listener.stop()  # Stop the background thread first

        console_logger.info("Explicitly closing logger handlers...")
        # Use module-level loggers or root logger to get handlers
        all_handlers = []
        # Close root handlers first
        for handler in logging.root.handlers[:]:
            all_handlers.append(handler)
            logging.root.removeHandler(handler)
        # Close handlers attached to specific loggers (console, error, analytics)
        for name in ['console_logger', 'main_logger', 'analytics_logger', 'year_updates']:  # Include names of loggers used
            logger = logging.getLogger(name)
            for handler in logger.handlers[:]:
                all_handlers.append(handler)
                logger.removeHandler(handler)

        # Close all collected handlers
        for handler in all_handlers:
            try:
                handler.close()
            except Exception as e:
                print(f"Error closing handler {handler}: {e}", file=sys.stderr)

        end_all = time.time()
        print(f"\nTotal script execution time: {end_all - start_all:.2f} seconds")
        # sys.exit(0) # Exit status is handled by explicit sys.exit calls above


if __name__ == "__main__":
    main()
