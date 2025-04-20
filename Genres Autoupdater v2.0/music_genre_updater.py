#!/usr/bin/env python3

"""
Music Genre Updater Script v2.0

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
    - Dry run mode: Simulates all operations without making any actual changes to the Music library or files.

Commands:
    - Default (no arguments): Runs the main processing pipeline (clean names, update genres, update years). Operates incrementally unless forced.
    - clean_artist: Cleans track/album names only for tracks by a specific artist.
    - update_years: Updates album years from external APIs, optionally filtered by artist.
    - verify_database: Checks the internal track database (CSV) against Music.app and removes non-existent tracks. Respects interval unless forced.
    - verify_pending: (Experimental/Specific Use) Triggers verification for all albums marked as pending, regardless of their individual timers.

Options:
    - --force: Overrides incremental interval checks, forces cache refresh for fetches, database verification, and processing of all relevant items.
    - --dry-run: Simulates changes without modifying the Music library or saving reports/CSV updates.
    - --artist: Specify target artist (used with `clean_artist` or `update_years` commands).

The script uses a dependency container (`DependencyContainer`) to manage services:
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
    # Run standard incremental update
    python3 music_genre_updater.py

    # Force a full run, ignoring incremental interval and cache
    python3 music_genre_updater.py --force

    # Clean names only for a specific artist
    python3 music_genre_updater.py clean_artist --artist "Artist Name"

    # Update album years for all artists (incrementally)
    python3 music_genre_updater.py update_years

    # Force update album years for a specific artist
    python3 music_genre_updater.py update_years --artist "Another Artist" --force

    # Verify the database, forcing check even if within interval
    python3 music_genre_updater.py verify_database --force

    # Run in simulation mode
    python3 music_genre_updater.py --dry-run
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
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

from services.dependencies_service import DependencyContainer
from services.pending_verification import PendingVerificationService
from utils import dry_run
from utils.analytics import Analytics
from utils.config import load_config
from utils.logger import get_full_log_path, get_loggers
from utils.reports import (
    load_track_list,
    save_changes_report,
    save_to_csv,
    save_unified_changes_report,
    sync_track_list_with_current,
)

# Global variable for dependency container (set in main_async)
DEPS = None

# Load configuration from YAML
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "my-config.yaml")


def get_config() -> Dict[str, Any]:
    """
    Get the current configuration by reloading the YAML file.
    """
    return load_config(CONFIG_PATH)


# Initialize loggers
CONFIG = get_config()
console_logger, error_logger, analytics_logger, listener = get_loggers(CONFIG)
analytics_log_file = get_full_log_path(CONFIG, "analytics_log_file", "analytics/analytics.log")

if CONFIG.get("python_settings", {}).get("prevent_bytecode", False):
    # Disable Python bytecode generation
    sys.dont_write_bytecode = True
    console_logger.debug("Python bytecode generation disabled (__pycache__ will not be created)")


def check_paths(paths: List[str], logger: logging.Logger) -> None:
    """
    Check if the specified paths exist and are readable.

    :param paths: A list of paths to check.
    :param logger: The logger to use for error messages.
    """
    for path in paths:
        if not os.path.exists(path):
            logger.error(f"Path {path} does not exist.")
            raise FileNotFoundError(f"Path {path} does not exist.")
        if not os.access(path, os.R_OK):
            logger.error(f"No read access to {path}.")
            raise PermissionError(f"No read access to {path}.")


check_paths([CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]], error_logger)

# Initialize analytics tracking
analytics = Analytics(CONFIG, console_logger, error_logger, analytics_logger)


def get_decorator(event_type: str):
    """
    A smart decorator factory that applies analytics tracking only when available.

    This decorator dynamically finds the current analytics instance at runtime and
    caches the decorated function for performance. Key features:

    - Lazily initializes: Only applies the real decorator when the function is called, not when it's defined
    - Graceful fallback: If no analytics instance is available, executes the original function without tracking
    - Caching: Saves the decorated function after first use to avoid re-decorating
    - Flexible source: Tries to find analytics from DEPS container first, then global scope

    :param event_type: The type of event to track in analytics (e.g., "AppleScript Execution")
    :return: A decorator function that will apply analytics tracking when available
    """

    def decorator(func):
        decorated_func = None  # cached decorated function

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal decorated_func
            current_analytics = None
            # No global statement needed when only reading global variables
            if DEPS and hasattr(DEPS, 'analytics') and DEPS.analytics:
                current_analytics = DEPS.analytics
            elif 'analytics' in globals() and analytics:
                current_analytics = analytics
            if current_analytics is None:
                return func(*args, **kwargs)
            if decorated_func is None:
                # Decorate the original function using the current analytics instance
                decorated_func = current_analytics.decorator(event_type)(func)
            return decorated_func(*args, **kwargs)

        return wrapper

    return decorator


@get_decorator("AppleScript Execution")
async def run_applescript_async(script_name: str, args: Optional[List[str]] = None) -> Optional[str]:
    """
    Runs the specified AppleScript with optional arguments asynchronously.

    :param script_name: The name of the AppleScript file to run
    :param args: Optional list of arguments to pass to the script
    :return: The result of the AppleScript execution
    """
    try:
        # AppleScript client checking
        if DEPS is None:
            error_logger.error("DependencyContainer is not initialized.")
            return None

        if DEPS.ap_client is None:
            error_logger.error("AppleScriptClient not initialized in dependency container.")
            return None

        # Checking the availability of the script
        apple_scripts_dir = CONFIG.get("apple_scripts_dir", "")
        script_path = os.path.join(apple_scripts_dir, script_name)

        if not os.path.exists(script_path):
            error_logger.error("Script file does not exist: %s", script_path)
            return None

        # Improved logging for tracking execution with arguments
        safe_args = [f"'{arg}'" for arg in args] if args else []
        console_logger.info("Running AppleScript: %s with args: %s", script_name, ', '.join(safe_args))
        console_logger.debug("Full script path: %s", script_path)

        # Launch script with configurable timeout
        timeout = CONFIG.get("applescript_timeout_seconds", 600)
        try:
            result = await asyncio.wait_for(DEPS.ap_client.run_script(script_name, args), timeout=float(timeout))
        except asyncio.TimeoutError:
            error_logger.error("Timeout while executing AppleScript %s", script_name)
            return None

        # Analysis of the result
        if result is None:
            console_logger.error("AppleScript %s returned None", script_name)
            return None

        if not result.strip():
            console_logger.error("AppleScript %s returned empty result", script_name)
            return None

        # Successful execution
        result_preview = result[:50] + "..." if len(result) > 50 else result
        console_logger.info("AppleScript %s executed successfully, got %d bytes. Preview: %s", script_name, len(result), result_preview)
        return result
    except (subprocess.SubprocessError, asyncio.TimeoutError, ValueError) as e:
        error_logger.error("Error running AppleScript %s: %s", script_name, e, exc_info=True)
        return None


@get_decorator("Parse Tracks")
def parse_tracks(raw_data: str) -> List[Dict[str, str]]:
    """
    Parses the raw data from AppleScript into a list of track dictionaries.

    :param raw_data: The raw data string fetched from AppleScript
    :return: A list of dictionaries representing the parsed track data
    """
    if not raw_data:
        error_logger.error("No data fetched from AppleScript.")
        return []
    tracks = []
    rows = raw_data.strip().split("\n")
    for row in rows:
        fields = row.split("~|~")
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
            # Adding old_year and new_year fields
            if len(fields) > 7:
                track["old_year"] = fields[7].strip()
            if len(fields) > 8:
                track["new_year"] = fields[8].strip()
            else:
                track["new_year"] = ""  # Initialize as an empty string if not present
            tracks.append(track)
        else:
            error_logger.error("Malformed track data: %s", row)
    return tracks


@get_decorator("Group Tracks by Artist")
def group_tracks_by_artist(tracks: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Group tracks by artist name into a dictionary for efficient processing.
    This allows the script to process tracks by artist, which is necessary for
    determining the dominant genre for each artist.

    :param tracks: A list of dictionaries containing track information.
    :return: A dictionary with artist names as keys and lists of tracks as values.
    """
    # Use defaultdict for efficient grouping without checking for key existence
    artists = defaultdict(list)
    for track in tracks:
        artist = track.get("artist", "Unknown")
        artists[artist].append(track)
    # Return defaultdict directly without converting to dict for better performance
    return artists


@get_decorator("Determine Dominant Genre")
def determine_dominant_genre_for_artist(artist_tracks: List[Dict[str, str]]) -> str:
    """
    Determine the dominant genre for an artist based on the earliest genre of their track.

    The logic of genre definition:
    1. Find the earliest track for each album
    2. From these tracks, find the earliest album (by the date of addition)
    3. In the earliest album, find the earliest track
    4. Return the genre of this track as the dominant one for the artist

    Rationale: The earliest track from the earliest album usually
    best represents the artist's main genre in the Music library.

    :param artist_tracks: A list of dictionaries containing track information for the artist.
    :return: The dominant genre for the artist.
    """
    if not artist_tracks:
        return "Unknown"
    try:
        album_earliest: Dict[str, Dict[str, str]] = {}
        for track in artist_tracks:
            album = track.get("album", "Unknown")
            track_date = datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            if album not in album_earliest:
                album_earliest[album] = track
            else:
                existing_date = datetime.strptime(album_earliest[album].get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
                if track_date < existing_date:
                    album_earliest[album] = track
        earliest_album = min(
            album_earliest.values(),
            key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"),
        ).get("album", "Unknown")
        earliest_album_tracks = [track for track in artist_tracks if track.get("album") == earliest_album]
        earliest_track = min(
            earliest_album_tracks,
            key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"),
        )
        return earliest_track.get("genre") or "Unknown"
    except (TypeError, ValueError, AttributeError, IndexError) as e:
        error_logger.error("Error in determine_dominant_genre_for_artist: %s", e, exc_info=True)
        return "Unknown"


@get_decorator("Check Music App Running")
def is_music_app_running() -> bool:
    """
    Checks if the Music.app is currently running.

    :return: True if Music.app is running, False otherwise
    """
    try:
        script = 'tell application "System Events" to (name of processes) contains "Music"'
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
        return result.stdout.strip().lower() == "true"
    except (subprocess.SubprocessError, OSError) as e:
        error_logger.error("Unable to check Music.app status: %s", e, exc_info=True)
        return False


@get_decorator("Remove Parentheses with Keywords")
def remove_parentheses_with_keywords(name: str, keywords: List[str]) -> str:
    """
    Remove parentheses and their content if they contain any of the specified keywords.
    Handles nested parentheses correctly using a stack-based algorithm that processes
    brackets from inner to outer, ensuring proper handling of complex cases.

    :param name: The name to clean (e.g. "Track Name (2019 Remaster) [Deluxe Edition]")
    :param keywords: A list of keywords to check for (e.g. ["remaster", "deluxe"])
    :return: The cleaned name with matching parentheses removed
    """
    try:
        logging.debug("remove_parentheses_with_keywords called with name='%s' and keywords=%s", name, keywords)
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

            # Sort pairs by start index
            pairs.sort()

            # Find which pairs to remove based on keywords
            to_remove = set()
            for start, end in pairs:
                content = current_name[start + 1 : end]  # noqa: E203
                if any(keyword.lower() in content.lower() for keyword in keyword_set):
                    to_remove.add((start, end))

            # If nothing found to remove, we're done
            if not to_remove:
                break

            # Remove brackets (from right to left to maintain indices)
            for start, end in sorted(to_remove, reverse=True):
                current_name = current_name[:start] + current_name[end + 1 :]  # noqa: E203

        # Clean up multiple spaces
        result = re.sub(r"\s+", " ", current_name).strip()
        logging.debug("Cleaned result: '%s'", result)
        return result

    except (TypeError, ValueError, AttributeError, IndexError) as e:
        error_logger.error("Error in remove_parentheses_with_keywords: %s", e, exc_info=True)
        return name


@get_decorator("Update Album Tracks Bulk")
async def update_album_tracks_bulk_async(track_ids: List[str], year: str) -> bool:
    """
    Updates the year property for multiple tracks in bulk using a batched approach for better performance.

    This function handles validation of track IDs, splits processing into configurable batches,
    and provides detailed success/failure tracking. It uses a resilient approach by continuing
    even if some track updates fail.

    :param track_ids: List of track IDs to update with the new year value
    :param year: The year value to set (as a string)
    :return: True if at least one track was successfully updated, False if all updates failed or inputs were invalid
    """
    try:
        if not track_ids or not year:
            console_logger.warning("No track IDs or year provided for bulk update.")
            return False

        # Validation of track IDs and year before processing
        filtered_track_ids = []
        for track_id in track_ids:
            try:
                if track_id and track_id.isdigit() and not track_id == year:
                    filtered_track_ids.append(track_id)
                else:
                    console_logger.warning("Skipping invalid track ID: '%s'", track_id)
            except (ValueError, TypeError):
                console_logger.warning("Skipping non-numeric track ID: '%s'", track_id)

        if not filtered_track_ids:
            console_logger.warning("No valid track IDs left after filtering")
            return False

        console_logger.info("Updating year to %s for %d tracks (filtered from %d initial tracks)", year, len(filtered_track_ids), len(track_ids))

        successful_updates = 0
        failed_updates = 0
        batch_size = min(20, CONFIG.get("batch_size", 20))

        for i in range(0, len(filtered_track_ids), batch_size):
            batch = filtered_track_ids[i : i + batch_size]  # noqa: E203
            tasks = []

            for track_id in batch:
                console_logger.info("Adding update task for track ID %s to year %s", track_id, year)
                tasks.append(run_applescript_async("update_property.applescript", [track_id, "year", year]))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(results):
                track_id = batch[idx] if idx < len(batch) else "unknown"
                if isinstance(result, Exception):
                    error_logger.error("Exception updating year for track %s: %s", track_id, result)
                    failed_updates += 1
                elif not result:
                    error_logger.error("Empty result updating year for track %s", track_id)
                    failed_updates += 1
                elif "Success" not in result:
                    if "not found" in result:
                        console_logger.warning("Track %s not found, removing from processing", track_id)
                    else:
                        error_logger.error("Failed to update year for track %s: %s", track_id, result)
                    failed_updates += 1
                else:
                    successful_updates += 1
                    console_logger.info("Successfully updated year for track %s to %s", track_id, year)

            if i + batch_size < len(filtered_track_ids):
                await asyncio.sleep(0.5)

        console_logger.info("Year update results: %s successful, %s failed", successful_updates, failed_updates)
        return successful_updates > 0
    except (ValueError, TypeError, KeyError) as e:
        error_logger.error("Error in update_album_tracks_bulk_async: %s", e, exc_info=True)
        return False


@get_decorator("Process Album Years")
async def process_album_years(
    tracks: List[Dict[str, str]], last_run_time: datetime = datetime.min, force: bool = False
) -> Optional[tuple[list[Any], list[Any]]]:
    """
    Manages album year updates for all tracks using external music databases.

    This function orchestrates the album year update process by:
    1. Filtering tracks based on the last run time (for incremental updates)
    2. Filtering out prerelease and unavailable tracks
    3. Calling the external API service to retrieve accurate album years
    4. Updating tracks in the Music library with the retrieved years
    5. Persisting changes to CSV files and generating change reports

    The function respects the configuration settings for year retrieval and
    properly handles API rate limits and service connections.

    :param tracks: List of track dictionaries to process for year updates
    :param last_run_time: The timestamp of the last incremental update (datetime.min for full runs)
    :param force: If True, updates years even if they already exist and ignores last_run_time
    :return: Tuple of (updated_tracks, changes_log)
    """
    if not CONFIG.get("year_retrieval", {}).get("enabled", False):
        console_logger.info("Album year updates are disabled in config. Skipping.")
        return [], []

    if not tracks:
        console_logger.info("No tracks provided for year updates.")
        return [], []

    console_logger.info("Starting album year updates (force=%s)", force)

    # Skip prerelease tracks
    filtered_tracks = []
    for track in tracks:
        track_status = track.get("trackStatus", "").lower()
        if track_status == "prerelease":
            console_logger.debug("Skipping prerelease track: %s - %s - %s", track.get('name', ''), track.get('artist', ''), track.get('album', ''))
            continue
        elif track_status == "no longer available":
            console_logger.debug("Skipping unavailable track: %s - %s - %s", track.get('name', ''), track.get('artist', ''), track.get('album', ''))
            continue
        else:
            filtered_tracks.append(track)

    console_logger.info("Using %d/%d tracks (skipped prerelease/unavailable)", len(filtered_tracks), len(tracks))

    # Filter by last_run_time for incremental updates if not forced
    if last_run_time and last_run_time != datetime.min and not force:
        console_logger.info("Filtering tracks added after %s for album year updates", last_run_time)
        filtered_tracks = [
            track
            for track in filtered_tracks
            if datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") > last_run_time
        ]
        console_logger.info("Found %d tracks added since last run for album year updates", len(filtered_tracks))

    # If no tracks remain after filtering, return early
    if not filtered_tracks:
        console_logger.info("No tracks to process for album year updates after filtering.")
        return [], []

    try:
        await DEPS.external_api_service.initialize()
        updated_y, changes_y = await update_album_years_async(filtered_tracks, force=force)

        if updated_y:
            await sync_track_list_with_current(
                updated_y,
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                DEPS.cache_service,
                console_logger,
                error_logger,
                partial_sync=True,
            )
            save_changes_report(
                changes_y,
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]),
                console_logger,
                error_logger,
                force_mode=force,
            )
            console_logger.info("Updated %d tracks with album years.", len(updated_y))
        else:
            console_logger.info("No tracks needed album year updates.")

        return updated_y, changes_y
    except (ValueError, TypeError, KeyError, IOError, ConnectionError) as e:
        error_logger.error("Error in process_album_years: %s", e, exc_info=True)
        return [], []
    finally:
        await DEPS.external_api_service.close()


@get_decorator("Clean Names")
def clean_names(artist: str, track_name: str, album_name: str) -> Tuple[str, str]:
    """
    Cleans the artist, track name, and album name based on the configuration settings.

    :param artist: The artist name to clean
    :param track_name: The track name to clean
    :param album_name: The album name to clean
    :return: A tuple of cleaned track name and album name
    """
    console_logger.info("clean_names called with: artist='%s', track_name='%s', album_name='%s'", artist, track_name, album_name)
    exceptions = CONFIG.get("exceptions", {}).get("track_cleaning", [])
    is_exception = any(exc.get("artist", "").lower() == artist.lower() and exc.get("album", "").lower() == album_name.lower() for exc in exceptions)
    if is_exception:
        console_logger.info("No cleaning applied due to exceptions for artist '%s', album '%s'.", artist, album_name)
        return track_name.strip(), album_name.strip()
    remaster_keywords = CONFIG.get("cleaning", {}).get("remaster_keywords", ["remaster", "remastered"])
    album_suffixes = set(CONFIG.get("cleaning", {}).get("album_suffixes_to_remove", []))

    def clean_string(val: str, remaster_keywords: List[str]) -> str:
        new_val = remove_parentheses_with_keywords(val, remaster_keywords)
        new_val = re.sub(r"\s+", " ", new_val).strip()
        return new_val if new_val else "Unknown"

    original_track = track_name
    original_album = album_name
    cleaned_track = clean_string(track_name, remaster_keywords)
    cleaned_album = clean_string(album_name, remaster_keywords)
    for suffix in album_suffixes:
        if cleaned_album.endswith(suffix):
            cleaned_album = cleaned_album[: -len(suffix)].strip()
            console_logger.info("Removed suffix '%s' from album. New album name: '%s'", suffix, cleaned_album)
    console_logger.info("Original track name: '%s' -> '%s'", original_track, cleaned_track)
    console_logger.info("Original album name: '%s' -> '%s'", original_album, cleaned_album)
    return cleaned_track, cleaned_album


async def update_album_years_async(tracks: List[Dict[str, str]], force: bool = False) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Manages album year updates for tracks by querying external music databases with optimized concurrency.

    This function orchestrates the album year update process with:
    - Batch processing with controlled concurrent API calls (via semaphore)
    - Album-based processing to minimize redundant API requests
    - Caching of previously fetched years to reduce API usage
    - Adaptive delays between batches based on API usage
    - Integration with pending verification system for uncertain year data
    - Proper handling of tracks that cannot be modified (prerelease/unavailable)
    - Detailed logging of all changes for auditing purposes

    Args:
        tracks: List of track dictionaries to process
        force: If True, bypasses cache and updates years even if they already exist

    Returns:
        Tuple of (updated_tracks, changes_log) containing modified tracks and change records

    Example:
        updated_tracks, changes = await update_album_years_async(tracks, force=True)
        print(f"Updated {len(updated_tracks)} tracks with new year information")
    """
    try:
        # Initialize pending verification service
        pending_verification_service = None
        if hasattr(DEPS, 'pending_verification_service') and DEPS.pending_verification_service:
            pending_verification_service = DEPS.pending_verification_service
            console_logger.info("Using pending verification service for album year tracking")
        else:
            console_logger.warning("Pending verification service not available, all year determinations will be treated as definitive")

        # Setup logging
        year_changes_log_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"].get("year_changes_log_file", "main/year_changes.log"))
        os.makedirs(os.path.dirname(year_changes_log_file), exist_ok=True)
        year_logger = logging.getLogger("year_updates")
        year_logger.setLevel(logging.INFO)
        if not year_logger.handlers:
            fh = logging.FileHandler(year_changes_log_file)
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            year_logger.addHandler(fh)

        console_logger.info("Starting album year update process")
        year_logger.info("Starting year update for %d tracks", len(tracks))

        # Skip prerelease tracks that cannot be modified
        filtered_tracks = []
        for track in tracks:
            track_status = track.get("trackStatus", "").lower()
            if track_status == "prerelease":
                console_logger.debug(
                    "Skipping prerelease track: %s - %s - %s", track.get('name', ''), track.get('artist', ''), track.get('album', '')
                )
                continue
            elif track_status == "no longer available":
                console_logger.debug(
                    "Skipping unavailable track: %s - %s - %s", track.get('name', ''), track.get('artist', ''), track.get('album', '')
                )
                continue
            else:
                filtered_tracks.append(track)

        console_logger.info("Using %d/%d tracks (skipped prerelease/unavailable)", len(filtered_tracks), len(tracks))
        tracks = filtered_tracks

        # Group tracks by album to reduce API calls
        albums = {}
        for track in tracks:
            artist = track.get("artist", "Unknown")
            album = track.get("album", "Unknown")
            key = f"{artist}|{album}"
            if key not in albums:
                albums[key] = {"artist": artist, "album": album, "tracks": []}
            albums[key]["tracks"].append(track)

        console_logger.info("Processing %d unique albums", len(albums))
        year_logger.info("Starting year update for %d albums", len(albums))

        updated_tracks = []
        changes_log = []

        # Define the album processing function
        async def process_album(album_data):
            nonlocal updated_tracks, changes_log
            artist = album_data["artist"]
            album = album_data["album"]
            album_tracks = album_data["tracks"]

            if not album_tracks:
                return False

            # Extract the current library year from the first track
            current_library_year = album_tracks[0].get("old_year", "") if album_tracks else ""

            # Check if this album is pending verification
            should_verify = False
            if pending_verification_service:
                should_verify = pending_verification_service.is_verification_needed(artist, album)
                if should_verify:
                    console_logger.info("Album '%s - %s' is due for verification", artist, album)

            # Try to get cached year first, unless forcing or verification is due
            year = None
            is_definitive = True

            if force or should_verify:
                # Bypass cache when forcing or verification is due
                year_logger.info("Fetching year for '%s - %s' from external API (force=%s, verify=%s)", artist, album, force, should_verify)
                year, is_definitive = await DEPS.external_api_service.get_album_year(
                    artist, album, current_library_year, pending_verification_service
                )
                api_call_made = True

                if year and is_definitive:
                    await DEPS.cache_service.store_album_year_in_cache(artist, album, year)
                    year_logger.info("Stored year %s for '%s - %s' in cache", year, artist, album)

                    # Remove from pending if we got a definitive result
                    if pending_verification_service:
                        pending_verification_service.remove_from_pending(artist, album)
            else:
                # Try to get cached year first
                year = await DEPS.cache_service.get_album_year_from_cache(artist, album)

                if not year:
                    year_logger.info("No cached year found for '%s - %s', fetching from API", artist, album)
                    year, is_definitive = await DEPS.external_api_service.get_album_year(
                        artist, album, current_library_year, pending_verification_service
                    )
                    api_call_made = True

                    if year and is_definitive:
                        await DEPS.cache_service.store_album_year_in_cache(artist, album, year)
                        year_logger.info("Stored year %s for '%s - %s' in cache", year, artist, album)

                    # Remove from pending if we got a definitive result
                    if pending_verification_service and is_definitive:
                        pending_verification_service.remove_from_pending(artist, album)
                else:
                    year_logger.info("Using cached year %s for '%s - %s'", year, artist, album)
                    api_call_made = False

            if not year:
                return api_call_made

            # Skip update if year is the same as current library year
            if year == current_library_year:
                year_logger.info("Year %s for '%s - %s' matches current library year, no update needed", year, artist, album)
                return api_call_made

            track_ids = [track.get("id", "") for track in album_tracks]
            track_ids = [tid for tid in track_ids if tid]

            if track_ids:
                tracks_to_update = []

                for track in album_tracks:
                    current_year = track.get("new_year", "").strip()
                    track_status = track.get("trackStatus", "").lower()

                    # Skip tracks with statuses that can't be modified
                    if track_status in ("prerelease", "no longer available"):
                        continue

                    if force or not current_year or current_year != year:
                        tracks_to_update.append(track)

                if not tracks_to_update:
                    year_logger.info("No tracks need update for '%s - %s' (year: %s, force=%s)", artist, album, year, force)
                    return api_call_made

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

                success = await update_album_tracks_bulk_async(track_ids_to_update, year)
                if success:
                    for track in tracks_to_update:
                        track_id = track.get("id", "")
                        if track_id:
                            track["old_year"] = track.get("old_year", "") or track.get("new_year", "")
                            track["new_year"] = year
                            updated_tracks.append(track)

                            changes_log.append(
                                {
                                    "change_type": "year",
                                    "artist": artist,
                                    "album": album,
                                    "track_name": track.get("name", "Unknown"),
                                    "old_year": track.get("old_year", ""),
                                    "new_year": year,
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                }
                            )
                    year_logger.info("Successfully updated %d tracks for '%s - %s' to year %s", len(track_ids_to_update), artist, album, year)
                else:
                    year_logger.error("Failed to update year for '%s - %s'", artist, album)

            return api_call_made

        # Fetch optimal batch parameters from config
        batch_size = CONFIG.get("year_retrieval", {}).get("batch_size", 20)
        delay_between_batches = CONFIG.get("year_retrieval", {}).get("delay_between_batches", 30)
        concurrent_limit = CONFIG.get("year_retrieval", {}).get("concurrent_api_calls", 5)
        adaptive_delay = CONFIG.get("year_retrieval", {}).get("adaptive_delay", False)

        album_items = list(albums.items())

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(concurrent_limit)

        # Process albums in batches
        for i in range(0, len(album_items), batch_size):
            batch = album_items[i : i + batch_size]  # noqa: E203
            batch_tasks = []

            # Create tasks for each album in the batch
            for _, album_data in batch:
                # Use semaphore to limit concurrency
                async def process_with_semaphore(data):
                    async with semaphore:
                        return await process_album(data)

                batch_tasks.append(process_with_semaphore(album_data))

            # Execute batch with controlled concurrency
            api_calls_made = await asyncio.gather(*batch_tasks)

            # Log progress
            batch_num = i // batch_size + 1
            total_batches = (len(album_items) + batch_size - 1) // batch_size
            console_logger.info("Processed batch %d/%d", batch_num, total_batches)

            # Calculate adaptive delay for next batch if needed
            if i + batch_size < len(album_items):
                api_calls_count = sum(1 for call in api_calls_made if call)

                if adaptive_delay and api_calls_count > 0:
                    # Scale delay based on API usage ratio
                    usage_ratio = api_calls_count / len(batch)
                    adjusted_delay = max(1, round(delay_between_batches * usage_ratio))

                    console_logger.info("Adaptive delay: %ds based on %d/%d API calls", adjusted_delay, api_calls_count, len(batch))
                    await asyncio.sleep(adjusted_delay)
                elif any(api_calls_made):
                    console_logger.info("API calls were made. Waiting %ds before next batch", delay_between_batches)
                    await asyncio.sleep(delay_between_batches)
                else:
                    console_logger.info("No API calls made. Proceeding to next batch immediately.")

        console_logger.info("Album year update complete. Updated %d tracks.", len(updated_tracks))
        year_logger.info("Album year update complete. Updated %d tracks.", len(updated_tracks))

        return updated_tracks, changes_log
    except (ValueError, TypeError, KeyError, IOError, ConnectionError) as e:
        error_logger.error("Error in update_album_years_async: %s", e, exc_info=True)
        return [], []


@get_decorator("Can Run Incremental")
def can_run_incremental(force_run: bool = False) -> bool:
    """
    Checks if the incremental interval has passed since the last run with improved resilience.

    :param force_run: True to force the run, False to check the interval.
    :return: True if the script can run, False otherwise.
    """
    if force_run:
        return True

    last_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["last_incremental_run_file"])
    interval = CONFIG.get("incremental_interval_minutes", 60)

    # If the file does not exist, allow it to run
    if not os.path.exists(last_file):
        console_logger.info("Last incremental run file not found. Proceeding.")
        return True

    # Trying to read a file with repeated attempts
    max_retries = 3
    retry_delay = 0.5  # seconds

    for attempt in range(max_retries):
        try:
            # Trying to read a file with rollback on failure
            with open(last_file, "r", encoding="utf-8") as f:
                last_run_str = f.read().strip()

            try:
                last_run_time = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                error_logger.error("Invalid date in %s. Proceeding with execution.", last_file)
                return True

            next_run_time = last_run_time + timedelta(minutes=interval)
            now = datetime.now()

            if now >= next_run_time:
                return True
            else:
                diff = next_run_time - now
                minutes_remaining = diff.seconds // 60
                console_logger.info("Last run: %s. Next run in %d mins.", last_run_time.strftime('%Y-%m-%d %H:%M'), minutes_remaining)
                return False

        except (OSError, IOError) as e:
            if attempt < max_retries - 1:
                # If this is not the last attempt, we wait and try again
                console_logger.warning(f"Error reading last run file (attempt {attempt+1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential increase in latency
            else:
                # If all attempts fail, log and allow launch
                error_logger.error(f"Error accessing last run file after {max_retries} attempts: {e}")
                return True

    # This code should not be reached, but for security:
    return True


@get_decorator("Update Last Incremental Run")
def update_last_incremental_run() -> None:
    """
    Update the timestamp of the last incremental run in a file.

    :return: None
    """
    last_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["last_incremental_run_file"])
    with open(last_file, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


@get_decorator("Database Verification")
async def verify_and_clean_track_database(force: bool = False) -> int:
    """
    Verifies tracks in the CSV database against Music.app and removes entries for tracks
    that no longer exist. Respects the 'development.test_artists' config setting.

    Args:
        force (bool): If True, runs verification even if recently performed based on auto_verify_days.

    Returns:
        int: The number of invalid tracks removed from the database.
    """
    removed_count = 0
    try:
        console_logger.info("Starting database verification process...")
        csv_path = get_full_log_path(CONFIG, "csv_output_file", "csv/track_list.csv")

        if not os.path.exists(csv_path):
            console_logger.info("Track database not found at %s, nothing to verify.", csv_path)
            return 0

        # Load the entire track list from CSV into a dictionary {track_id: track_dict}
        csv_tracks_map = load_track_list(csv_path)

        if not csv_tracks_map:
            console_logger.info("No tracks loaded from database to verify.")
            return 0

        # --- Check if verification is needed based on interval (unless forced) ---
        last_verify_file = get_full_log_path(CONFIG, "last_db_verify.log", "main/last_db_verify.log")  # Define log file path
        auto_verify_days = CONFIG.get("database_verification", {}).get("auto_verify_days", 7)

        if not force and auto_verify_days > 0 and os.path.exists(last_verify_file):
            try:
                with open(last_verify_file, "r", encoding="utf-8") as f:
                    last_verify_str = f.read().strip()
                last_date = datetime.strptime(last_verify_str, "%Y-%m-%d").date()
                days_since_last = (datetime.now().date() - last_date).days
                if days_since_last < auto_verify_days:
                    console_logger.info(
                        f"Database verified {days_since_last} days ago ({last_date}). "
                        f"Skipping verification (interval: {auto_verify_days} days). Use --force to override."
                    )
                    return 0  # Verification not needed yet
            except (FileNotFoundError, PermissionError, IOError, ValueError) as e:
                error_logger.warning(f"Could not read or parse last verification date ({last_verify_file}): {e}. Proceeding with verification.")
                pass  # Proceed if file is missing or corrupt

        # --- Apply test_artists filter if configured ---
        tracks_to_verify_map = csv_tracks_map  # Start with all tracks by default
        test_artists_config = CONFIG.get("development", {}).get("test_artists", [])

        if test_artists_config:
            console_logger.info("Limiting database verification to specified test_artists: %s", test_artists_config)
            # Filter the map to include only tracks from test artists
            tracks_to_verify_map = {
                track_id: track for track_id, track in csv_tracks_map.items() if track.get("artist") in test_artists_config  # Filter condition
            }
            console_logger.info("After filtering by test_artists, %d tracks will be verified.", len(tracks_to_verify_map))

        # --- Check if any tracks remain after filtering ---
        if not tracks_to_verify_map:
            console_logger.info("No tracks to verify (either DB is empty or after filtering by test_artists).")
            # Update the verification timestamp even if nothing was checked
            try:
                os.makedirs(os.path.dirname(last_verify_file), exist_ok=True)
                with open(last_verify_file, "w", encoding="utf-8") as f:
                    f.write(datetime.now().strftime("%Y-%m-%d"))
            except IOError as e:
                error_logger.error(f"Failed to write last verification timestamp: {e}")
            return 0  # Nothing removed

        # --- Log correct count and get IDs ---
        tracks_to_verify_count = len(tracks_to_verify_map)
        console_logger.info("Verifying %d tracks against Music.app...", tracks_to_verify_count)
        track_ids_to_check = list(tracks_to_verify_map.keys())  # Get IDs of tracks to check

        # --- Async verification helper ---
        async def verify_track_exists(track_id: str) -> bool:
            """Checks if a track ID exists in the Music library."""
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
                # Ensure dependency container and client are available
                if DEPS and DEPS.ap_client:
                    result = await DEPS.ap_client.run_script_code(script)
                    # Return True only if AppleScript explicitly confirmed existence
                    return result == "exists"
                else:
                    error_logger.error("Cannot verify track %s: AppleScriptClient not available.", track_id)
                    return True  # Assume exists if client unavailable
            except Exception as e:
                error_logger.error(f"Exception during AppleScript execution for track {track_id}: {e}")
                return True  # Assume exists on error to prevent accidental deletion

        # --- Batch Processing ---
        batch_size = CONFIG.get("database_verification", {}).get("batch_size", 10)
        invalid_track_ids: List[str] = []
        console_logger.info("Checking tracks in batches of %d...", batch_size)

        for i in range(0, len(track_ids_to_check), batch_size):
            batch_ids = track_ids_to_check[i : i + batch_size]  # noqa: E203
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
                        error_logger.warning(f"Exception during verification task for track ID {track_id}: {result}")
                    # Add track ID to the list of invalid ones
                    invalid_track_ids.append(track_id)

            console_logger.info("Verified batch %d/%d", (i // batch_size) + 1, (tracks_to_verify_count + batch_size - 1) // batch_size)
            # Optional short sleep between batches
            if i + batch_size < len(track_ids_to_check):
                await asyncio.sleep(0.1)

        # --- Remove Invalid Tracks and Save ---
        if invalid_track_ids:
            console_logger.info("Found %d tracks that no longer exist in Music.app (within the verified subset).", len(invalid_track_ids))
            # Remove invalid tracks from the *original full map* loaded from CSV
            for track_id in invalid_track_ids:
                if track_id in csv_tracks_map:
                    # Remove the track and log its details
                    removed_track_info = csv_tracks_map.pop(track_id)
                    artist_name = removed_track_info.get('artist', 'Unknown Artist')
                    track_name = removed_track_info.get('name', 'Unknown Track')
                    console_logger.info(f"Removing track ID {track_id}: '{artist_name} - {track_name}'")
                    removed_count += 1
                else:
                    # This case indicates a potential logic error if an ID is invalid but not in the map
                    console_logger.warning(f"Attempted to remove track ID {track_id} which was not found in the loaded map. Skipping.")

            # Save the modified full track map back to the CSV
            final_list_to_save = list(csv_tracks_map.values())
            console_logger.info("Final CSV track count after verification: %d", len(final_list_to_save))
            # Use the dedicated save function
            save_to_csv(final_list_to_save, csv_path, console_logger, error_logger)
            console_logger.info("Removed %d invalid tracks from database.", removed_count)
        else:
            console_logger.info("All verified tracks (%d) exist in Music.app.", tracks_to_verify_count)
            removed_count = 0

        # --- Update Last Verification Timestamp ---
        try:
            os.makedirs(os.path.dirname(last_verify_file), exist_ok=True)  # Ensure directory exists
            with open(last_verify_file, "w", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d"))
        except IOError as e:
            error_logger.error(f"Failed to write last verification timestamp: {e}")

        return removed_count

    except (FileNotFoundError, PermissionError, ValueError, TypeError, KeyError, IOError, asyncio.TimeoutError, Exception) as e:
        # Catch broad exceptions during the verification process
        error_logger.error("Critical error during database verification: %s", e, exc_info=True)
        return 0  # Return 0 removed on error


@get_decorator("Update Track")
async def update_track_async(
    track_id: str,
    new_track_name: Optional[str] = None,
    new_album_name: Optional[str] = None,
    new_genre: Optional[str] = None,
    new_year: Optional[str] = None,
) -> bool:
    """
    Updates the track properties asynchronously via AppleScript.

    :param track_id: The ID of the track to update
    :param new_track_name: The new name for the track
    :param new_album_name: The new name for the album
    :param new_genre: The new genre for the track
    :param new_year: The new year for the track
    :return: True if the track was successfully updated, False otherwise
    """
    try:
        if not track_id:
            error_logger.error("No track_id provided.")
            return False

        success = True

        if new_track_name:
            res = await run_applescript_async("update_property.applescript", [track_id, "name", new_track_name])
            if not res or "Success" not in res:
                error_logger.error("Failed to update track name for %s", track_id)
                success = False

        if new_album_name:
            res = await run_applescript_async("update_property.applescript", [track_id, "album", new_album_name])
            if not res or "Success" not in res:
                error_logger.error("Failed to update album name for %s", track_id)
                success = False

        if new_genre:
            max_retries = CONFIG.get("max_retries", 3)
            delay = CONFIG.get("retry_delay_seconds", 2)
            genre_updated = False
            for attempt in range(1, max_retries + 1):
                res = await run_applescript_async("update_property.applescript", [track_id, "genre", new_genre])
                if res and "Success" in res:
                    console_logger.info("Updated genre for %s to %s (attempt %s/%s)", track_id, new_genre, attempt, max_retries)
                    genre_updated = True
                    break
                else:
                    console_logger.warning("Attempt %s/%s failed. Retrying in %ss...", attempt, max_retries, delay)
                    await asyncio.sleep(delay)
            if not genre_updated:
                error_logger.error("Failed to update genre for track %s", track_id)
                success = False
        if new_year:
            res = await run_applescript_async("update_property.applescript", [track_id, "year", new_year])
            if not res or "Success" not in res:
                error_logger.error("Failed to update year for %s", track_id)
                success = False
            else:
                console_logger.info("Updated year for %s to %s", track_id, new_year)
        return success
    except (ValueError, TypeError, asyncio.TimeoutError, IOError) as e:
        error_logger.error("Error in update_track_async for track %s: %s", track_id, e, exc_info=True)
        return False


@get_decorator("Update Genres by Artist")
async def update_genres_by_artist_async(tracks: List[Dict[str, str]], last_run_time: datetime) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Updates the genres for tracks based on the earliest genre of each artist.

    This function harmonizes genres across an artist's tracks by:
    1. Filtering tracks based on the last run time (for incremental updates)
    2. Grouping tracks by artist and determining each artist's dominant genre
    3. Processing tracks in efficient batches for better performance
    4. Updating only tracks that have a different genre than the artist's dominant genre
    5. Recording all changes for reporting and auditing

    The function only updates "subscription" or "downloaded" tracks, skipping tracks with
    other statuses that may not be modifiable.

    :param tracks: A list of dictionaries containing track information
    :param last_run_time: The timestamp of the last incremental update (datetime.min for full runs)
    :return: A tuple containing (updated_tracks, changes_log)
    """
    try:
        csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
        load_track_list(csv_path)
        if last_run_time and last_run_time != datetime.min:
            console_logger.info("Filtering tracks added after %s", last_run_time)
            tracks = [
                track for track in tracks if datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") > last_run_time
            ]
            console_logger.info("Found %d tracks added since last run", len(tracks))

        grouped = group_tracks_by_artist(tracks)
        updated_tracks = []
        changes_log = []

        async def process_track(track: Dict[str, str], dom_genre: str) -> None:
            old_genre = track.get("genre", "Unknown")
            track_id = track.get("id", "")
            status = track.get("trackStatus", "unknown")
            if track_id and old_genre != dom_genre and status in ("subscription", "downloaded"):
                console_logger.info("Updating track %s (Old Genre: %s, New Genre: %s)", track_id, old_genre, dom_genre)
                if await update_track_async(track_id, new_genre=dom_genre):
                    track["genre"] = dom_genre
                    changes_log.append(
                        {
                            "artist": track.get("artist", "Unknown"),
                            "album": track.get("album", "Unknown"),
                            "track_name": track.get("name", "Unknown"),
                            "old_genre": old_genre,
                            "new_genre": dom_genre,
                            "new_track_name": track.get("name", "Unknown"),
                        }
                    )
                    updated_tracks.append(track)
                else:
                    error_logger.error("Failed to update genre for track %s", track_id)

        async def process_tasks_in_batches(tasks: List[asyncio.Task], batch_size: int = 1000) -> None:
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i : i + batch_size]  # noqa: E203
                await asyncio.gather(*batch, return_exceptions=True)

        tasks = []
        for artist, artist_tracks in grouped.items():
            if not artist_tracks:
                continue
            try:
                earliest = min(
                    artist_tracks,
                    key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"),
                )
                dom_genre = earliest.get("genre", "Unknown")
            except (ValueError, TypeError, AttributeError) as e:
                error_logger.error("Error determining earliest track for artist '%s': %s", artist, e, exc_info=True)
                dom_genre = "Unknown"
            console_logger.info("Artist: %s, Dominant Genre: %s (from %d tracks)", artist, dom_genre, len(artist_tracks))
            for track in artist_tracks:
                if track.get("genre", "Unknown") != dom_genre:
                    tasks.append(asyncio.create_task(process_track(track, dom_genre)))
        if tasks:
            await process_tasks_in_batches(tasks, batch_size=1000)
        return updated_tracks, changes_log
    except (ValueError, TypeError, KeyError, AttributeError) as e:
        error_logger.error("Error in update_genres_by_artist_async: %s", e, exc_info=True)
        return [], []


@get_decorator("Fetch Tracks Async")
async def fetch_tracks_async(artist: Optional[str] = None, force_refresh: bool = False) -> List[Dict[str, str]]:
    """
    Fetches tracks asynchronously from Music.app via AppleScript and manages caching.

    :param artist: Optional artist name to filter tracks (None fetches all tracks)
    :param force_refresh: True to force a fetch from Music.app, False to use cached data when available
    :return: A list of track dictionaries, or empty list if fetch fails
    """
    try:
        cache_key = artist if artist else "ALL"
        console_logger.info("Fetching tracks with cache_key='%s', force_refresh=%s", cache_key, force_refresh)

        if not is_music_app_running():
            console_logger.error("Music app is not running! Please start Music.app before running this script.")
            return []

        if not force_refresh and DEPS and DEPS.cache_service:
            cached_tracks = await DEPS.cache_service.get_async(cache_key)
            if cached_tracks:
                console_logger.info("Using cached data for %s, found %d tracks", cache_key, len(cached_tracks))
                return cached_tracks
            else:
                console_logger.info("No cache found for %s, fetching from Music.app", cache_key)
        elif force_refresh:
            console_logger.info("Force refresh requested, ignoring cache for %s", cache_key)
            if DEPS and DEPS.cache_service:
                DEPS.cache_service.invalidate(cache_key)

        script_name = "fetch_tracks.applescript"
        script_args = [artist] if artist else []

        timeout = CONFIG.get("applescript_timeout_seconds", 900)

        if DEPS and DEPS.ap_client:
            console_logger.info("Executing AppleScript via client: %s with args: %s", script_name, script_args)
            raw_data = await DEPS.ap_client.run_script(script_name, script_args, timeout=timeout)
        else:
            error_logger.error("AppleScriptClient is not initialized!")
            return []

        if raw_data:
            lines_count = raw_data.count('\n') + 1
            console_logger.info("AppleScript returned data: %d bytes, approximately %d lines", len(raw_data), lines_count)
        else:
            error_logger.error("Empty response from AppleScript %s. Possible script error.", script_name)
            return []

        tracks = parse_tracks(raw_data)
        console_logger.info("Successfully parsed %d tracks from Music.app", len(tracks))

        if tracks and DEPS and DEPS.cache_service:
            await DEPS.cache_service.set_async(cache_key, tracks)
            console_logger.info("Cached %d tracks with key '%s'", len(tracks), cache_key)

        return tracks
    except (ValueError, TypeError, KeyError, asyncio.TimeoutError, IOError, ConnectionError) as e:
        error_logger.error("Error in fetch_tracks_async: %s", e, exc_info=True)
        return []


async def main_async(args: argparse.Namespace) -> None:
    """
    Main asynchronous execution function that orchestrates all operations based on command-line arguments.

    Handles various operation modes:
    - Default mode: Performs full processing (clean names, update genres, update years)
    - clean_artist: Processes only tracks by a specific artist
    - update_years: Updates album years from external music databases
    - verify_database: Checks and removes tracks that no longer exist in Music.app

    Respects incremental update intervals, test artist limitations,
    manages the dependency container, and implements proper cleanup of resources.
    The 'development.test_artists' list in config acts as a global filter if populated.

    Args:
        args (argparse.Namespace): Command line arguments parsed by argparse.

    Returns:
        None
    """
    start_all = time.time()
    # Use module-level variable without global statement; DEPS is set below
    deps = None
    force_run = args.force  # Store force flag locally

    try:
        console_logger.info("Starting script with arguments: %s", vars(args))

        # Initialize Dependency Container
        deps = DependencyContainer(CONFIG_PATH)
        # Assign to the module-level variable for functions that might need it
        # via global scope (though direct passing is preferred where possible)
        current_module = sys.modules[__name__]
        setattr(current_module, 'DEPS', deps)

        # Initialize pending verification service if not automatically created by DependencyContainer
        if not hasattr(deps, 'pending_verification_service') or deps.pending_verification_service is None:
            # Assuming PendingVerificationService class exists and is imported
            try:
                deps.pending_verification_service = PendingVerificationService(CONFIG, console_logger, error_logger)
                console_logger.info("Initialized PendingVerificationService manually.")
            except NameError:
                error_logger.error("PendingVerificationService class not found. Cannot initialize.")
            except Exception as init_err:
                error_logger.error(f"Failed to initialize PendingVerificationService: {init_err}")

        # Always check if Music.app is running first
        if not is_music_app_running():
            console_logger.error("Music app is not running! Please start Music.app before running this script.")
            return  # Exit if Music app is not running

        console_logger.info("Force run flag: %s", force_run)

        # --- Command-Specific Execution Paths ---

        if hasattr(args, 'command') and args.command == "verify_database":
            # Specific command to verify the database
            console_logger.info("Executing verify_database command...")
            # This command ignores test_artists from config by default,
            # but the function itself will respect it if set.
            # Pass the command's force flag.
            await verify_and_clean_track_database(force=args.force)
            # Note: verify_database now handles its own exit in main(), so this return might not be reached if called from main()
            return

        # Run database verification in force mode
        if force_run:
            console_logger.info("Force flag detected, verifying track database...")
            removed_count = await verify_and_clean_track_database(force=True)
            if removed_count > 0:
                console_logger.info("Database cleanup removed %d non-existent tracks", removed_count)

        if args.command == "clean_artist":
            # Specific command to clean tracks for one artist
            artist = args.artist
            console_logger.info("Running in 'clean_artist' mode for artist='%s'", artist)
            # Fetch only tracks for the specified artist, force refresh if requested
            tracks = await fetch_tracks_async(artist=artist, force_refresh=force_run)

            if not tracks:
                console_logger.warning("No tracks found for artist: %s", artist)
                return

            # Make copies to avoid modifying the original list during iteration
            all_tracks = [track.copy() for track in tracks]
            updated_tracks = []  # Tracks modified by cleaning
            changes_log = []  # Log of cleaning changes

            # Define async helper for cleaning a single track
            async def clean_track(track: Dict[str, str]) -> None:
                orig_name = track.get("name", "")
                orig_album = track.get("album", "")
                track_id = track.get("id", "")
                artist_name = track.get("artist", artist)  # Use artist from track data or command arg

                if not track_id:
                    return  # Skip if no ID

                # Log the cleaning attempt
                # console_logger.info("Cleaning track ID %s - '%s' by '%s' from '%s'", track_id, orig_name, artist_name, orig_album)
                # Perform cleaning
                cleaned_nm, cleaned_al = clean_names(artist_name, orig_name, orig_album)
                # Determine if changes were actually made
                new_tn = cleaned_nm if cleaned_nm != orig_name else None
                new_an = cleaned_al if cleaned_al != orig_album else None

                # If changes exist, attempt to update via AppleScript
                if new_tn or new_an:
                    track_status = track.get("trackStatus", "").lower()
                    if track_status in ("subscription", "downloaded"):
                        if await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
                            # If update successful, modify track dict and log changes
                            if new_tn:
                                track["name"] = cleaned_nm
                            if new_an:
                                track["album"] = cleaned_al
                            changes_log.append(
                                {  # Log the change details
                                    "change_type": "name",
                                    "artist": artist_name,
                                    "album": track.get("album", "Unknown"),
                                    "track_name": orig_name,
                                    "old_track_name": orig_name,
                                    "new_track_name": cleaned_nm,
                                    "old_album_name": orig_album,
                                    "new_album_name": cleaned_al,
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                }
                            )
                            updated_tracks.append(track)
                        else:
                            error_logger.error("Failed to apply cleaning update for track ID %s", track_id)
                    else:
                        console_logger.debug(f"Skipping track update for '{orig_name}' (ID: {track_id}) due to status '{track_status}'")

            # Run cleaning tasks concurrently
            clean_tasks = [asyncio.create_task(clean_track(t)) for t in all_tracks]
            await asyncio.gather(*clean_tasks)

            # --- Post-Processing for clean_artist ---
            # Note: Genre and Year updates are NOT run in clean_artist mode by default.
            # If desired, `update_genres_by_artist_async` and `process_album_years`
            # could be called here, operating on `all_tracks`.

            # Save results if any tracks were updated
            if updated_tracks:
                # Sync changes with the main CSV database
                await sync_track_list_with_current(
                    updated_tracks,
                    get_full_log_path(CONFIG, "csv_output_file", "csv/track_list.csv"),
                    deps.cache_service,
                    console_logger,
                    error_logger,
                    partial_sync=True,  # Partial sync might be appropriate here
                )
                # Save a report of the specific changes made
                save_unified_changes_report(
                    changes_log,
                    get_full_log_path(CONFIG, "changes_report_file", "csv/changes_report.csv"),
                    console_logger,
                    error_logger,
                    force_mode=force_run,  # Use force_mode for console output if needed
                )
                console_logger.info("Processed and cleaned %d tracks for artist: %s", len(updated_tracks), artist)
            else:
                console_logger.info("No cleaning updates needed for artist: %s", artist)

        elif args.command == "update_years":
            # Specific command to update album years
            artist = args.artist  # Optional artist filter from command line
            force_year_update = args.force  # Use force flag specific to this command
            artist_msg = f' for artist={artist}' if artist else ' for all artists'
            console_logger.info(f"Running in 'update_years' mode{artist_msg} (force={force_year_update})")

            # Fetch tracks (filtered by artist if provided, force refresh if requested)
            tracks = await fetch_tracks_async(artist=artist, force_refresh=force_year_update)
            if not tracks:
                console_logger.warning(f"No tracks found{artist_msg}.")
                return

            # Determine whether to use incremental or full processing
            if force_year_update:
                # Force mode - process all tracks
                updated_y, changes_y = await process_album_years(tracks, datetime.min, force=force_year_update)
            else:
                # Incremental mode - only process tracks added since last run
                try:
                    last_run_time = await deps.cache_service.get_last_run_timestamp()
                    console_logger.info("Last incremental run timestamp: %s", last_run_time if last_run_time != datetime.min else "Never or Failed")
                except Exception as e:
                    error_logger.warning(f"Could not get last run timestamp from cache service: {e}. Assuming full run.")
                    last_run_time = datetime.min

                updated_y, changes_y = await process_album_years(tracks, last_run_time, force=force_year_update)

            # Save results if updates occurred
            if updated_y:
                # Sync updated tracks (containing new years) with the main CSV
                await sync_track_list_with_current(
                    updated_y,
                    get_full_log_path(CONFIG, "csv_output_file", "csv/track_list.csv"),
                    deps.cache_service,
                    console_logger,
                    error_logger,
                    partial_sync=True,
                )
                # Save a report of the year changes
                save_changes_report(
                    changes_y,
                    get_full_log_path(CONFIG, "changes_report_file", "csv/changes_report.csv"),
                    console_logger,
                    error_logger,
                    force_mode=force_year_update,
                )
                # Update last run time
                update_last_incremental_run()

        else:
            # --- Default Mode (Incremental or Full Run) ---
            console_logger.info("Running in default mode (incremental or full processing).")
            if not can_run_incremental(force_run=force_run):
                console_logger.info("Incremental interval not reached. Skipping main processing.")
                return  # Exit if interval not met and not forced

            # Determine last run time for incremental filtering
            last_run_time = datetime.min  # Default to beginning of time for full run
            if not force_run:  # Only check last run time if not forcing
                try:
                    last_run_time = await deps.cache_service.get_last_run_timestamp()
                    console_logger.info("Last incremental run timestamp: %s", last_run_time if last_run_time != datetime.min else "Never or Failed")
                except Exception as e:
                    error_logger.warning(f"Could not get last run timestamp from cache service: {e}. Assuming full run.")
                    last_run_time = datetime.min

            # --- Fetch Tracks (respecting test_artists if set globally) ---
            all_tracks: List[Dict[str, str]] = []
            test_artists_global = CONFIG.get("development", {}).get("test_artists", [])

            if test_artists_global:
                # Fetch tracks only for test artists if the list is populated
                console_logger.info(f"Development mode: Fetching tracks only for test_artists: {test_artists_global}")
                fetched_track_map = {}  # Use map to avoid duplicates if artist listed multiple times
                for art in test_artists_global:
                    art_tracks = await fetch_tracks_async(art, force_refresh=force_run)
                    for track in art_tracks:
                        if track.get("id"):
                            fetched_track_map[track["id"]] = track
                all_tracks = list(fetched_track_map.values())
                console_logger.info("Loaded %d tracks total for test artists.", len(all_tracks))
            else:
                # Fetch all tracks if test_artists list is empty
                all_tracks = await fetch_tracks_async(force_refresh=force_run)
                console_logger.info("Loaded %d tracks from Music.app.", len(all_tracks))

            if not all_tracks:
                console_logger.warning("No tracks fetched or found for processing.")
                # Update last run time even if no tracks processed, to respect interval
                if not force_run:
                    update_last_incremental_run()
                return

            # --- Apply Global Test Artists Filter (Redundant if fetch already filtered, but ensures consistency) ---
            # This step is important if fetch_tracks_async() might return more than test_artists
            # or if the default fetch (no artist specified) doesn't internally filter by test_artists.
            if test_artists_global:
                original_count = len(all_tracks)
                all_tracks = [track for track in all_tracks if track.get("artist") in test_artists_global]
                filtered_count = len(all_tracks)
                if original_count != filtered_count:  # Log only if filtering actually removed tracks
                    console_logger.info(f"Applied global test_artists filter. Processing {filtered_count} tracks (filtered from {original_count}).")
                if not all_tracks:
                    console_logger.warning("No tracks remaining after applying global test_artists filter.")
                    if not force_run:
                        update_last_incremental_run()
                    return

            # Make copies AFTER filtering to avoid modifying cached data if applicable
            all_tracks = [track.copy() for track in all_tracks]
            updated_tracks_cleaning = []
            changes_log_cleaning = []

            # --- Step 1: Clean Track/Album Names ---
            console_logger.info("Starting track name cleaning process...")

            async def clean_track_default(track: Dict[str, str]) -> None:  # Renamed helper
                # Same logic as clean_track in clean_artist mode
                orig_name = track.get("name", "")
                orig_album = track.get("album", "")
                track_id = track.get("id", "")
                artist_name = track.get("artist", "Unknown")
                if not track_id:
                    return

                cleaned_nm, cleaned_al = clean_names(artist_name, orig_name, orig_album)
                new_tn = cleaned_nm if cleaned_nm != orig_name else None
                new_an = cleaned_al if cleaned_al != orig_album else None

                if new_tn or new_an:
                    track_status = track.get("trackStatus", "").lower()
                    if track_status in ("subscription", "downloaded"):
                        if await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
                            if new_tn:
                                track["name"] = cleaned_nm
                            if new_an:
                                track["album"] = cleaned_al
                            changes_log_cleaning.append(
                                {
                                    "change_type": "name",
                                    "artist": artist_name,
                                    "album": track.get("album", "Unknown"),
                                    "track_name": orig_name,
                                    "old_track_name": orig_name,
                                    "new_track_name": cleaned_nm,
                                    "old_album_name": orig_album,
                                    "new_album_name": cleaned_al,
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                }
                            )
                            # Use a separate list to track which ones were updated *by cleaning*
                            updated_tracks_cleaning.append(track)  # Appending the modified track dict
                        else:
                            error_logger.error("Failed to apply cleaning update for track ID %s", track_id)
                    else:
                        console_logger.debug(f"Skipping track update for '{orig_name}' (ID: {track_id}) due to status '{track_status}'")

            clean_tasks_default = [asyncio.create_task(clean_track_default(t)) for t in all_tracks]
            await asyncio.gather(*clean_tasks_default)
            if updated_tracks_cleaning:
                console_logger.info("Cleaned %d track/album names.", len(updated_tracks_cleaning))
            else:
                console_logger.info("No track names or album names needed cleaning.")

            # --- Step 2: Update Genres ---
            console_logger.info("Starting genre update process...")
            # Pass the effective last run time (min if forced, actual otherwise)
            effective_last_run = datetime.min if force_run else last_run_time
            # update_genres_by_artist_async modifies tracks in-place and returns lists of *which* tracks were modified and the change log
            updated_g, changes_g = await update_genres_by_artist_async(all_tracks, effective_last_run)
            if updated_g:
                console_logger.info("Updated genres for %d tracks.", len(updated_g))
            else:
                console_logger.info("No genre updates needed based on incremental check or existing genres.")

            # --- Step 3: Update Album Years ---
            console_logger.info("Starting album year update process...")
            # process_album_years also needs last_run_time for incremental updates
            effective_last_run = datetime.min if force_run else last_run_time
            updated_y, changes_y = await process_album_years(all_tracks, effective_last_run, force=force_run)
            if updated_y:
                console_logger.info("Updated years for %d tracks.", len(updated_y))
            else:
                console_logger.info("No year updates needed or year retrieval disabled.")

            # --- Consolidate Changes and Save ---
            # Combine all change logs
            all_changes = changes_log_cleaning + changes_g + changes_y

            # The `all_tracks` list now contains all modifications from cleaning, genres, and years
            if all_changes:  # Check if any changes were logged
                console_logger.info("Consolidating and saving results...")
                await sync_track_list_with_current(
                    all_tracks,  # Pass the fully updated list of tracks
                    get_full_log_path(CONFIG, "csv_output_file", "csv/track_list.csv"),
                    deps.cache_service,
                    console_logger,
                    error_logger,
                    partial_sync=not force_run,  # Use partial sync for incremental, full sync for force
                )
                save_unified_changes_report(
                    all_changes,
                    get_full_log_path(CONFIG, "changes_report_file", "csv/changes_report.csv"),
                    console_logger,
                    error_logger,
                    force_mode=force_run,  # Use force_mode for console output if needed
                )
                console_logger.info("Processing complete. Logged %d changes.", len(all_changes))
            else:
                console_logger.info("No changes detected in this run.")

            # Update last run time only if it was an incremental run (not forced)
            if not force_run:
                update_last_incremental_run()

            # --- Step 4: Verify Pending Albums (after 30 days) ---
            if hasattr(deps, 'pending_verification_service') and deps.pending_verification_service:
                console_logger.info("Checking for albums that need verification...")
                albums_to_verify = deps.pending_verification_service.get_verified_album_keys()

                if albums_to_verify:
                    console_logger.info("Found %d albums that need verification", len(albums_to_verify))
                    verification_updated_tracks = []
                    verification_changes = []

                    # Process each album that needs verification
                    for album_key in albums_to_verify:
                        try:
                            artist, album = album_key.split("|||", 1)
                            console_logger.info("Verifying album '%s - %s' after 30-day interval", artist, album)

                            # Find tracks for this album
                            album_tracks = [t for t in all_tracks if t.get("artist") == artist and t.get("album") == album]

                            if album_tracks:
                                # Force verification regardless of current year values
                                verified_tracks, verified_changes = await process_album_years(album_tracks, datetime.min, force=True)

                                if verified_tracks:
                                    verification_updated_tracks.extend(verified_tracks)
                                    verification_changes.extend(verified_changes)
                                    # Remove from pending verification as we've processed it
                                    deps.pending_verification_service.remove_from_pending(artist, album)
                                    console_logger.info("Successfully verified and updated album '%s - %s'", artist, album)
                                else:
                                    console_logger.warning("Verification didn't result in updates for '%s - %s'", artist, album)
                            else:
                                console_logger.warning("No tracks found for album '%s - %s' needing verification", artist, album)
                                # If no tracks found, still remove from verification list to avoid endless processing
                                deps.pending_verification_service.remove_from_pending(artist, album)
                        except Exception as e:
                            error_logger.error(f"Error verifying album {album_key}: {e}", exc_info=True)

                    # Add verification results to other changes
                    if verification_updated_tracks:
                        console_logger.info("Verified and updated %d tracks from %d albums", len(verification_updated_tracks), len(albums_to_verify))
                        all_tracks.extend([t for t in verification_updated_tracks if t.get("id") not in {x.get("id") for x in all_tracks}])
                        all_changes.extend(verification_changes)
                    else:
                        console_logger.info("No updates resulted from album verification process")
                else:
                    console_logger.info("No albums currently need verification")

    except KeyboardInterrupt:
        # Handle user interruption gracefully
        console_logger.info("Script interrupted by user. Cancelling pending tasks...")
        # Cancel pending asyncio tasks (important for clean shutdown)
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            console_logger.info("Cancelling %d pending tasks.", len(tasks))
            for task in tasks:
                task.cancel()
            # Wait for tasks to finish cancelling
            await asyncio.gather(*tasks, return_exceptions=True)
        console_logger.info("Tasks cancelled.")
        # Re-raise KeyboardInterrupt so the main() finally block still runs
        raise
    except Exception as e:
        # Catch any other unexpected errors during async execution
        error_logger.error("Error during main_async execution: %s", e, exc_info=True)
        # Optionally re-raise or handle specific exceptions differently
        # raise e # Re-raise if needed for main() to catch
    finally:
        # --- Final Cleanup ---
        # Ensure API session is closed
        if deps is not None and hasattr(deps, 'external_api_service') and deps.external_api_service:
            try:
                await deps.external_api_service.close()
            except Exception as close_err:
                error_logger.error(f"Error closing external API service: {close_err}")

        # Force sync album cache to disk before exit
        if deps is not None and hasattr(deps, 'cache_service') and deps.cache_service:
            try:
                await deps.cache_service.sync_cache()
                console_logger.info("Album cache synchronized to disk before exit")
            except Exception as cache_err:
                error_logger.error(f"Error synchronizing cache before exit: {cache_err}")

        end_all = time.time()
        console_logger.info("main_async execution time: %.2f seconds", end_all - start_all)
        # DO NOT call update_last_incremental_run() here if called earlier based on logic

        # --- Step 4: Verify Pending Albums (after 30 days) ---
        if hasattr(deps, 'pending_verification_service') and deps.pending_verification_service:
            console_logger.info("Checking for albums that need verification...")
            albums_to_verify = deps.pending_verification_service.get_verified_album_keys()

            if albums_to_verify:
                console_logger.info("Found %d albums that need verification", len(albums_to_verify))
                verification_updated_tracks = []
                verification_changes = []

                # Process each album that needs verification
                for album_key in albums_to_verify:
                    try:
                        artist, album = album_key.split("|||", 1)
                        console_logger.info("Verifying album '%s - %s' after 30-day interval", artist, album)

                        # Find tracks for this album
                        album_tracks = [t for t in all_tracks if t.get("artist") == artist and t.get("album") == album]

                        if album_tracks:
                            # Force verification regardless of current year values
                            verified_tracks, verified_changes = await process_album_years(album_tracks, datetime.min, force=True)

                            if verified_tracks:
                                verification_updated_tracks.extend(verified_tracks)
                                verification_changes.extend(verified_changes)
                                # Remove from pending verification as we've processed it
                                deps.pending_verification_service.remove_from_pending(artist, album)
                                console_logger.info("Successfully verified and updated album '%s - %s'", artist, album)
                            else:
                                console_logger.warning("Verification didn't result in updates for '%s - %s'", artist, album)
                        else:
                            console_logger.warning("No tracks found for album '%s - %s' needing verification", artist, album)
                            # If no tracks found, still remove from verification list to avoid endless processing
                            deps.pending_verification_service.remove_from_pending(artist, album)
                    except Exception as e:
                        error_logger.error(f"Error verifying album {album_key}: {e}", exc_info=True)

                # Add verification results to other changes
                if verification_updated_tracks:
                    console_logger.info("Verified and updated %d tracks from %d albums", len(verification_updated_tracks), len(albums_to_verify))
                    # Add to shared tracks and changes
                    updated_tracks = []
                    changes_log = []
                    updated_tracks.extend(verification_updated_tracks)
                    changes_log.extend(verification_changes)

                    console_logger.info("Updated tracks and changes logged.")
                else:
                    console_logger.info("No updates resulted from album verification process")
            else:
                console_logger.info("No albums currently need verification")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments using argparse. The script supports subcommands for different modes.
    """
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    parser.add_argument("--force", action="store_true", help="Force run the incremental update")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without applying them")
    subparsers = parser.add_subparsers(dest="command")
    clean_artist_parser = subparsers.add_parser("clean_artist", help="Clean track/album names for a given artist")
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name")
    clean_artist_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")
    update_years_parser = subparsers.add_parser("update_years", help="Update album years from external APIs")
    update_years_parser.add_argument("--artist", help="Artist name (optional)")
    update_years_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")
    verify_db_parser = subparsers.add_parser("verify_database", help="Verify and clean the track database")
    verify_db_parser.add_argument("--force", action="store_true", help="Force database check even if recently performed")
    verify_pending_parser = subparsers.add_parser("verify_pending", help="Verify all pending albums regardless of timeframe")
    verify_pending_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")
    args = parser.parse_args()
    return args


def handle_dry_run() -> None:
    """
    Handle the dry-run mode by running the script without applying any changes.
    """
    try:
        from utils import dry_run
    except ImportError:
        error_logger.error("Dry run module not found. Ensure 'utils/dry_run.py' exists.")
        sys.exit(1)
    console_logger.info("Running in dry-run mode. No changes will be applied.")

    asyncio.run(dry_run.main())
    sys.exit(0)


def main() -> None:
    """
    Main synchronous function to parse arguments and run the async main logic.
    Handles top-level setup, teardown, exception handling, and listener shutdown.
    """
    start_all = time.time()
    args = parse_arguments()
    # Initialize loggers and listener *early*
    _console_logger, _error_logger, _analytics_logger, listener = get_loggers(get_config())

    # --- Add safety check for listener ---
    if listener is None:
        _console_logger.error("Failed to initialize QueueListener for logging. File logs might be missing.")
    if args.dry_run:
        # Run dry run logic
        try:
            asyncio.run(dry_run.main())
        except Exception as dry_run_err:
            _error_logger.error(f"Error during dry run: {dry_run_err}", exc_info=True)
        finally:
            # Stop listener after dry run
            if listener:
                _console_logger.info("Stopping QueueListener (after dry run)...")
                listener.stop()
            # Explicitly close handlers (might be redundant if listener closes them)
            _console_logger.info("Explicitly closing logger handlers (after dry run)...")
            end_all = time.time()
            print(f"\nTotal script execution time (dry run): {end_all - start_all:.2f} seconds")
            sys.exit(0)

    # Explicitly handle verify_database command
    if hasattr(args, 'command') and args.command == "verify_database":
        _console_logger.info(f"Executing verify_database command with force={args.force}")
        local_deps = None
        try:
            local_deps = DependencyContainer(CONFIG_PATH)
            current_module = sys.modules[__name__]
            setattr(current_module, 'DEPS', local_deps)
            # Use existing event loop if available, otherwise create one
            loop = asyncio.get_event_loop_policy().get_event_loop()
            removed = loop.run_until_complete(verify_and_clean_track_database(force=args.force))
            _console_logger.info(f"Database verification completed: removed {removed} invalid tracks")
        except Exception as e:
            _console_logger.error(f"Error during database verification: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Ensure analytics report is generated
            final_deps_verify = getattr(sys.modules[__name__], 'DEPS', None)
            if final_deps_verify and hasattr(final_deps_verify, 'analytics'):
                try:
                    final_deps_verify.analytics.generate_reports(force_mode=args.force)
                except Exception as report_err:
                    _console_logger.error(f"Error generating report after verify_database: {report_err}", exc_info=True)
            # Stop listener before closing handlers
            if listener:
                _console_logger.info("Stopping QueueListener (after verify_database)...")
                listener.stop()
            # Explicitly close handlers
            _console_logger.info("Explicitly closing logger handlers (after verify_database)...")
            # ... (handler closing logic as before) ...
            end_all = time.time()
            print(f"\nTotal executing time for verify_database: {end_all - start_all:.2f} seconds")
            sys.exit(0)

    # Default execution path
    try:
        asyncio.run(main_async(args))  # main_async now uses loggers set up earlier
    except KeyboardInterrupt:
        _console_logger.info("Script interrupted by user (main level).")
    except Exception as exc:
        _console_logger.error("Critical error during main execution: %s", exc, exc_info=True)
        # Try logging to error logger if DEPS was initialized within main_async
        final_deps_main = getattr(sys.modules[__name__], 'DEPS', None)
        if final_deps_main and hasattr(final_deps_main, 'error_logger'):
            final_deps_main.error_logger.error("Critical error during main execution: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        # Analytics report is generated in main_async's finally block

        # --- Stop Listener and Close Handlers ---
        if listener:
            _console_logger.info("Stopping QueueListener...")
            listener.stop()  # Stop the background thread first
        _console_logger.info("Explicitly closing logger handlers...")
        all_handlers = []
        # Close root handlers first
        for handler in logging.root.handlers[:]:
            all_handlers.append(handler)
            logging.root.removeHandler(handler)
        # Close handlers attached to specific loggers
        for name in list(logging.Logger.manager.loggerDict.keys()):
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
        # logging.shutdown() # May no longer be necessary after manual closing and listener stop

        end_all = time.time()
        print(f"\nTotal script execution time: {end_all - start_all:.2f} seconds")


if __name__ == "__main__":
    main()
