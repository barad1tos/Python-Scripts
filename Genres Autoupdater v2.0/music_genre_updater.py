#!/usr/bin/env python3

"""
Music Genre Updater Script v2.0

This script automatically manages your Music.app library by updating genres, cleaning track/album names,
and retrieving album years. It operates asynchronously for improved performance and uses caching to
minimize AppleScript calls.

Key Features:
    - Genre harmonization: Assigns consistent genres to an artist's tracks based on their earliest releases
    - Track/album name cleaning: Removes remaster tags, promotional text, and other clutter from titles
    - Album year retrieval: Fetches and updates year information from external music databases
    - Database verification: Checks and removes tracks that no longer exist in Music.app
    - Incremental processing: Efficiently updates only tracks added since the last run
    - Analytics tracking: Monitors performance and operations with detailed logging
    - Pending verification system: Tracks album years that need additional verification

Commands:
    - Default (no arguments): Run incremental update of genres and clean names
    - clean_artist: Process only tracks by a specific artist
    - update_years: Update album years from external APIs
    - verify_database: Check for and remove tracks that no longer exist in Music.app

Options:
    - --force: Override incremental interval checks and force processing
    - --dry-run: Simulate changes without modifying the Music library
    - --artist: Specify target artist (with clean_artist or update_years commands)

The script uses a dependency container to manage services:
    - AppleScriptClient: For interactions with Music.app
    - CacheService: For storing retrieved data between runs
    - ExternalAPIService: For querying album years from music databases
    - PendingVerificationService: For tracking album years that need verification
    - Analytics: For tracking performance and operation metrics

Configuration (my-config.yaml) includes:
    - music_library_path: Path to the Music library
    - apple_scripts_dir: Directory containing AppleScripts
    - logs_base_dir: Base directory for logs and reports
    - year_retrieval: Settings for album year updates (API limits, batch sizes, adaptive delays)
    - cleaning: Rules for cleaning track and album names (remaster_keywords, album_suffixes_to_remove)
    - incremental_interval_minutes: Time between incremental runs
    - batch_size: Number of tracks to process in parallel
    - verify_database: Settings for database verification
    - exceptions: Special handling rules for specific artists/albums
    - test_artists: Optional list of artists to limit processing for testing

Example usage:
    python3 music_genre_updater.py
    python3 music_genre_updater.py --force
    python3 music_genre_updater.py clean_artist --artist "Metallica"
    python3 music_genre_updater.py update_years
    python3 music_genre_updater.py verify_database --force
"""

import argparse
import asyncio
import logging
import os
import re
import subprocess
import time

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yaml

from services.dependencies_service import DependencyContainer
from services.pending_verification import PendingVerificationService
from utils.analytics import Analytics
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


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load the configuration from the specified YAML file.

    :param config_path: Path to the YAML configuration file.
    :return: A dictionary containing the configuration.
    """
    if not os.path.exists(config_path):
        error_logger.error(f"Config file {config_path} does not exist.")
        sys.exit(1)
    if not os.access(config_path, os.R_OK):
        error_logger.error(f"No read access to config file {config_path}.")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_config() -> Dict[str, Any]:
    """
    Get the current configuration by reloading the YAML file.

    :return: A dictionary containing the configuration.
    """
    return load_config(CONFIG_PATH)


# Initialize loggers
CONFIG = get_config()
console_logger, error_logger, analytics_logger = get_loggers(CONFIG)
analytics_log_file = get_full_log_path(CONFIG, "analytics_log_file", "analytics/analytics.log")

if CONFIG.get("python_settings", {}).get("prevent_bytecode", False):
    import sys

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
    from functools import wraps

    def decorator(func):
        decorated_func = None  # cached decorated function

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal decorated_func
            current_analytics = None
            global DEPS, analytics
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
            error_logger.error(f"Script file does not exist: {script_path}")
            return None

        # Improved logging for tracking execution with arguments
        safe_args = [f"'{arg}'" for arg in args] if args else []
        console_logger.info(f"Running AppleScript: {script_name} with args: {', '.join(safe_args)}")
        console_logger.debug(f"Full script path: {script_path}")

        # Launch script with configurable timeout
        timeout = CONFIG.get("applescript_timeout_seconds", 600)
        try:
            result = await asyncio.wait_for(DEPS.ap_client.run_script(script_name, args), timeout=float(timeout))
        except asyncio.TimeoutError:
            error_logger.error(f"Timeout while executing AppleScript {script_name}")
            return None

        # Analysis of the result
        if result is None:
            console_logger.error(f"AppleScript {script_name} returned None")
            return None

        if not result.strip():
            console_logger.error(f"AppleScript {script_name} returned empty result")
            return None

        # Successful execution
        result_preview = result[:50] + "..." if len(result) > 50 else result
        console_logger.info(f"AppleScript {script_name} executed successfully, got {len(result)} bytes. Preview: {result_preview}")
        return result
    except Exception as e:
        error_logger.error(f"Error running AppleScript {script_name}: {e}", exc_info=True)
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
            error_logger.error(f"Malformed track data: {row}")
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
    except Exception as e:
        error_logger.error(f"Error in determine_dominant_genre_for_artist: {e}", exc_info=True)
        return "Unknown"


@get_decorator("Check Music App Running")
def is_music_app_running() -> bool:
    """
    Checks if the Music.app is currently running.

    :return: True if Music.app is running, False otherwise
    """
    try:
        script = 'tell application "System Events" to (name of processes) contains "Music"'
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
        return result.stdout.strip().lower() == "true"
    except Exception as e:
        error_logger.error(f"Unable to check Music.app status: {e}", exc_info=True)
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
        logging.debug(f"remove_parentheses_with_keywords called with name='{name}' and keywords={keywords}")
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
                content = current_name[start + 1 : end]
                if any(keyword.lower() in content.lower() for keyword in keyword_set):
                    to_remove.add((start, end))

            # If nothing found to remove, we're done
            if not to_remove:
                break

            # Remove brackets (from right to left to maintain indices)
            for start, end in sorted(to_remove, reverse=True):
                current_name = current_name[:start] + current_name[end + 1 :]

        # Clean up multiple spaces
        result = re.sub(r"\s+", " ", current_name).strip()
        logging.debug(f"Cleaned result: '{result}'")
        return result

    except Exception as e:
        error_logger.error(f"Error in remove_parentheses_with_keywords: {e}", exc_info=True)
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
                    console_logger.warning(f"Skipping invalid track ID: '{track_id}'")
            except Exception:
                console_logger.warning(f"Skipping non-numeric track ID: '{track_id}'")

        if not filtered_track_ids:
            console_logger.warning("No valid track IDs left after filtering")
            return False

        console_logger.info(f"Updating year to {year} for {len(filtered_track_ids)} tracks (filtered from {len(track_ids)} initial tracks)")

        successful_updates = 0
        failed_updates = 0
        all_success = True
        batch_size = min(20, CONFIG.get("batch_size", 20))

        for i in range(0, len(filtered_track_ids), batch_size):
            batch = filtered_track_ids[i : i + batch_size]
            tasks = []

            for track_id in batch:
                console_logger.info(f"Adding update task for track ID {track_id} to year {year}")
                tasks.append(run_applescript_async("update_property.applescript", [track_id, "year", year]))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(results):
                track_id = batch[idx] if idx < len(batch) else "unknown"
                if isinstance(result, Exception):
                    error_logger.error(f"Exception updating year for track {track_id}: {result}")
                    failed_updates += 1
                    all_success = False
                elif not result:
                    error_logger.error(f"Empty result updating year for track {track_id}")
                    failed_updates += 1
                    all_success = False
                elif "Success" not in result:
                    if "not found" in result:
                        console_logger.warning(f"Track {track_id} not found, removing from processing")
                    else:
                        error_logger.error(f"Failed to update year for track {track_id}: {result}")
                    failed_updates += 1
                    all_success = False
                else:
                    successful_updates += 1
                    console_logger.info(f"Successfully updated year for track {track_id} to {year}")

            if i + batch_size < len(filtered_track_ids):
                await asyncio.sleep(0.5)

        console_logger.info(f"Year update results: {successful_updates} successful, {failed_updates} failed")
        return successful_updates > 0
    except Exception as e:
        error_logger.error(f"Error in update_album_tracks_bulk_async: {e}", exc_info=True)
        return False


@get_decorator("Process Album Years")
async def process_album_years(tracks: List[Dict[str, str]], force: bool = False) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Manages album year updates for all tracks using external music databases.

    This function orchestrates the album year update process by:
    1. Filtering out prerelease and unavailable tracks
    2. Calling the external API service to retrieve accurate album years
    3. Updating tracks in the Music library with the retrieved years
    4. Persisting changes to CSV files and generating change reports

    The function respects the configuration settings for year retrieval and
    properly handles API rate limits and service connections.

    :param tracks: List of track dictionaries to process for year updates
    :param force: If True, updates years even if they already exist
    :return: Tuple of (updated_tracks, changes_log)
    """
    if not CONFIG.get("year_retrieval", {}).get("enabled", False):
        console_logger.info("Album year updates are disabled in config. Skipping.")
        return [], []

    if not tracks:
        console_logger.info("No tracks provided for year updates.")
        return [], []

    console_logger.info(f"Starting album year updates (force={force})")

    # Skip prerelease tracks
    filtered_tracks = []
    for track in tracks:
        track_status = track.get("trackStatus", "").lower()
        if track_status == "prerelease":
            console_logger.debug(f"Skipping prerelease track: {track.get('name', '')} - {track.get('artist', '')} - {track.get('album', '')}")
            continue
        elif track_status == "no longer available":
            console_logger.debug(f"Skipping unavailable track: {track.get('name', '')} - {track.get('artist', '')} - {track.get('album', '')}")
            continue
        else:
            filtered_tracks.append(track)

    console_logger.info(f"Using {len(filtered_tracks)}/{len(tracks)} tracks (skipped prerelease/unavailable)")
    tracks = filtered_tracks

    try:
        await DEPS.external_api_service.initialize()
        updated_y, changes_y = await update_album_years_async(tracks, force=force)

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
            console_logger.info(f"Updated {len(updated_y)} tracks with album years.")
        else:
            console_logger.info("No tracks needed album year updates.")

        return updated_y, changes_y
    except Exception as e:
        error_logger.error(f"Error in process_album_years: {e}", exc_info=True)
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
    console_logger.info(f"clean_names called with: artist='{artist}', track_name='{track_name}', album_name='{album_name}'")
    exceptions = CONFIG.get("exceptions", {}).get("track_cleaning", [])
    is_exception = any(exc.get("artist", "").lower() == artist.lower() and exc.get("album", "").lower() == album_name.lower() for exc in exceptions)
    if is_exception:
        console_logger.info(f"No cleaning applied due to exceptions for artist '{artist}', album '{album_name}'.")
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
            console_logger.info(f"Removed suffix '{suffix}' from album. New album name: '{cleaned_album}'")
    console_logger.info(f"Original track name: '{original_track}' -> '{cleaned_track}'")
    console_logger.info(f"Original album name: '{original_album}' -> '{cleaned_album}'")
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
        year_logger.info(f"Starting year update for {len(tracks)} tracks")

        # Skip prerelease tracks that cannot be modified
        filtered_tracks = []
        for track in tracks:
            track_status = track.get("trackStatus", "").lower()
            if track_status == "prerelease":
                console_logger.debug(f"Skipping prerelease track: {track.get('name', '')} - {track.get('artist', '')} - {track.get('album', '')}")
                continue
            elif track_status == "no longer available":
                console_logger.debug(f"Skipping unavailable track: {track.get('name', '')} - {track.get('artist', '')} - {track.get('album', '')}")
                continue
            else:
                filtered_tracks.append(track)

        console_logger.info(f"Using {len(filtered_tracks)}/{len(tracks)} tracks (skipped prerelease/unavailable)")
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

        console_logger.info(f"Processing {len(albums)} unique albums")
        year_logger.info(f"Starting year update for {len(albums)} albums")

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
                    console_logger.info(f"Album '{artist} - {album}' is due for verification")

            # Try to get cached year first, unless forcing or verification is due
            year = None
            is_definitive = True

            if force or should_verify:
                # Bypass cache when forcing or verification is due
                year_logger.info(f"Fetching year for '{artist} - {album}' from external API (force={force}, verify={should_verify})")
                year, is_definitive = await DEPS.external_api_service.get_album_year(
                    artist, album, current_library_year, pending_verification_service
                )
                api_call_made = True

                if year and is_definitive:
                    await DEPS.cache_service.store_album_year_in_cache(artist, album, year)
                    year_logger.info(f"Stored year {year} for '{artist} - {album}' in cache")

                    # Remove from pending if we got a definitive result
                    if pending_verification_service:
                        pending_verification_service.remove_from_pending(artist, album)
            else:
                # Try to get cached year first
                year = await DEPS.cache_service.get_album_year_from_cache(artist, album)

                if not year:
                    year_logger.info(f"No cached year found for '{artist} - {album}', fetching from API")
                    year, is_definitive = await DEPS.external_api_service.get_album_year(
                        artist, album, current_library_year, pending_verification_service
                    )
                    api_call_made = True

                    if year and is_definitive:
                        await DEPS.cache_service.store_album_year_in_cache(artist, album, year)
                        year_logger.info(f"Stored year {year} for '{artist} - {album}' in cache")

                    # Remove from pending if we got a definitive result
                    if pending_verification_service and is_definitive:
                        pending_verification_service.remove_from_pending(artist, album)
                else:
                    year_logger.info(f"Using cached year {year} for '{artist} - {album}'")
                    api_call_made = False

            if not year:
                return api_call_made

            # Skip update if year is the same as current library year
            if year == current_library_year:
                year_logger.info(f"Year {year} for '{artist} - {album}' matches current library year, no update needed")
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
                    year_logger.info(f"No tracks need update for '{artist} - {album}' (year: {year}, force={force})")
                    return api_call_made

                year_logger.info(
                    f"Updating {len(tracks_to_update)} of {len(album_tracks)} tracks for '{artist} - {album}' " f"to year {year} (force={force})"
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
                    year_logger.info(f"Successfully updated {len(track_ids_to_update)} tracks for '{artist} - {album}' to year {year}")
                else:
                    year_logger.error(f"Failed to update year for '{artist} - {album}'")

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
            batch = album_items[i : i + batch_size]
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
            console_logger.info(f"Processed batch {batch_num}/{total_batches}")

            # Calculate adaptive delay for next batch if needed
            if i + batch_size < len(album_items):
                api_calls_count = sum(1 for call in api_calls_made if call)

                if adaptive_delay and api_calls_count > 0:
                    # Scale delay based on API usage ratio
                    usage_ratio = api_calls_count / len(batch)
                    adjusted_delay = max(1, round(delay_between_batches * usage_ratio))

                    console_logger.info(f"Adaptive delay: {adjusted_delay}s based on {api_calls_count}/{len(batch)} API calls")
                    await asyncio.sleep(adjusted_delay)
                elif any(api_calls_made):
                    console_logger.info(f"API calls were made. Waiting {delay_between_batches}s before next batch")
                    await asyncio.sleep(delay_between_batches)
                else:
                    console_logger.info("No API calls made. Proceeding to next batch immediately.")

        console_logger.info(f"Album year update complete. Updated {len(updated_tracks)} tracks.")
        year_logger.info(f"Album year update complete. Updated {len(updated_tracks)} tracks.")

        return updated_tracks, changes_log
    except Exception as e:
        error_logger.error(f"Error in update_album_years_async: {e}", exc_info=True)
        return [], []


@get_decorator("Can Run Incremental")
def can_run_incremental(force_run: bool = False) -> bool:
    """
    Checks if the incremental interval has passed since the last run.

    :param force_run: True to force the run, False to check the interval.
    :return: True if the script can run, False otherwise.
    """
    if force_run:
        return True
    last_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["last_incremental_run_file"])
    interval = CONFIG.get("incremental_interval_minutes", 60)
    if not os.path.exists(last_file):
        console_logger.info("Last incremental run file not found. Proceeding.")
        return True
    with open(last_file, "r", encoding="utf-8") as f:
        last_run_str = f.read().strip()
    try:
        last_run_time = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        error_logger.error(f"Invalid date in {last_file}. Proceeding with execution.")
        return True
    next_run_time = last_run_time + timedelta(minutes=interval)
    now = datetime.now()
    if now >= next_run_time:
        return True
    else:
        diff = next_run_time - now
        minutes_remaining = diff.seconds // 60
        console_logger.info(f"Last run: {last_run_time.strftime('%Y-%m-%d %H:%M')}. Next run in {minutes_remaining} mins.")
        return False


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
    Verifies and cleans the track database by removing tracks that no longer exist in Music.app.

    This function loads the CSV track database, checks each track's existence in Music.app,
    and removes any that are no longer present. Key features:
    - Daily verification throttling: Only runs once per day unless force=True
    - Test artist filtering: Optionally limits verification to specified artists
    - Batched processing: Checks tracks in configurable batches for better performance
    - Efficient AppleScript execution: Uses async/await for non-blocking operation
    - Detailed logging: Tracks which items are removed from the database

    :param force: If True, runs verification even if already performed today.
    :return: The number of tracks removed from the database.
    """
    try:
        console_logger.info("Starting database verification process")
        csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])

        if not os.path.exists(csv_path):
            console_logger.info(f"Track database not found at {csv_path}")
            return 0

        csv_tracks = load_track_list(csv_path)

        if not csv_tracks:
            console_logger.info("No tracks in database to verify")
            return 0

        last_verify_file = os.path.join(CONFIG["logs_base_dir"], "last_db_verify.log")

        if not force and os.path.exists(last_verify_file):
            try:
                with open(last_verify_file, "r", encoding="utf-8") as f:
                    last_verify = f.read().strip()
                last_date = datetime.strptime(last_verify, "%Y-%m-%d").date()
                if last_date == datetime.now().date():
                    console_logger.info(f"Database was already verified today ({last_date}). Use force=True to recheck.")
                    return 0
            except Exception:
                pass

        test_artists = CONFIG.get("test_artists", [])
        if test_artists:
            console_logger.info(f"Limiting verification to specified test_artists: {test_artists}")
            csv_tracks = {track_id: track for track_id, track in csv_tracks.items() if track.get("artist", "") in test_artists}
            console_logger.info(f"After filtering, {len(csv_tracks)} tracks will be verified")

        if not csv_tracks:
            console_logger.info("No tracks to verify after filtering by test_artists")
            with open(last_verify_file, "w", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d"))
            return 0

        console_logger.info(f"Verifying {len(csv_tracks)} tracks in database")
        track_ids = list(csv_tracks.keys())

        async def verify_track_exists(track_id: str) -> bool:
            script = f'''
            tell application "Music"
                try
                    set trackRef to (first track of library playlist 1 whose id is {track_id})
                    return "exists"
                on error
                    return "not_found"
                end try
            end tell
            '''
            try:
                result = await DEPS.ap_client.run_script_code(script)
                return result and "exists" in result
            except Exception:
                return False

        batch_size = CONFIG.get("verify_database", {}).get("batch_size", 10)
        invalid_track_ids = []

        console_logger.info(f"Checking tracks in batches of {batch_size}...")

        for i in range(0, len(track_ids), batch_size):
            batch = track_ids[i : i + batch_size]
            tasks = [verify_track_exists(track_id) for track_id in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, result in enumerate(results):
                track_id = batch[idx] if idx < len(batch) else None
                if track_id and (result is False or isinstance(result, Exception)):
                    invalid_track_ids.append(track_id)
            console_logger.info(f"Verified batch {i//batch_size + 1}/{(len(track_ids) + batch_size - 1)//batch_size}")
            if i + batch_size < len(track_ids):
                await asyncio.sleep(0.2)

        if invalid_track_ids:
            console_logger.info(f"Found {len(invalid_track_ids)} tracks that no longer exist in Music.app")
            for track_id in invalid_track_ids:
                if track_id in csv_tracks:
                    track_info = csv_tracks[track_id]
                    console_logger.info(f"Removing track {track_id}: {track_info.get('artist', '')} - {track_info.get('name', '')}")
                    del csv_tracks[track_id]
            final_list = list(csv_tracks.values())
            console_logger.info(f"Final CSV track count after sync: {len(final_list)}")
            fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded", "trackStatus", "old_year", "new_year"]
            save_to_csv(final_list, csv_path, console_logger, error_logger)
            console_logger.info(f"Removed {len(invalid_track_ids)} invalid tracks from database")
        else:
            console_logger.info("All tracks in database exist in Music.app")

        with open(last_verify_file, "w", encoding="utf-8") as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))

        return len(invalid_track_ids)

    except Exception as e:
        error_logger.error(f"Error during database verification: {e}", exc_info=True)
        return 0


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
                error_logger.error(f"Failed to update track name for {track_id}")
                success = False

        if new_album_name:
            res = await run_applescript_async("update_property.applescript", [track_id, "album", new_album_name])
            if not res or "Success" not in res:
                error_logger.error(f"Failed to update album name for {track_id}")
                success = False

        if new_genre:
            max_retries = CONFIG.get("max_retries", 3)
            delay = CONFIG.get("retry_delay_seconds", 2)
            genre_updated = False
            for attempt in range(1, max_retries + 1):
                res = await run_applescript_async("update_property.applescript", [track_id, "genre", new_genre])
                if res and "Success" in res:
                    console_logger.info(f"Updated genre for {track_id} to {new_genre} (attempt {attempt}/{max_retries})")
                    genre_updated = True
                    break
                else:
                    console_logger.warning(f"Attempt {attempt}/{max_retries} failed. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
            if not genre_updated:
                error_logger.error(f"Failed to update genre for track {track_id}")
                success = False
        if new_year:
            res = await run_applescript_async("update_property.applescript", [track_id, "year", new_year])
            if not res or "Success" not in res:
                error_logger.error(f"Failed to update year for {track_id}")
                success = False
            else:
                console_logger.info(f"Updated year for {track_id} to {new_year}")
        return success
    except Exception as e:
        error_logger.error(f"Error in update_track_async for track {track_id}: {e}", exc_info=True)
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
            console_logger.info(f"Filtering tracks added after {last_run_time}")
            tracks = [
                track for track in tracks if datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") > last_run_time
            ]
            console_logger.info(f"Found {len(tracks)} tracks added since last run")

        grouped = group_tracks_by_artist(tracks)
        updated_tracks = []
        changes_log = []

        async def process_track(track: Dict[str, str], dom_genre: str) -> None:
            old_genre = track.get("genre", "Unknown")
            track_id = track.get("id", "")
            status = track.get("trackStatus", "unknown")
            if track_id and old_genre != dom_genre and status in ("subscription", "downloaded"):
                console_logger.info(f"Updating track {track_id} (Old Genre: {old_genre}, New Genre: {dom_genre})")
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
                    error_logger.error(f"Failed to update genre for track {track_id}")

        async def process_tasks_in_batches(tasks: List[asyncio.Task], batch_size: int = 1000) -> None:
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i : i + batch_size]
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
            except Exception as e:
                error_logger.error(f"Error determining earliest track for artist '{artist}': {e}", exc_info=True)
                dom_genre = "Unknown"
            console_logger.info(f"Artist: {artist}, Dominant Genre: {dom_genre} (from {len(artist_tracks)} tracks)")
            for track in artist_tracks:
                if track.get("genre", "Unknown") != dom_genre:
                    tasks.append(asyncio.create_task(process_track(track, dom_genre)))
        if tasks:
            await process_tasks_in_batches(tasks, batch_size=1000)
        return updated_tracks, changes_log
    except Exception as e:
        error_logger.error(f"Error in update_genres_by_artist_async: {e}", exc_info=True)
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
        console_logger.info(f"Fetching tracks with cache_key='{cache_key}', force_refresh={force_refresh}")

        if not is_music_app_running():
            console_logger.error("Music app is not running! Please start Music.app before running this script.")
            return []

        if not force_refresh and DEPS and DEPS.cache_service:
            cached_tracks = await DEPS.cache_service.get_async(cache_key)
            if cached_tracks:
                console_logger.info(f"Using cached data for {cache_key}, found {len(cached_tracks)} tracks")
                return cached_tracks
            else:
                console_logger.info(f"No cache found for {cache_key}, fetching from Music.app")
        elif force_refresh:
            console_logger.info(f"Force refresh requested, ignoring cache for {cache_key}")
            if DEPS and DEPS.cache_service:
                DEPS.cache_service.invalidate(cache_key)

        script_name = "fetch_tracks.applescript"
        script_args = [artist] if artist else []

        timeout = CONFIG.get("applescript_timeout_seconds", 900)

        if DEPS and DEPS.ap_client:
            console_logger.info(f"Executing AppleScript via client: {script_name} with args: {script_args}")
            raw_data = await DEPS.ap_client.run_script(script_name, script_args, timeout=timeout)
        else:
            error_logger.error("AppleScriptClient is not initialized!")
            return []

        if raw_data:
            lines_count = raw_data.count('\n') + 1
            console_logger.info(f"AppleScript returned data: {len(raw_data)} bytes, approximately {lines_count} lines")
        else:
            error_logger.error(f"Empty response from AppleScript {script_name}. Possible script error.")
            return []

        tracks = parse_tracks(raw_data)
        console_logger.info(f"Successfully parsed {len(tracks)} tracks from Music.app")

        if tracks and DEPS and DEPS.cache_service:
            await DEPS.cache_service.set_async(cache_key, tracks)
            console_logger.info(f"Cached {len(tracks)} tracks with key '{cache_key}'")

        return tracks
    except Exception as e:
        error_logger.error(f"Error in fetch_tracks_async: {e}", exc_info=True)
        return []


async def main_async(args: argparse.Namespace) -> None:
    """
    Main asynchronous execution function that orchestrates all operations based on command-line arguments.

    This function handles various operation modes:
    - Default mode: Performs full processing (clean names, update genres, update years)
    - clean_artist: Processes only tracks by a specific artist
    - update_years: Updates album years from external music databases
    - verify_database: Checks and removes tracks that no longer exist in Music.app

    The function respects incremental update intervals, test artist limitations,
    manages the dependency container, and implements proper cleanup of resources.

    Args:
        args: Command line arguments parsed by argparse

    Returns:
        None
    """
    start_all = time.time()
    try:
        console_logger.info(f"Starting script with arguments: {vars(args)}")

        global DEPS
        DEPS = DependencyContainer(CONFIG_PATH)

        # Initialize pending verification service if not automatically created
        if not hasattr(DEPS, 'pending_verification_service') or DEPS.pending_verification_service is None:
            DEPS.pending_verification_service = PendingVerificationService(CONFIG, console_logger, error_logger)
            console_logger.info("Initialized pending verification service manually")

        # Always check if Music.app is running first
        if not is_music_app_running():
            console_logger.error("Music app is not running! Please start Music.app before running this script.")
            return

        force_run = args.force
        console_logger.info(f"Force run flag: {force_run}")

        if args.force:
            last_run_time = datetime.min  # Set to datetime.min when force=True to process all tracks
        else:
            last_run_time = await DEPS.cache_service.get_last_run_time()

        if not can_run_incremental(force_run=force_run):
            console_logger.info("Incremental interval has not passed. Exiting.")
            return

        # Get last run time - IMPORTANT: set to datetime.min when force=True to process all tracks
        last_run_time = datetime.min if force_run else await DEPS.cache_service.get_last_run_time()

        if hasattr(args, 'command') and args.command == "verify_database":
            console_logger.info("Executing verify_database command")
            await verify_and_clean_track_database(force=args.force)
            return

        # Run database verification in force mode
        if force_run:
            console_logger.info("Force flag detected, verifying track database...")
            removed_count = await verify_and_clean_track_database(force=True)
            if removed_count > 0:
                console_logger.info(f"Database cleanup removed {removed_count} non-existent tracks")

        # Command-specific execution paths
        if args.command == "clean_artist":
            artist = args.artist
            console_logger.info(f"Running in 'clean_artist' mode for artist='{artist}'")
            tracks = await fetch_tracks_async(artist=artist, force_refresh=force_run)

            if not tracks:
                console_logger.warning(f"No tracks found for artist: {artist}")
                return

            # Create copies of tracks before modification to avoid reference issues
            all_tracks = [track.copy() for track in tracks]
            updated_tracks = []
            changes_log = []

            # Clean track names
            async def clean_track(track: Dict[str, str]) -> None:
                orig_name = track.get("name", "")
                orig_album = track.get("album", "")
                track_id = track.get("id", "")
                artist_name = track.get("artist", artist)
                console_logger.info(f"Cleaning track ID {track_id} - '{orig_name}' by '{artist_name}' from '{orig_album}'")
                cleaned_nm, cleaned_al = clean_names(artist_name, orig_name, orig_album)
                new_tn = cleaned_nm if cleaned_nm != orig_name else None
                new_an = cleaned_al if cleaned_al != orig_album else None
                if new_tn or new_an:
                    if await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
                        if new_tn:
                            track["name"] = cleaned_nm
                        if new_an:
                            track["album"] = cleaned_al
                        changes_log.append(
                            {
                                "change_type": "name",
                                "artist": artist_name,
                                "album": track.get("album", "Unknown"),
                                "track_name": orig_name,
                                "old_genre": track.get("genre", "Unknown"),
                                "new_genre": track.get("genre", "Unknown"),
                                "new_track_name": track.get("name", "Unknown"),
                                "old_track_name": orig_name,
                                "old_album_name": orig_album,
                                "new_album_name": cleaned_al,
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )
                        updated_tracks.append(track)
                    else:
                        error_logger.error(f"Failed to update track ID {track_id}")
                else:
                    console_logger.info(f"No cleaning needed for track '{orig_name}'")

            # First pass: clean track names
            clean_tasks = [asyncio.create_task(clean_track(t)) for t in all_tracks]
            await asyncio.gather(*clean_tasks)

            # Second pass: update genres (we work on all tracks even in clean_artist mode)
            # This ensures consistent genres across all the artist's tracks
            console_logger.info(f"Updating genres for artist: {artist}")
            updated_g, changes_g = await update_genres_by_artist_async(all_tracks, datetime.min)

            # Third pass: process album years
            console_logger.info(f"Updating album years for artist: {artist}")
            updated_y, changes_y = await process_album_years(all_tracks, force=force_run)

            # Combine all changes
            all_updated = updated_tracks + updated_g + updated_y
            all_changes = changes_log + changes_g + changes_y

            # Save results
            if all_updated:
                await sync_track_list_with_current(
                    all_updated,
                    os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                    DEPS.cache_service,
                    console_logger,
                    error_logger,
                    partial_sync=True,
                )
                save_unified_changes_report(
                    all_changes,
                    os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]),
                    console_logger,
                    error_logger,
                    force_mode=force_run,
                )
                console_logger.info(f"Processed and updated {len(all_updated)} tracks for artist: {artist}")
            else:
                console_logger.info(f"No updates needed for artist: {artist}")

        elif args.command == "update_years":
            # Year update mode
            artist = args.artist
            force_run = args.force
            console_logger.info(f"Running in 'update_years' mode{f' for artist={artist}' if artist else ''}")

            tracks = await fetch_tracks_async(artist=artist, force_refresh=force_run)
            if not tracks:
                console_logger.warning(f"No tracks found{f' for artist: {artist}' if artist else ''}.")
                return

            # Process album years with force flag
            updated_y, changes_y = await process_album_years(tracks, force=force_run)

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
                    force_mode=force_run,
                )
                console_logger.info(f"Updated years for {len(updated_y)} tracks")

            update_last_incremental_run()

        else:
            # Default mode: full processing
            if not can_run_incremental(force_run=force_run):
                console_logger.info("Incremental interval not reached. Skipping.")
                return

            # Get last run time
            last_run_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["last_incremental_run_file"])
            last_run_time_str = None

            if os.path.exists(last_run_file):
                try:
                    with open(last_run_file, "r", encoding="utf-8") as f:
                        last_run_time_str = f.read().strip()
                except Exception as e:
                    error_logger.error(f"Failed to read {last_run_file}: {e}", exc_info=True)

            if last_run_time_str:
                try:
                    last_run_time = datetime.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    error_logger.error(f"Invalid date format in {last_run_file}. Resetting to full run.")
                    last_run_time = datetime.min
            else:
                console_logger.info("No valid last run timestamp found. Considering all tracks as existing.")
                last_run_time = datetime.min

            # Load tracks from Music app
            test_artists = CONFIG.get("test_artists", [])
            if test_artists:
                all_tracks = []
                for art in test_artists:
                    art_tracks = await fetch_tracks_async(art, force_refresh=force_run)
                    all_tracks.extend(art_tracks)
                console_logger.info(f"Loaded {len(all_tracks)} tracks from test artists: {', '.join(test_artists)}")
            else:
                all_tracks = await fetch_tracks_async(force_refresh=force_run)
                console_logger.info(f"Loaded {len(all_tracks)} tracks from Music.app")

            if not all_tracks:
                console_logger.warning("No tracks fetched.")
                return

            # Make copies to avoid reference issues
            all_tracks = [track.copy() for track in all_tracks]
            updated_tracks = []
            changes_log = []

            # Clean track names
            async def clean_track(track: Dict[str, str]) -> None:
                orig_name = track.get("name", "")
                orig_album = track.get("album", "")
                track_id = track.get("id", "")
                artist_name = track.get("artist", "Unknown")

                console_logger.info(f"Cleaning track ID {track_id} - '{orig_name}' (global cleaning)")
                cleaned_nm, cleaned_al = clean_names(artist_name, orig_name, orig_album)
                new_tn = cleaned_nm if cleaned_nm != orig_name else None
                new_an = cleaned_al if cleaned_al != orig_album else None

                if new_tn or new_an:
                    if await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
                        if new_tn:
                            track["name"] = cleaned_nm
                        if new_an:
                            track["album"] = cleaned_al
                        changes_log.append(
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
                        updated_tracks.append(track)
                    else:
                        error_logger.error(f"Failed to update track ID {track_id}")
                else:
                    console_logger.debug(f"No cleaning needed for track '{orig_name}'")

            # First step: clean track names
            console_logger.info("Starting track name cleaning process")
            clean_tasks = [asyncio.create_task(clean_track(t)) for t in all_tracks]
            await asyncio.gather(*clean_tasks)

            if updated_tracks:
                console_logger.info(f"Cleaned {len(updated_tracks)} track/album names")
            else:
                console_logger.info("No track names or album names needed cleaning in this run.")

            # Second step: update genres
            console_logger.info("Starting genre update process")
            updated_g, changes_g = await update_genres_by_artist_async(all_tracks, last_run_time if not force_run else datetime.min)

            if updated_g:
                console_logger.info(f"Updated genres for {len(updated_g)} tracks")
            else:
                console_logger.info("No genre updates needed")

            # Third step: update album years
            console_logger.info("Starting album year update process")
            updated_y, changes_y = await process_album_years(all_tracks, force=force_run)

            if updated_y:
                console_logger.info(f"Updated years for {len(updated_y)} tracks")
            else:
                console_logger.info("No year updates needed")

            # Combine all changes
            all_updated = updated_tracks + updated_g + updated_y
            all_changes = changes_log + changes_g + changes_y

            # Save results
            if all_updated:
                await sync_track_list_with_current(
                    all_updated,
                    os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                    DEPS.cache_service,
                    console_logger,
                    error_logger,
                    partial_sync=not force_run,  # Full sync in force mode
                )
                save_unified_changes_report(
                    all_changes,
                    os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]),
                    console_logger,
                    error_logger,
                    force_mode=force_run,
                )
                console_logger.info(f"Processed and updated {len(all_updated)} tracks total")
            else:
                console_logger.info("No updates needed in this run")

            update_last_incremental_run()

    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user. Cancelling pending tasks…")
        current_task = asyncio.current_task()
        pending = [t for t in asyncio.all_tasks() if t is not current_task and not t.done()]
        console_logger.info(f"Cancelling {len(pending)} pending tasks.")
        for task in pending:
            try:
                task.cancel()
            except Exception as cancel_e:
                console_logger.error(f"Error cancelling task {task}: {cancel_e}", exc_info=True)
        try:
            await asyncio.gather(*pending, return_exceptions=True)
        except Exception as e:
            console_logger.error(f"Error while awaiting cancelled tasks: {e}", exc_info=True)
        raise
    except Exception as e:
        error_logger.error(f"Error during execution: {e}", exc_info=True)
    finally:
        if DEPS is not None and hasattr(DEPS, 'external_api_service'):
            await DEPS.external_api_service.close()
        if DEPS and hasattr(DEPS, 'analytics'):
            DEPS.analytics.generate_reports(force_mode=force_run)
    end_all = time.time()
    console_logger.info(f"Total executing time: {end_all - start_all:.2f} seconds")


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
    Main function to run the script.
    """
    start_all = time.time()
    args = parse_arguments()

    if args.dry_run:
        handle_dry_run()
        sys.exit(0)

    if hasattr(args, 'command') and args.command == "verify_database":
        print(f"Executing verify_database command with force={args.force}")
        from services.dependencies_service import DependencyContainer

        global DEPS
        DEPS = DependencyContainer(CONFIG_PATH)
        csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
        print(f"Checking database at: {csv_path}")
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at {csv_path}")
            sys.exit(1)
        try:
            loop = asyncio.get_event_loop()
            removed = loop.run_until_complete(verify_and_clean_track_database(force=args.force))
            print(f"Database verification completed: removed {removed} invalid tracks")
        except Exception as e:
            print(f"Error during verification: {e}")
            sys.exit(1)
        if DEPS and hasattr(DEPS, 'analytics'):
            DEPS.analytics.generate_reports()
        end_all = time.time()
        print(f"Total executing time: {end_all - start_all:.2f} seconds")
        sys.exit(0)

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user.")
    except Exception as exc:
        error_logger.error(f"An unexpected error occurred: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        if DEPS is not None and hasattr(DEPS, 'analytics'):
            DEPS.analytics.generate_reports(force_mode=args.force)
    end_all = time.time()
    console_logger.info(f"Total executing time: {end_all - start_all:.2f} seconds")


if __name__ == "__main__":
    main()
