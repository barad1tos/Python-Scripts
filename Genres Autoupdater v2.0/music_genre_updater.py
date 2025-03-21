#!/usr/bin/env python3

"""
Music Genre Updater Script v2.0

This script automatically manages your Music.app library by updating genres, cleaning track/album names,
and retrieving album years. It operates asynchronously for improved performance and uses caching to 
minimize AppleScript calls.

Key Features:
    - Genre harmonization: Assigns consistent genres to an artist's tracks based on their earliest releases
    - Track/album name cleaning: Removes remaster tags, promotional text, and other clutter
    - Album year retrieval: Fetches and updates year information from external music databases 
    - Database verification: Checks and removes tracks that no longer exist in Music.app
    - Incremental processing: Efficiently updates only tracks added since the last run

Commands:
    - Default (no arguments): Run incremental update of genres and clean names
    - clean_artist: Process only tracks by a specific artist
    - update_years: Update album years from external APIs 
    - verify_database: Check for and remove tracks that no longer exist in Music.app

Options:
    - --force: Override incremental interval checks and force processing
    - --dry-run: Simulate changes without modifying the Music library

The script uses a dependency container to manage services:
    - AppleScriptClient: For interactions with Music.app
    - CacheService: For storing retrieved data between runs
    - ExternalAPIService: For querying album years from music databases
    - Analytics: For tracking performance and operations

Configuration (my-config.yaml) includes:
    - music_library_path: Path to the Music library
    - apple_scripts_dir: Directory containing AppleScripts
    - logs_base_dir: Base directory for logs and reports
    - year_retrieval: Settings for album year updates (API limits, batch sizes)
    - cleaning: Rules for cleaning track and album names
    - incremental_interval_minutes: Time between incremental runs
    - batch_size: Number of tracks to process in parallel

Example usage:
    python3 music_genre_updater.py
    python3 music_genre_updater.py --force
    python3 music_genre_updater.py clean_artist --artist "Metallica"
    python3 music_genre_updater.py update_years
    python3 music_genre_updater.py verify_database
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
from typing import Any, Dict, List, Optional, Tuple

import yaml

from services.dependencies_service import DependencyContainer
from utils.analytics import Analytics
from utils.logger import get_full_log_path, get_loggers
from utils.reports import load_track_list, save_changes_report, save_to_csv, sync_track_list_with_current

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
    Creates a decorator that tracks function execution for the given event type.
    
    If the DEPS container is initialized with analytics, it synchronizes the 
    analytics events between the global and DEPS objects to ensure consistent tracking.

    :param event_type: The type of event to track in analytics
    :return: A decorator function that wraps the target function for analytics tracking
    """
    global analytics
    
    # If DEPS is initialized and has analytics, synchronize it with the global
    if DEPS and hasattr(DEPS, 'analytics'):
        # Synchronize events between the global and DEPS objects
        if analytics is not DEPS.analytics:
            # Add events from global analytics to DEPS.analytics
            if hasattr(analytics, 'events'):
                DEPS.analytics.events.extend(analytics.events)
                analytics.events.clear() # Clean up to avoid duplication
                
            # Combining counters
            for fn, count in analytics.call_counts.items():
                DEPS.analytics.call_counts[fn] = DEPS.analytics.call_counts.get(fn, 0) + count
            
            for fn, count in analytics.success_counts.items():
                DEPS.analytics.success_counts[fn] = DEPS.analytics.success_counts.get(fn, 0) + count
                
            for fn, overhead in analytics.decorator_overhead.items():
                DEPS.analytics.decorator_overhead[fn] = DEPS.analytics.decorator_overhead.get(fn, 0) + overhead
            
            # Replace the global object with DEPS.analytics
            analytics = DEPS.analytics
        
        return DEPS.analytics.decorator(event_type)
    # Fallback to global analytics for backward compatibility
    return analytics.decorator(event_type)

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
            
        console_logger.info(f"Running AppleScript: {script_name} with args: {args}")
        console_logger.debug(f"Full script path: {script_path}")
        
        # Launch script with 600 second timeout
        try:
            result = await asyncio.wait_for(
                DEPS.ap_client.run_script(script_name, args),
                timeout=600.0
            )
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
        console_logger.info(f"AppleScript {script_name} executed successfully, got {len(result)} bytes")
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
    Determine the dominant genre for an artist based on the earliest album and track.
    
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
            track_date = datetime.strptime(
                track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"
            )
            if album not in album_earliest:
                album_earliest[album] = track
            else:
                existing_date = datetime.strptime(
                    album_earliest[album].get("dateAdded", "1900-01-01 00:00:00"),
                    "%Y-%m-%d %H:%M:%S"
                )
                if track_date < existing_date:
                    album_earliest[album] = track
        earliest_album = min(
            album_earliest.values(),
            key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
        ).get("album", "Unknown")
        earliest_album_tracks = [track for track in artist_tracks if track.get("album") == earliest_album]
        earliest_track = min(
            earliest_album_tracks,
            key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
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
    
    Algorithm:
    1. Finds all pairs of brackets (using the stack to track opening brackets)
    2. For each pair, checks whether the text inside contains keywords
    3. Removes brackets with keywords (from the end to the beginning to preserve indexes)
    4. Repeats the process until all keyword brackets are found and removed
    
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
                content = current_name[start + 1:end]
                if any(keyword.lower() in content.lower() for keyword in keyword_set):
                    to_remove.add((start, end))
            
            # If nothing found to remove, we're done
            if not to_remove:
                break
                
            # Remove brackets (from right to left to maintain indices)
            for start, end in sorted(to_remove, reverse=True):
                current_name = current_name[:start] + current_name[end + 1:]
        
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
    Updates the year property for multiple tracks in bulk using batch processing.
    
    The process:
    1. Validates and filters track IDs to ensure they are valid numeric identifiers
    2. Processes tracks in manageable batches (controlled by CONFIG batch_size)
    3. Makes concurrent AppleScript calls to update each track's year property
    4. Tracks successful and failed updates for reporting
    5. Implements small pauses between batches to avoid overloading Music.app
    
    :param track_ids: List of track IDs to update
    :param year: The year value to set for all tracks in the batch
    :return: True if at least one track was successfully updated, False otherwise
    """
    try:
        if not track_ids or not year:
            console_logger.warning("No track IDs or year provided for bulk update.")
            return False
            
        # Validation of track IDs and year before processing
        filtered_track_ids = []
        for track_id in track_ids:
            # Check that ID is a number
            try:
                # Verify that ID is a number, not the year or other value
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
        
        # Track successful updates
        successful_updates = 0
        failed_updates = 0
        all_success = True
        batch_size = min(20, CONFIG.get("batch_size", 20))  # No more than 20 at a time
        
        # Process tracks in batches
        for i in range(0, len(filtered_track_ids), batch_size):
            batch = filtered_track_ids[i:i+batch_size]
            tasks = []
            
            for track_id in batch:
                tasks.append(run_applescript_async("update_property.applescript", [track_id, "year", year]))
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Analyze results
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
                    # Check for "Track not found" or other errors
                    if "not found" in result:
                        console_logger.warning(f"Track {track_id} not found, removing from processing")
                    else:
                        error_logger.error(f"Failed to update year for track {track_id}: {result}")
                    failed_updates += 1
                    all_success = False
                else:
                    successful_updates += 1
                    
            # Small pause between batches to avoid overloading Music.app
            if i + batch_size < len(filtered_track_ids):
                await asyncio.sleep(0.5)
        
        # Log update results        
        console_logger.info(f"Year update results: {successful_updates} successful, {failed_updates} failed")
        return successful_updates > 0  # Return True if at least one update was successful
    except Exception as e:
        error_logger.error(f"Error in update_album_tracks_bulk_async: {e}", exc_info=True)
        return False

@get_decorator("Process Album Years")
async def process_album_years(tracks: List[Dict[str, str]], force: bool = False) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Centralized function for processing album year updates.
    Uses CSV cache to retain years between runs.
    
    :param tracks: The list of tracks to process
    :param force: If True, forces an update even if the year is already cached
    :return: A tuple of updated tracks and changes log
    """
    if not CONFIG.get("year_retrieval", {}).get("enabled", False):
        console_logger.info("Album year updates are disabled in config. Skipping.")
        return [], []
    
    if not tracks:
        console_logger.info("No tracks provided for year updates.")
        return [], []
    
    console_logger.info(f"Starting album year updates (force={force})")
    try:
        # Initialization of external API service
        await DEPS.external_api_service.initialize()
        
        # The main update of years
        updated_y, changes_y = await update_album_years_async(tracks, force=force)
        
        if updated_y:
            # Synchronization of updated data with CSV
            await sync_track_list_with_current(
                updated_y,
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                DEPS.cache_service,
                console_logger,
                error_logger,
                partial_sync=True
            )
            
            # Save a change report
            save_changes_report(
                changes_y, 
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"].get("year_changes_report_file", "reports/year_changes_report.csv")), 
                console_logger, 
                error_logger
            )
            
            console_logger.info(f"Updated {len(updated_y)} tracks with album years.")
        else:
            console_logger.info("No tracks needed album year updates.")
        
        return updated_y, changes_y
    except Exception as e:
        error_logger.error(f"Error in process_album_years: {e}", exc_info=True)
        return [], []
    finally:
        # Always close the service to avoid leakage of resources
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
    is_exception = any(
        exc.get("artist", "").lower() == artist.lower() and exc.get("album", "").lower() == album_name.lower()
        for exc in exceptions
    )
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
            cleaned_album = cleaned_album[:-len(suffix)].strip()
            console_logger.info(f"Removed suffix '{suffix}' from album. New album name: '{cleaned_album}'")
    console_logger.info(f"Original track name: '{original_track}' -> '{cleaned_track}'")
    console_logger.info(f"Original album name: '{original_album}' -> '{cleaned_album}'")
    return cleaned_track, cleaned_album

@get_decorator("Update Album Years")
async def update_album_years_async(tracks: List[Dict[str, str]], force: bool = False) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Updates the album years for a list of tracks using external APIs.

    :param tracks: The list of tracks to update
    :param force: If True, forces an update even if the year is already cached
    :return: A tuple of updated tracks and changes log
    """
    try:
        # Setting up logic
        year_changes_log_file = os.path.join(
            CONFIG["logs_base_dir"],
            CONFIG["logging"].get("year_changes_log_file", "main/year_changes.log")
        )
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(year_changes_log_file), exist_ok=True)
        
        # Create a year-specific logger
        year_logger = logging.getLogger("year_updates")
        year_logger.setLevel(logging.INFO)
        
        # Add a file handler for the year changes log
        if not year_logger.handlers:
            fh = logging.FileHandler(year_changes_log_file)
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            year_logger.addHandler(fh)
        
        console_logger.info("Starting album year update process")
        
        # Group tracks by album to minimize API requests
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
        
        # Lists to track changes
        updated_tracks = []
        changes_log = []

        # Process albums in parallel
        async def process_album(album_data):
            """
            Processes the album data to update the year for all its tracks.

            :param album_data: The album data dictionary
            :return: True if an API call was made, False otherwise
            """
            nonlocal updated_tracks, changes_log
            artist = album_data["artist"]
            album = album_data["album"]
            album_tracks = album_data["tracks"]
            
            # Skip if no tracks in album
            if not album_tracks:
                return False  # No API call if no tracks
            
            # First check if we have the year in cache (unless force=True)
            year = None if force else await DEPS.cache_service.get_album_year_from_cache(artist, album)
            
            # If we don't have it in cache or force=True, make an API call
            api_call_made = False
            
            if not year:
                year_logger.info(f"Fetching year for '{artist} - {album}' from external API")
                year = await DEPS.external_api_service.get_album_year(artist, album)
                api_call_made = True  # Mark that we made an API call
                
                if year:
                    # Store in cache only if successfully got a year
                    await DEPS.cache_service.store_album_year_in_cache(artist, album, year)
                    year_logger.info(f"Stored year {year} for '{artist} - {album}' in cache")
                else:
                    year_logger.warning(f"No year found for '{artist} - {album}'")
                    return True  # Return true since we made an API call
            else:
                year_logger.info(f"Using cached year {year} for '{artist} - {album}'")
                
            # No need to continue if we don't have a year
            if not year:
                return api_call_made
                
            # Updating a year for all the tracks of the album
            track_ids = [track.get("id", "") for track in album_tracks]
            track_ids = [tid for tid in track_ids if tid] # Filter empty IDs
            
            if track_ids:
                # Update all tracks in the album in bulk
                success = await update_album_tracks_bulk_async(track_ids, year)
                
                if success:
                    # Update track data in memory
                    for track in album_tracks:
                        track_id = track.get("id", "")
                        if track_id:
                            # Save the old value of the year before updating
                            track["old_year"] = track.get("old_year", "") or track.get("new_year", "")
                            track["new_year"] = year
                            updated_tracks.append(track)
                            
                            # Add to changes log
                            changes_log.append({
                                "artist": artist,
                                "album": album,
                                "track_name": track.get("name", "Unknown"),
                                "year_updated": "true",
                                "new_year": year
                            })
                            
                    year_logger.info(f"Updated {len(track_ids)} tracks for '{artist} - {album}' to year {year}")
                else:
                    year_logger.error(f"Failed to update year for '{artist} - {album}'")
            
            return api_call_made  # Return whether we made an API call
            
        # Processing albums based on API limits
        batch_size = CONFIG.get("year_retrieval", {}).get("batch_size", 10)
        delay_between_batches = CONFIG.get("year_retrieval", {}).get("delay_between_batches", 60)
        
        # Process albums in batches
        album_items = list(albums.items())
        
        for i in range(0, len(album_items), batch_size):
            batch = album_items[i:i + batch_size]
            batch_tasks = []
            
            for _, album_data in batch:
                batch_tasks.append(process_album(album_data))
                
            # Gather results to see if any API calls were made
            api_calls_made = await asyncio.gather(*batch_tasks)            # Log progress
            console_logger.info(f"Processed batch {i//batch_size + 1}/{(len(album_items) + batch_size - 1)//batch_size}")
            
            # Only wait between batches if API calls were made
            if i + batch_size < len(album_items) and any(api_calls_made):
                console_logger.info(f"API calls were made in this batch. Waiting {delay_between_batches} seconds before next batch")
                await asyncio.sleep(delay_between_batches)
            elif i + batch_size < len(album_items):
                console_logger.info("No API calls were made in this batch. Proceeding to next batch immediately.")
                
        # Log final stats
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
    
    This file is used to track the interval between incremental script runs. 
    The can_run_incremental() function checks the time of the last run and
    determines whether the required interval has passed for the next run.
    
    :return: None
    """
    last_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["last_incremental_run_file"])
    with open(last_file, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@get_decorator("Database Verification")
async def verify_and_clean_track_database(force: bool = False) -> int:
    """
    This async function checks all tracks stored in the CSV database against the Music application
    to ensure they still exist. Tracks that can no longer be found are removed from the database.
    By default, verification is performed once per day, unless forced.
    
    The function:
        1. Loads the track database from CSV
        2. Checks for a previous verification on the same day
        3. Verifies each track's existence in Music.app using AppleScript
        4. Removes invalid tracks from the database
        5. Updates the verification timestamp

    :param force: If True, forces a recheck of all tracks
    :return: The number of tracks removed from the database
    """
    try:
        # Additional loging at the beginning of the function
        console_logger.info("Starting database verification process")
        
        # Path to CSV track database
        csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
        
        # Check if the database exists
        if not os.path.exists(csv_path):
            console_logger.info(f"Track database not found at {csv_path}")
            return 0
            
        # Load track list from CSV
        csv_tracks = load_track_list(csv_path)
        
        if not csv_tracks:
            console_logger.info("No tracks in database to verify")
            return 0
            
        console_logger.info(f"Verifying {len(csv_tracks)} tracks in database")
        
        # File Marker to track last check
        last_verify_file = os.path.join(CONFIG["logs_base_dir"], "last_db_verify.log")
        
        # Check whether you need to perform check
        if not force and os.path.exists(last_verify_file):
            try:
                with open(last_verify_file, "r", encoding="utf-8") as f:
                    last_verify = f.read().strip()
                last_date = datetime.strptime(last_verify, "%Y-%m-%d").date()
                if last_date == datetime.now().date():
                    console_logger.info(f"Database was already verified today ({last_date}). Use force=True to recheck.")
                    return 0
            except Exception:
                # If there is a problem with reading or format -perform check
                pass
        
        # ID tracks to check
        track_ids = list(csv_tracks.keys())
        
        # Function to verify track existence via AppleScript
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
        
        # Check tracks in batches
        batch_size = 10
        invalid_track_ids = []
        
        # Display progress
        console_logger.info(f"Checking tracks in batches of {batch_size}...")
        
        # Process in small batches to avoid overload
        for i in range(0, len(track_ids), batch_size):
            batch = track_ids[i:i+batch_size]
            
            # Perform verification in parallel
            tasks = [verify_track_exists(track_id) for track_id in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Analyze results
            for idx, result in enumerate(results):
                track_id = batch[idx] if idx < len(batch) else None
                if track_id and (result is False or isinstance(result, Exception)):
                    invalid_track_ids.append(track_id)
            
            # Log progress
            console_logger.info(f"Verified batch {i//batch_size + 1}/{(len(track_ids) + batch_size - 1)//batch_size}")
            
            # Small pause between batches
            if i + batch_size < len(track_ids):
                await asyncio.sleep(0.2)
        
        # Removing invalid tracks from base
        if invalid_track_ids:
            console_logger.info(f"Found {len(invalid_track_ids)} tracks that no longer exist in Music.app")
            
            # Removal of tracks from CSV-map
            for track_id in invalid_track_ids:
                if track_id in csv_tracks:
                    # Log tracking of removed tracks
                    track_info = csv_tracks[track_id]
                    console_logger.info(f"Removing track {track_id}: {track_info.get('artist', '')} - {track_info.get('name', '')}")
                    del csv_tracks[track_id]
            
            # Saving updated database
            final_list = list(csv_tracks.values())
            save_to_csv(final_list, csv_path, console_logger, error_logger)
            
            console_logger.info(f"Removed {len(invalid_track_ids)} invalid tracks from database")
        else:
            console_logger.info("All tracks in database exist in Music.app")
        
        # Updating the last verification marker file
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
    new_year: Optional[str] = None
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
    
    The process:
    1. Loads existing track data from CSV for reference
    2. For incremental updates, filters to only tracks added since last run
    3. Groups tracks by artist using group_tracks_by_artist()
    4. For each artist, determines the "dominant" genre from their earliest track
    5. Asynchronously updates any tracks whose genre doesn't match the artist's dominant genre
    6. Tracks changes for reporting purposes
    
    The function processes tasks in batches to avoid overwhelming the event loop,
    and uses helper functions to handle track updates efficiently.
    
    :param tracks: A list of dictionaries containing track information
    :param last_run_time: The timestamp of the last incremental update (datetime.min for full runs)
    :return: A tuple containing (updated_tracks, changes_log)
    """
    try:
        csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
        load_track_list(csv_path)
        # Filter tracks if last_run_time is set (not full run)
        if last_run_time and last_run_time != datetime.min:
            console_logger.info(f"Filtering tracks added after {last_run_time}")
            tracks = [
                track for track in tracks 
                if datetime.strptime(track.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") > last_run_time
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
                    changes_log.append({
                        "artist": track.get("artist", "Unknown"),
                        "album": track.get("album", "Unknown"),
                        "track_name": track.get("name", "Unknown"),
                        "old_genre": old_genre,
                        "new_genre": dom_genre,
                        "new_track_name": track.get("name", "Unknown"),
                    })
                    updated_tracks.append(track)
                else:
                    error_logger.error(f"Failed to update genre for track {track_id}")

        # Helper: process tasks in batches to avoid overwhelming the event loop.
        async def process_tasks_in_batches(tasks: List[asyncio.Task], batch_size: int = 1000) -> None:
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                await asyncio.gather(*batch, return_exceptions=True)

        tasks = []
        for artist, artist_tracks in grouped.items():
            if not artist_tracks:
                continue
            try:
                earliest = min(
                    artist_tracks,
                    key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
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

    This function:
        - Checks if Music.app is running before attempting any operations
        - Uses a cache key based on the artist name or "ALL" for all tracks
        - Retrieves cached data if available and force_refresh is False
        - Invalidates the cache entry when force_refresh is True
        - Executes the fetch_tracks.applescript with appropriate timeout
        - Parses the raw data returned by AppleScript using parse_tracks()
        - Caches successful results for future use
        - Includes comprehensive error handling and logging

    :param artist: Optional artist name to filter tracks (None fetches all tracks)
    :param force_refresh: True to force a fetch from Music.app, False to use cached data when available
    :return: A list of track dictionaries, or empty list if fetch fails
    """
    try:
        # Generate a cache key based on the artist
        cache_key = artist if artist else "ALL"
        console_logger.info(f"Fetching tracks with cache_key='{cache_key}', force_refresh={force_refresh}")
        
        # Checking if Music.app is running
        if not is_music_app_running():
            console_logger.error("Music app is not running! Please start Music.app before running this script.")
            return []
            
        # Attempt to use cache if no forced update is required
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
                # With force_refresh, clear the cache for this key
                DEPS.cache_service.invalidate(cache_key)
        
        # AppleScript for data retrieval
        script_name = "fetch_tracks.applescript"
        script_args = [artist] if artist else []
        
        # Set a timeout from the configuration
        timeout = CONFIG.get("applescript_timeout_seconds", 900)  # 15 minutes by default
        
        # Use ap_client with sufficient timeout
        if DEPS and DEPS.ap_client:
            console_logger.info(f"Executing AppleScript via client: {script_name} with args: {script_args}")
            raw_data = await DEPS.ap_client.run_script(script_name, script_args, timeout=timeout)
        else:
            error_logger.error("AppleScriptClient is not initialized!")
            return []
        
        # Logging the result
        if raw_data:
            lines_count = raw_data.count('\n') + 1
            console_logger.info(f"AppleScript returned data: {len(raw_data)} bytes, approximately {lines_count} lines")
        else:
            error_logger.error(f"Empty response from AppleScript {script_name}. Possible script error.")
            return []
        
        # Track analysis
        tracks = parse_tracks(raw_data)
        console_logger.info(f"Successfully parsed {len(tracks)} tracks from Music.app")
        
        # Save to cache, if possible
        if tracks and DEPS and DEPS.cache_service:
            await DEPS.cache_service.set_async(cache_key, tracks)
            console_logger.info(f"Cached {len(tracks)} tracks with key '{cache_key}'")
        
        return tracks
    except Exception as e:
        error_logger.error(f"Error in fetch_tracks_async: {e}", exc_info=True)
        return []

async def main_async(args: argparse.Namespace) -> None:
    try:
        # Add detailed argument logging
        console_logger.info(f"Starting script with arguments: {vars(args)}")
        
        global DEPS
        DEPS = DependencyContainer(CONFIG_PATH)
        
        # Now check the command and the force flag correctly
        if hasattr(args, 'command') and args.command == "verify_database":
            console_logger.info("Executing verify_database command")
            # For the verify_database subcommand, use force from the subcommand arguments
            await verify_and_clean_track_database(force=args.force)
            return
        
        # Test calls
        console_logger.info("Тестовий виклик AppleScript перед основним кодом")
        test_result = await DEPS.ap_client.run_script("test.applescript")
        console_logger.info(f"Тестовий результат: {test_result}")
        
        # Test with fetch_tracks
        console_logger.info("Тестовий виклик fetch_tracks перед основним кодом")
        fetch_result = await DEPS.ap_client.run_script("fetch_tracks.applescript", ["Spiritbox"])
        console_logger.info(f"Результат fetch_tracks довжина: {len(fetch_result) if fetch_result else 0}")
        
        # During a forced start, we check and clear the database of non-existent tracks
        if args.force:
            console_logger.info("Force flag detected, verifying track database...")
            removed_count = await verify_and_clean_track_database(force=True)
            if removed_count > 0:
                console_logger.info(f"Database cleanup removed {removed_count} non-existent tracks")
        
        if args.command == "clean_artist":
            artist = args.artist
            console_logger.info(f"Running in 'clean_artist' mode for artist='{artist}'")
            tracks = await fetch_tracks_async(artist=artist)
            if not tracks:
                console_logger.warning(f"No tracks found for artist: {artist}")
                return
            updated_tracks = []
            changes_log = []

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
                        changes_log.append({
                            "artist": artist_name,
                            "album": track.get("album", "Unknown"),
                            "track_name": orig_name,
                            "old_genre": track.get("genre", "Unknown"),
                            "new_genre": track.get("genre", "Unknown"),
                            "new_track_name": track.get("name", "Unknown"),
                        })
                        updated_tracks.append(track)
                    else:
                        error_logger.error(f"Failed to update track ID {track_id}")
                else:
                    console_logger.info(f"No cleaning needed for track '{orig_name}'")

            tasks = [asyncio.create_task(clean_track(t)) for t in tracks]
            await asyncio.gather(*tasks)
            if updated_tracks:
                await sync_track_list_with_current(
                    updated_tracks,
                    os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                    DEPS.cache_service,
                    console_logger,
                    error_logger,
                    partial_sync=True
                )
                save_changes_report(changes_log, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
                console_logger.info(f"Processed and updated {len(updated_tracks)} tracks (clean_artist).")
            else:
                console_logger.info("No track names or album names needed cleaning (clean_artist).")
            await process_album_years(updated_tracks, force=args.force)
            
        elif args.command == "update_years":
            artist = args.artist
            force_run = args.force
            console_logger.info(f"Running in 'update_years' mode{f' for artist={artist}' if artist else ''}")
            tracks = await fetch_tracks_async(artist=artist, force_refresh=force_run)
            if not tracks:
                console_logger.warning(f"No tracks found{f' for artist: {artist}' if artist else ''}.")
                return
            
            # Оновлення років
            updated_y, changes_y = await process_album_years(tracks, force=force_run)
            
            if updated_y:
                console_logger.info(f"Updated years for {len(updated_y)} tracks")
            
            update_last_incremental_run()
        
        elif args.command == "verify_database":
            console_logger.info("Running database verification")
            removed_count = await verify_and_clean_track_database(force=args.force)
            console_logger.info(f"Database verification completed: {removed_count} tracks removed")
            
        else:
            force_run = args.force
            console_logger.info(f"Force run flag: {force_run}")
            if not can_run_incremental(force_run=force_run):
                console_logger.info("Incremental interval not reached. Skipping.")
                return
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
            test_artists = CONFIG.get("test_artists", [])
            if test_artists:
                all_tracks = []
                for art in test_artists:
                    art_tracks = await fetch_tracks_async(art)
                    all_tracks.extend(art_tracks)
            else:
                all_tracks = await fetch_tracks_async(force_refresh=force_run)
            if not all_tracks:
                console_logger.warning("No tracks fetched.")
                return
            updated_tracks = []
            changes_log = []

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
                        changes_log.append({
                            "artist": artist_name,
                            "album": track.get("album", "Unknown"),
                            "track_name": orig_name,
                            "old_genre": track.get("genre", "Unknown"),
                            "new_genre": track.get("genre", "Unknown"),
                            "new_track_name": track.get("name", "Unknown"),
                        })
                        updated_tracks.append(track)
                    else:
                        error_logger.error(f"Failed to update track ID {track_id}")
                else:
                    console_logger.info(f"No cleaning needed for track '{orig_name}'")
            
            # Batch processing tasks to avoid overloading the event loop with too many concurrent tasks
            tasks = [asyncio.create_task(clean_track(t)) for t in all_tracks]
            await asyncio.gather(*tasks)
            if updated_tracks:
                save_to_csv(updated_tracks, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]), console_logger, error_logger)
                save_changes_report(changes_log, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
                console_logger.info(f"Processed and updated {len(updated_tracks)} tracks (global cleaning).")
            else:
                console_logger.info("No track names or album names needed cleaning in this run.")
            
            updated_g, changes_g = await update_genres_by_artist_async(all_tracks, last_run_time)
            if updated_g:
                save_to_csv(updated_g, os.path.join(CONFIG["logs_base_dir"], "genre_updates.csv"), console_logger, error_logger)
                save_changes_report(changes_g, os.path.join(CONFIG["logs_base_dir"], "genre_changes.csv"), console_logger, error_logger)
                console_logger.info(f"Updated genres for {len(updated_g)} tracks")
            
            # Update album years once
            await process_album_years(all_tracks, force=force_run)
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
        # Closing resources
        if DEPS and hasattr(DEPS, 'external_api_service'):
            await DEPS.external_api_service.close()
            
        # Generation of analytics reports
        if DEPS and hasattr(DEPS, 'analytics'):
            DEPS.analytics.generate_reports()   

def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments using argparse. The script supports two main commands:
    - clean_artist: Clean track/album names for a given artist
    - update_years: Update album years from external APIs
    - verify_database: Verify and clean the track database
    - dry-run: Run the script in dry-run mode (simulate changes)
    - force: Force run the script regardless of the last run time

    - No arguments: Run the script in incremental mode
    """
    # Create the main parser and subparsers for different commands
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    
    # Global arguments
    parser.add_argument("--force", action="store_true", help="Force run the incremental update")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without applying them")
    
    # Subcommands (optional)
    subparsers = parser.add_subparsers(dest="command")
    
    # The clean_artist command
    clean_artist_parser = subparsers.add_parser("clean_artist", help="Clean track/album names for a given artist")
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name")
    clean_artist_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")
    
    # The update_years command
    update_years_parser = subparsers.add_parser("update_years", help="Update album years from external APIs")
    update_years_parser.add_argument("--artist", help="Artist name (optional)")
    update_years_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")
    
    # verify_database command
    verify_db_parser = subparsers.add_parser("verify_database", help="Verify and clean the track database")
    verify_db_parser.add_argument("--force", action="store_true", help="Force database check even if recently performed")
    
    # First, parse the arguments
    args = parser.parse_args()
    
    return args

def handle_dry_run() -> None:
    """
    Handle the dry-run mode by running the script without applying
    any changes to the Music library. This mode is useful for testing
    the script logic without modifying the library.
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
    Main function to run the script. Parses command-line arguments,
    initializes the script, and runs the main_async function
    using asyncio. Handles exceptions and script interruptions
    gracefully, logging errors and analytics
    """
    start_all = time.time()
    args = parse_arguments()
    
    # Dry-run mode processing
    if args.dry_run:
        handle_dry_run()
        sys.exit(0)
    
    # **NEW**: Direct call to verify_database command, bypassing main_async
    if hasattr(args, 'command') and args.command == "verify_database":
        print(f"Executing verify_database command with force={args.force}")
        
        # Ініціалізація залежностей напряму
        from services.dependencies_service import DependencyContainer
        global DEPS
        DEPS = DependencyContainer(CONFIG_PATH)
        
        # Starting the check
        csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
        print(f"Checking database at: {csv_path}")
        
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at {csv_path}")
            sys.exit(1)
            
        # Running an asynchronous function in a synchronous context
        try:
            loop = asyncio.get_event_loop()
            removed = loop.run_until_complete(verify_and_clean_track_database(force=args.force))
            print(f"Database verification completed: removed {removed} invalid tracks")
        except Exception as e:
            print(f"Error during verification: {e}")
            sys.exit(1)
            
        # Generating reports before exit
        if DEPS and hasattr(DEPS, 'analytics'):
            DEPS.analytics.generate_reports()
            
        end_all = time.time()
        print(f"Total executing time: {end_all - start_all:.2f} seconds")
        sys.exit(0)
    
    # Standard execution for other commands
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user.")
    except Exception as exc:
        error_logger.error(f"An unexpected error occurred: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        if DEPS is not None and hasattr(DEPS, 'analytics'):
            DEPS.analytics.generate_reports()
    end_all = time.time()
    console_logger.info(f"Total executing time: {end_all - start_all:.2f} seconds")

if __name__ == "__main__":
    main()