#!/usr/bin/env python3

"""
Music Genre Updater Script

This script fetches tracks from the Music app using AppleScript and updates the genres
based on each artist's earliest album and track. It also cleans track and album names
by removing remaster keywords and suffixes. The script can be run in incremental mode
to update only the tracks that have been added since the last run.

The script can be run in two modes:
    1. Global Cleaning: Cleans all track and album names and updates genres for all tracks.
    2. Artist Cleaning: Cleans track and album names for a specific artist and updates genres.

The script requires the following configuration in 'my-config.yaml':
    1. music_library_path: Path to the Music library XML file.
    2. apple_scripts_dir: Path to the directory containing the AppleScript files.
    3. logs_base_dir: Base directory for logs and reports.
    4. cache_ttl_seconds: Cache TTL for fetched tracks in seconds (default 900).
    5. incremental_interval_minutes: Interval between incremental runs in minutes (default 60).
    6. exceptions: List of exceptions for track cleaning and genre updates.
    7. cleaning: Configuration for cleaning track and album names.
    8. test_artists: List of test artists to fetch tracks for (for testing purposes).

The script logs errors and changes to CSV files and generates reports for analytics.

Example usage:
    python3 music_genre_updater.py --dry-run
    python3 music_genre_updater.py --force
    python3 music_genre_updater.py clean_artist --artist "Artist Name" --force
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

from services.applescript_client import AppleScriptClient
from utils.analytics import Analytics
from utils.logger import get_full_log_path, get_loggers
from utils.reports import load_track_list, save_changes_report, save_to_csv, sync_track_list_with_current

# This client is used to run AppleScript commands asynchronously throughout the script.
# It is initialized once and reused to avoid creating multiple instances.
AP_CLIENT: Optional[AppleScriptClient] = None

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

check_paths([get_config()["music_library_path"], get_config()["apple_scripts_dir"]], error_logger)

# Initialize analytics tracking
analytics = Analytics(get_config(), console_logger, error_logger, analytics_logger)

# Global cache for fetched tracks; cache TTL is set in config (default 900 seconds)
# The cache stores the results of requests to Music.app to reduce the load on AppleScript
# Format: {cache_key: (track_data, timestamp)}
CACHE_TTL = CONFIG.get("cache_ttl_seconds", 900)
fetch_cache: Dict[str, Tuple[List[Dict[str, str]], float]] = {}

def get_decorator(event_type: str):
    """
    Decorator function to track function calls with the specified event type.

    :param event_type: The event type to track.
    :return: A decorator function.
    """
    return analytics.decorator(event_type)

@get_decorator("AppleScript Execution")
async def run_applescript_async(script_name: str, args: Optional[List[str]] = None) -> Optional[str]:
    """
    Run an AppleScript asynchronously using the AppleScriptClient.

    :param script_name: The name of the AppleScript file to run.
    :param args: A list of arguments to pass to the AppleScript.
    :return: The output of the AppleScript execution.
    """
    global AP_CLIENT
    if AP_CLIENT is None:
        AP_CLIENT = AppleScriptClient(get_config(), logger=console_logger)
    return await AP_CLIENT.run_script(script_name, args)

@get_decorator("Parse Tracks")
def parse_tracks(raw_data: str) -> List[Dict[str, str]]:
    """
    Parse the raw track data fetched from AppleScript into a list of dictionaries.

    :param raw_data: The raw data string fetched from AppleScript.
    :return: A list of dictionaries containing track information.
    """
    if not raw_data:
        error_logger.error("No data fetched from AppleScript.")
        return []
    tracks = []
    rows = raw_data.strip().split("\n")
    for row in rows:
        fields = row.split("~|~")
        if len(fields) == 7:
            tracks.append({
                "id": fields[0].strip(),
                "name": fields[1].strip(),
                "artist": fields[2].strip(),
                "album": fields[3].strip(),
                "genre": fields[4].strip(),
                "dateAdded": fields[5].strip(),
                "trackStatus": fields[6].strip(),
            })
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
    Check if the Music.app is currently running.

    :return: True if the Music.app is running, False otherwise.
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

@get_decorator("Clean Names")
def clean_names(artist: str, track_name: str, album_name: str) -> Tuple[str, str]:
    """
    Clean the track and album names by removing remaster keywords and album suffixes.
    
    The cleaning process includes:
    1. Checking for exceptions (some albums do not need to be cleaned)
    2. Removing brackets with remaster keywords (e.g. "(2019 Remaster)")
    3. Remove album suffixes from the configuration list
    4. Normalize spaces and remove redundant characters
    
    :param artist: The artist name (used to check for exceptions).
    :param track_name: The track name to clean.
    :param album_name: The album name to clean.
    :return: A tuple containing the cleaned track name and album name.
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

@get_decorator("Batch Bulk Update Album Year")
async def update_album_tracks_bulk_async(track_ids: List[str], new_year: str) -> bool:
    """
    Update the album year for a batch of tracks asynchronously via AppleScript.

    :param track_ids: A list of track IDs to update.
    :param new_year: The new year to set for the album.
    :return: True if the bulk update was successful, False otherwise.
    """
    if not track_ids:
        error_logger.error("No track IDs provided for bulk update.")
        return False    
    track_ids_str = ",".join(track_ids)    
    res = await run_applescript_async("update_property.applescript", ["year", new_year, track_ids_str])
    if res and "Success" in res:
        console_logger.info(f"Bulk update success: {res}")
        return True    
    else:
        error_logger.error(f"Bulk update failed: {res}")
        return False  

@get_decorator("Can Run Incremental")
def can_run_incremental(force_run: bool = False) -> bool:
    """
    Check if the incremental interval has passed since the last run.

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

@get_decorator("Update Track")
async def update_track_async(
    track_id: str,
    new_track_name: Optional[str] = None,
    new_album_name: Optional[str] = None,
    new_genre: Optional[str] = None
) -> bool:
    """
    Update the track properties asynchronously via AppleScript.

    :param track_id: The ID of the track to update.
    :param new_track_name: The new track name.
    :param new_album_name: The new album name.
    :param new_genre: The new genre.
    :return: True if the update was successful, False otherwise.
    """
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

    return success

@get_decorator("Update Genres by Artist")
async def update_genres_by_artist_async(tracks: List[Dict[str, str]], last_run_time: datetime
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Update the genres for tracks based on the dominant genre of the artist.
    
    The process of updating genres:
    1. Grouping tracks by artist
    2. Determining the dominant genre for each artist
    3. Asynchronously update all tracks of the artist to the dominant genre
    4. Track changes for reporting purposes
    
    Parallel execution: The function creates a separate asynchronous task for each
    track that needs to be updated, which allows you to efficiently process large collections.
    
    :param tracks: A list of dictionaries containing track information.
    :param last_run_time: The last run time for the incremental update.
    :return: A tuple containing the updated tracks and the changes log.
    """
    csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
    load_track_list(csv_path)
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
                tasks.append(process_track(track, dom_genre))
    if tasks:
        await asyncio.gather(*tasks)
    return updated_tracks, changes_log

async def fetch_tracks_async(artist: Optional[str] = None, force_refresh: bool = False) -> List[Dict[str, str]]:
    """
    Fetch tracks asynchronously from AppleScript and cache the results.
    
    Caching strategy:
    - Uses a global time-to-live (TTL) cache from the configuration
    - Cache is identified by artist name or "ALL" for all tracks
    - Forced cache refresh is possible through the force_refresh parameter
    - An empty list is returned on an unsuccessful request
    
    :param artist: The artist name to fetch tracks for (None fetches all tracks).
    :param force_refresh: True to force a cache refresh, False to use cached data if available.
    :return
    """
    cache_key = artist if artist else "ALL"
    now = time.time()
    if not force_refresh and cache_key in fetch_cache:
        cached_result, cached_time = fetch_cache[cache_key]
        if now - cached_time < CACHE_TTL:
            console_logger.info(f"Returning cached tracks for '{cache_key}'. Cached count: {len(cached_result)}")
            return cached_result

    if force_refresh:
        console_logger.info(f"Forced cache refresh requested for '{cache_key}'")
    
    if artist:
        console_logger.info(f"Fetching tracks for artist: {artist}")
        raw_data = await run_applescript_async("fetch_tracks.applescript", [artist])
    else:
        console_logger.info("Fetching all tracks...")
        raw_data = await run_applescript_async("fetch_tracks.applescript")

    if raw_data:
        tracks = parse_tracks(raw_data)
        console_logger.info(f"Fetched {len(tracks)} tracks for key '{cache_key}'")
        if force_refresh and cache_key in fetch_cache:
            old_tracks, _ = fetch_cache[cache_key]
            if len(old_tracks) != len(tracks):
                console_logger.error(f"Track count mismatch on forced refresh for '{cache_key}': cached {len(old_tracks)} vs new {len(tracks)}. Aborting sync.")
        fetch_cache[cache_key] = (tracks, now)
        return tracks
    else:
        msg = "No data fetched" + (f" for artist: {artist}." if artist else " for all artists.")
        console_logger.warning(msg)
        return []

async def main_async(args: argparse.Namespace) -> None:
    """
    The main asynchronous function that runs the script based on the specified command.
    
    Modes of operation:
    1. "clean_artist" - clears track and album names for a specific artist and updates genres for that artist
    2. Global mode - processing of all tracks in the library with the possibility of incremental updates (only new tracks)
    
    The process of execution:
    1. Creating an AppleScriptClient
    2. Getting tracks from Music.app (with caching)
    3. Clearing track and album names
    4. Update genres based on the dominant genre for each artist
    5. Saves changes and updates the last run time
    
    :param args: The parsed command-line arguments.
    """
    # Creating an AppleScriptClient within the current loop and bind it to a global variable
    global AP_CLIENT
    AP_CLIENT = AppleScriptClient(CONFIG, logger=console_logger)
    
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
        
        tasks = [clean_track(t) for t in tracks]
        await asyncio.gather(*tasks)
        if updated_tracks:
            sync_track_list_with_current(
                updated_tracks, 
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]), 
                console_logger, 
                error_logger,
                partial_sync=True
            )
            save_changes_report(changes_log, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
            console_logger.info(f"Processed and updated {len(updated_tracks)} tracks (clean_artist).")
        else:
            console_logger.info("No track names or album names needed cleaning (clean_artist).")
        last_run_time = datetime.min
        updated_g, changes_g = await update_genres_by_artist_async(tracks, last_run_time)
        if updated_g:
            sync_track_list_with_current(
                updated_g, 
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                console_logger, 
                error_logger,
                partial_sync=True
            )
            save_changes_report(changes_g, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
            console_logger.info(f"Updated {len(updated_g)} tracks with new genres (clean_artist).")
        else:
            console_logger.info("No tracks needed genre updates (clean_artist).")
        return
    else:
        force_run = args.force
        if not can_run_incremental(force_run=force_run):
            console_logger.info("Incremental interval not reached. Skipping.")
            return
        last_run_file = CONFIG["logging"]["last_incremental_run_file"]
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
            all_tracks = await fetch_tracks_async()
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
        tasks = [clean_track(t) for t in all_tracks]
        await asyncio.gather(*tasks)
        if updated_tracks:
            save_to_csv(updated_tracks, CONFIG["logging"]["csv_output_file"], console_logger, error_logger)
            save_changes_report(changes_log, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
            console_logger.info(f"Processed and updated {len(updated_tracks)} tracks (global cleaning).")
        else:
            console_logger.info("No track names or album names needed cleaning in this run.")
        updated_g, changes_g = await update_genres_by_artist_async(all_tracks, last_run_time)
        if updated_g:
            sync_track_list_with_current(
                all_tracks,
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                console_logger,
                error_logger,
                partial_sync=False
            )
            save_changes_report(changes_g, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
            console_logger.info(f"Updated {len(updated_g)} tracks with new genres.")
            update_last_incremental_run()
        else:
            console_logger.info("No tracks needed genre updates.")
            update_last_incremental_run()

def main() -> None:
    """
    The main function that parses command-line arguments and runs the script.
    """
    start_all = time.time()
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    subparsers = parser.add_subparsers(dest="command")
    clean_artist_parser = subparsers.add_parser("clean_artist", help="Clean track/album names for a given artist")
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name")
    clean_artist_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")
    parser.add_argument("--force", action="store_true", help="Force run the incremental update")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without applying them")
    args = parser.parse_args()
    if args.dry_run:
        try:
            from utils import dry_run
        except ImportError:
            error_logger.error("Dry run module not found. Ensure 'utils/dry_run.py' exists.")
            sys.exit(1)
        console_logger.info("Running in dry-run mode. No changes will be applied.")
        asyncio.run(dry_run.main())
        sys.exit(0)
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user.")
    except Exception as exc:
        error_logger.error(f"An unexpected error occurred: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        analytics.generate_reports()
    end_all = time.time()
    console_logger.info(f"Total executing time: {end_all - start_all:.2f} seconds")

if __name__ == "__main__":
    main()