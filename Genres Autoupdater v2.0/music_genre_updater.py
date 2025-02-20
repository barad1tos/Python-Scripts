#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Music Genre Updater Script

This script interacts with AppleScript to retrieve tracks from the Music.app,
applies name cleansing and genre updates, and manages track data in CSV reports.
It is designed to run periodically (e.g. via launchd) and can also be run manually.
with specific commands to clean and update genres for all tracks or specific artists.
"""

import argparse
import asyncio
import logging
import os
import re
import subprocess
import sys
import time
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yaml

from scripts.logger import get_loggers, get_full_log_path
from scripts.reports import save_to_csv, save_changes_report, sync_track_list_with_current, load_track_list
from scripts.analytics import Analytics

# Define the directory and configuration path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "my-config.yaml")
CACHE_TTL = 900  # 15 minutes


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load the YAML configuration file.

    :param config_path: Path to the configuration file.
    :return: Configuration as a dictionary.
    """
    if not os.path.exists(config_path):
        print(f"Config file {config_path} does not exist.", file=sys.stderr)
        sys.exit(1)
    if not os.access(config_path, os.R_OK):
        print(f"No read access to config file {config_path}.", file=sys.stderr)
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# Initialize the configuration
CONFIG = load_config(CONFIG_PATH)

# Initialize the loggers
# Returns a tuple of three loggers: (console_logger, error_logger, analytics_logger)
console_logger, error_logger, analytics_logger, _ = get_loggers(CONFIG)
# Get the full path for the analytics log file
analytics_log_file = get_full_log_path(CONFIG, "analytics_log_file", "analytics/analytics.log")

# Check the necessary paths
def check_paths(paths: List[str], error_logger: Any) -> None:
    """
    Check the existence and read access of provided paths.

    :param paths: List of paths to check.
    :param error_logger: Logger for error messages.
    """
    for path in paths:
        if not os.path.exists(path):
            error_logger.error(f"Path {path} does not exist.")
            sys.exit(1)
        if not os.access(path, os.R_OK):
            error_logger.error(f"No read access to {path}.")
            sys.exit(1)

# Verify necessary paths are accessible
check_paths([CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]], error_logger=logging.getLogger("error_logger"))

# Initialize analytics tracking
analytics = Analytics(CONFIG, console_logger, error_logger, analytics_logger)

# Initialize the cache to store fetched tracks temporarily
fetch_cache: Dict[str, Tuple[List[Dict[str, str]], float]] = {}

# Add analytics decorator to functions
def get_decorator(event_type: str):
    """
    Retrieve the analytics decorator for a specific event type.

    :param event_type: The type of event to decorate.
    :return: The decorator function.
    """
    return analytics.decorator(event_type)

@get_decorator("AppleScript Execution")
async def run_applescript_async(script_name: str, args: Optional[List[str]] = None) -> Optional[str]:
    """
    Execute an AppleScript asynchronously and return its output.

    :param script_name: Name of the AppleScript file.
    :param args: List of arguments to pass to the script.
    :return: Output of the script or None if failed.
    """
    script_path = os.path.join(CONFIG["apple_scripts_dir"], script_name)
    if not os.path.exists(script_path):
        error_logger.error(f"AppleScript not found: {script_path}")
        return None

    cmd = ["osascript", script_path]
    if args:
        cmd.extend(args)

    console_logger.info(f"Executing {' '.join(cmd)}")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            error_logger.error(f"AppleScript failed: {stderr.decode().strip()}")
            return None
        return stdout.decode().strip()
    except Exception as e:
        error_logger.error(f"Error running AppleScript '{script_name}': {e}")
        return None


@get_decorator("Parse Tracks")
def parse_tracks(raw_data: str) -> List[Dict[str, str]]:
    """
    Parse the output from AppleScript and return a list of track dictionaries.

    :param raw_data: Raw string data from AppleScript.
    :return: List of track dictionaries.
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
    Group tracks by artist. 

    :param tracks: List of track dictionaries.
    :return: Dictionary of artist names to lists of track dictionaries.
    """
    artists = defaultdict(list)
    for track in tracks:
        artist = track.get("artist", "Unknown")
        artists[artist].append(track)
    
    return dict(artists)


@get_decorator("Determine Dominant Genre")
def determine_dominant_genre_for_artist(artist_tracks: List[Dict[str, str]]) -> str:
    """
    Determine the dominant genre by picking the genre from the oldest track of the earliest album.
    
    :param artist_tracks: List of track dictionaries for a specific artist.
    :return: Genre from the earliest album's oldest track or "Unknown".
    """
    if not artist_tracks:
        return "Unknown"
    try:
        # Group tracks by album and determine the earliest track for each album
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
                    album_earliest[album].get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"
                )
                if track_date < existing_date:
                    album_earliest[album] = track
        # Select the album with the earliest added track
        earliest_album_track = min(
            album_earliest.values(),
            key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
        )
        return earliest_album_track.get("genre") or "Unknown"
    except Exception as e:
        logging.error(f"Error in determine_dominant_genre_for_artist: {e}")
        return "Unknown"


@get_decorator("Check Music App Running")
def is_music_app_running() -> bool:
    """
    Check if the Music.app is currently running.

    :return: True if running, False otherwise.
    """
    try:
        script = 'tell application "System Events" to (name of processes) contains "Music"'
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
        return result.stdout.strip().lower() == "true"
    except Exception as e:
        error_logger.error(f"Unable to check Music.app status: {e}")
        return False


@get_decorator("Remove Parentheses with Keywords")
def remove_parentheses_with_keywords(name: str, keywords: List[str]) -> str:
    """
    Remove parentheses and their contents if they contain specified keywords.

    :param name: Original string to clean.
    :param keywords: List of keywords to check within parentheses.
    :return: Cleaned string.
    """
    try:
        logging.info(f"remove_parentheses_with_keywords called with name='{name}' and keywords={keywords}")
        # Removes brackets and their contents if they contain one of the keywords.
        stack = []
        to_remove = set()
        keyword_set = set(k.lower() for k in keywords)

        # Collect all pairs of brackets
        pairs = []
        for i, char in enumerate(name):
            if char == "(":
                stack.append(i)
            elif char == ")":
                if stack:
                    start = stack.pop()
                    end = i
                    pairs.append((start, end))

        logging.debug(f"Bracket pairs found: {pairs}")

        # Sort pairs in order
        pairs.sort()

        # Check each pair of brackets for keywords
        for start, end in reversed(pairs):
            content = name[start + 1:end]
            logging.debug(f"Checking content inside brackets: '{content}'")
            # Check if the content contains a keyword
            if any(keyword in content.lower() for keyword in keyword_set):
                # Remove this pair of brackets
                to_remove.add((start, end))
                logging.info(f"Marking brackets ({start}, {end}) for removal due to keyword match")
                # Also delete all external pairs containing this pair
                for outer_start, outer_end in pairs:
                    if outer_start < start and outer_end > end:
                        to_remove.add((outer_start, outer_end))
                        logging.info(f"Marking outer brackets ({outer_start}, {outer_end}) as well")

        # Remove the marked brackets, starting from the end of the line
        new_name = name
        for start, end in sorted(to_remove, reverse=True):
            logging.debug(f"Removing brackets from index {start} to {end}")
            new_name = new_name[:start] + new_name[end + 1:]

        # Remove extra spaces
        new_name = re.sub(r"\s+", " ", new_name).strip()
        logging.info(f"Cleaned name: '{new_name}'")

        return new_name
    except Exception as e:
        logging.error(f"Error in remove_parentheses_with_keywords: {e}")
        return name


@get_decorator("Clean Names")
def clean_names(artist: str, track_name: str, album_name: str) -> Tuple[str, str]:
    """
    Clean the track and album names by removing keywords and suffixes,
    unless exceptions are defined for that artist/album.

    :param artist: Artist name.
    :param track_name: Original track name.
    :param album_name: Original album name.
    :return: (cleaned_track_name, cleaned_album_name).
    """
    console_logger.info(
        f"clean_names called with: artist='{artist}', track_name='{track_name}', album_name='{album_name}'"
    )

    exceptions = CONFIG.get("exceptions", {}).get("track_cleaning", [])
    is_exception = any(
        exc.get("artist", "").lower() == artist.lower() and exc.get("album", "").lower() == album_name.lower()
        for exc in exceptions
    )
    if is_exception:
        console_logger.info(
            f"No cleaning applied due to exceptions for artist '{artist}', album '{album_name}'."
        )
        return track_name.strip(), album_name.strip()

    remaster_keywords = CONFIG.get("cleaning", {}).get("remaster_keywords", ["remaster", "remastered"])
    album_suffixes = CONFIG.get("cleaning", {}).get("album_suffixes_to_remove", [])

    def clean_string(val: str) -> str:
        """
        Helper function to clean a string by removing specified patterns.

        :param val: The original string.
        :return: The cleaned string.
        """
        new_val = remove_parentheses_with_keywords(val, remaster_keywords)
        new_val = re.sub(r"\s+", " ", new_val).strip()
        return new_val if new_val else "Unknown"

    original_track = track_name
    original_album = album_name
    cleaned_track = clean_string(track_name)
    cleaned_album = clean_string(album_name)

    # Delete album suffixes
    for suffix in album_suffixes:
        if cleaned_album.endswith(suffix):
            cleaned_album = cleaned_album[: -len(suffix)].strip()
            console_logger.info(
                f"Removed suffix '{suffix}' from album. New album name: '{cleaned_album}'"
            )

    console_logger.info(f"Original track name: '{original_track}' -> '{cleaned_track}'")
    console_logger.info(f"Original album name: '{original_album}' -> '{cleaned_album}'")

    return cleaned_track, cleaned_album


@get_decorator("Batch Bulk Update Album Year")
async def update_album_tracks_bulk_async(track_ids: List[str], new_year: str) -> bool:
    """
    Bulk update the release year for all tracks of an album in a single AppleScript call.
    
    :param track_ids: List of track IDs.
    :param new_year: New release year.
    :return: True if update is successful, False otherwise.
    """
    if not track_ids:
        error_logger.error("No track IDs provided for bulk update.")
        return False    
    # Join track IDs into a comma-separated string    
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
    Decide if an incremental update can be performed based on the last run time.

    :param force_run: If True, bypasses incremental checks.
    :return: True if the update can run, False otherwise.
    """
    if force_run:
        return True
    last_file = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["last_incremental_run_file"])
    interval = CONFIG["incremental_interval_minutes"]
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
        console_logger.info(
            f"Last incremental run was at {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}. Next run allowed after {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} ({minutes_remaining} minutes remaining)."
            )
        return False


@get_decorator("Update Last Incremental Run")
def update_last_incremental_run() -> None:
    """
    Update the last incremental run timestamp in the config file.
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
    Update track properties asynchronously via AppleScript (name, album, genre).

    :param track_id: ID of the track to update.
    :param new_track_name: New track name, if any.
    :param new_album_name: New album name, if any.
    :param new_genre: New genre, if any.
    :return: True if updates were successful, False otherwise.
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
    Update track genres asynchronously, grouping them by artist.
    Always picks the genre from 'existing_tracks' (old) if any, otherwise from all.

    :param tracks: List of all track dictionaries.
    :param last_run_time: Datetime of the last incremental run.
    :return: Tuple of (updated_tracks, changes_log).
    """
    # Load existing CSV map to see older statuses
    from scripts.reports import load_track_list
    csv_path = os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"])
    csv_map = load_track_list(csv_path)

    grouped = group_tracks_by_artist(tracks)
    updated_tracks = []
    changes_log = []

    @get_decorator("Process Track")
    async def process_track(track: Dict[str, str], dom_genre: str) -> None:
        old_genre = track.get("genre", "Unknown")
        track_id = track.get("id", "")
        status = track.get("trackStatus", "unknown")
        if track_id and old_genre != dom_genre and status in ("subscription", "downloaded"):
            console_logger.info(f"Updating track {track_id} (Old Genre: {old_genre}, New Genre: {dom_genre})")
            update_lock = asyncio.Lock()

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

    # Process each artist's tracks
    tasks = []
    for artist, artist_tracks in grouped.items():
        if not artist_tracks:
            continue

        # 1) Find the earliest track by dateAdded
        try:
            earliest = min(
                artist_tracks,
                key=lambda t: datetime.strptime(t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            )
            dom_genre = earliest.get("genre", "Unknown")
        except Exception as e:
            error_logger.error(f"Error determining earliest track for artist '{artist}': {e}")
            dom_genre = "Unknown"

        console_logger.info(
            f"Artist: {artist}, Dominant Genre: {dom_genre} (earliest track among {len(artist_tracks)})."
        )

        # 2) Compare all tracks of the artist to dom_genre
        for track in artist_tracks:
            if track.get("genre", "Unknown") != dom_genre:
                # queue update
                await process_track(track, dom_genre)

    await asyncio.gather(*tasks)
    return updated_tracks, changes_log


# Modified fetch_tracks_async in music_genre_updater.py
@get_decorator("Fetch Tracks")
async def fetch_tracks_async(artist: Optional[str] = None, force_refresh: bool = False) -> List[Dict[str, str]]:
    """
    Retrieve tracks for a specific artist or all tracks. Uses a cache to reduce AppleScript calls.
    Allows forced cache refresh.
    """
    cache_key = artist if artist else "ALL"
    now = time.time()
    # Use cached result if available and not force refresh
    if not force_refresh and cache_key in fetch_cache:
        cached_result, cached_time = fetch_cache[cache_key]
        if now - cached_time < CACHE_TTL:
            console_logger.info(f"Returning cached tracks for '{cache_key}' from cache. Cached count: {len(cached_result)}")
            return cached_result

    if force_refresh:
        console_logger.info(f"Forced cache refresh requested for '{cache_key}'")

    # Getting tracks via AppleScript
    if artist:
        console_logger.info(f"Fetching tracks for artist: {artist}")
        raw_data = await run_applescript_async("fetch_tracks.applescript", [artist])
    else:
        console_logger.info("Fetching all tracks...")
        raw_data = await run_applescript_async("fetch_tracks.applescript")

    if raw_data:
        tracks = parse_tracks(raw_data)
        console_logger.info(f"Fetched {len(tracks)} tracks for key '{cache_key}'")
        # If force_refresh is True and there is cached data, compare counts
        if force_refresh and cache_key in fetch_cache:
            old_tracks, _ = fetch_cache[cache_key]
            if len(old_tracks) != len(tracks):
                console_logger.error(
                    f"Track count mismatch on forced refresh for '{cache_key}': cached count {len(old_tracks)} vs new count {len(tracks)}. Aborting sync."
                )
        # Update cache with new data
        fetch_cache[cache_key] = (tracks, now)
        return tracks
    else:
        msg = "No data fetched"
        msg += f" for artist: {artist}." if artist else " for all artists."
        console_logger.warning(msg)
        return []


async def main_async(args: argparse.Namespace) -> None:
    """
    Asynchronous main function to handle different commands.

    :param args: Parsed command-line arguments.
    """
    # If the user specified "clean_artist" => only that artist
    if args.command == "clean_artist":
        # Clean a specific artist's tracks
        artist = args.artist
        console_logger.info(f"Running in 'clean_artist' mode for artist='{artist}'")

        # Fetch tracks for the specified artist
        tracks = await fetch_tracks_async(artist=artist)
        if not tracks:
            console_logger.warning(f"No tracks found for artist: {artist}")
            return

        # 1) Clean names for each track
        updated_tracks = []
        changes_log = []

        @get_decorator("Clean Track (Artist Only)")
        async def clean_track(track: Dict[str, str]) -> None:
            """
            Clean and update a single track's name and album.

            :param track: The track dictionary.
            """
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

        # Create and gather all cleaning tasks
        for t in tracks:
            await clean_track(t)

        # Save the results if any tracks were updated
        if updated_tracks:
            # PARTIAL sync — do not remove other artists' tracks from CSV
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

        # 2) Now update genres
        last_run_time = datetime.min  # Treat all as existing to ensure genre updates
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
        # "Standard" logic: fetch all (or test_artists), ALWAYS clean, ALWAYS update genres
        force_run = args.force
        if not can_run_incremental(force_run=force_run):
            console_logger.info("Incremental interval not reached. Skipping.")
            return

        # Determine last_run_time
        last_run_file = CONFIG["logging"]["last_incremental_run_file"]
        last_run_time_str = None

        if os.path.exists(last_run_file):
            try:
                with open(last_run_file, "r", encoding="utf-8") as f:
                    last_run_time_str = f.read().strip()
            except Exception as e:
                error_logger.error(f"Failed to read {last_run_file}: {e}")

        if last_run_time_str:
            try:
                last_run_time = datetime.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                error_logger.error(f"Invalid date format in {last_run_file}. Resetting to full run.")
                last_run_time = datetime.min
        else:
            console_logger.info("No valid last run timestamp found. Considering all tracks as existing.")
            last_run_time = datetime.min

        # Fetch tracks from test_artists if specified, else fetch all
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

        # 1) Always clean all tracks
        updated_tracks = []
        changes_log = []

        @get_decorator("Clean Track (Global)")
        async def clean_track(track: Dict[str, str]) -> None:
            """
            Clean and update a single track's name and album in the global run.

            :param track: The track dictionary.
            """
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

        # Create and gather all cleaning tasks using the correct variable "all_tracks"
        for t in all_tracks:
            await clean_track(t)

        # Save the results if any tracks were updated
        if updated_tracks:
            save_to_csv(updated_tracks, CONFIG["logging"]["csv_output_file"], console_logger, error_logger)
            save_changes_report(changes_log, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
            console_logger.info(f"Processed and updated {len(updated_tracks)} tracks (global cleaning).")
        else:
            console_logger.info("No track names or album names needed cleaning in this run.")

        # 2) Then always update genres
        updated_g, changes_g = await update_genres_by_artist_async(all_tracks, last_run_time)
        if updated_g:
            # Sync with the current CSV file, removing missing tracks
            sync_track_list_with_current(
                all_tracks,
                os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["csv_output_file"]),
                console_logger,
                error_logger,
                partial_sync=False  # full removal of missing
            )
            save_changes_report(changes_g, os.path.join(CONFIG["logs_base_dir"], CONFIG["logging"]["changes_report_file"]), console_logger, error_logger)
            console_logger.info(f"Updated {len(updated_g)} tracks with new genres.")
            update_last_incremental_run()
        else:
            console_logger.info("No tracks needed genre updates.")
            update_last_incremental_run()


def main() -> None:
    """
    The entry point of the script. Parses arguments and starts the asynchronous main function.
    """
    start_all = time.time()
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for cleaning artist
    clean_artist_parser = subparsers.add_parser("clean_artist", help="Clean track/album names for a given artist")
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name")
    clean_artist_parser.add_argument("--force", action="store_true", help="Force, bypassing incremental checks")

    # Global argument for force
    parser.add_argument("--force", action="store_true", help="Force run the incremental update")
    
    # Global argument for dry-run
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without applying them")
    
    args = parser.parse_args()

    if args.dry_run:
        try:
            from scripts import dry_run
        except ImportError:
            error_logger.error("Dry run module not found. Ensure 'scripts/dry_run.py' exists.")
            sys.exit(1)
        console_logger.info("Running in dry-run mode. No changes will be applied.")
        asyncio.run(dry_run.main())
        sys.exit(0)
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user.")
    except Exception as exc:
        error_logger.error(f"An unexpected error occurred: {exc}")
        analytics.generate_reports()
        sys.exit(1)

    # Generate analytics reports even if there is no exception
    analytics.generate_reports()

    end_all = time.time()
    console_logger.info(f"Total executing time: {end_all - start_all:.2f} seconds")


if __name__ == "__main__":
    main()