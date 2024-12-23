#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# music_genre_updater.py

import argparse
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yaml
import asyncio

from scripts.logger import get_loggers
from scripts.reports import save_to_csv, save_changes_report
from scripts.analytics import Analytics

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "my-config.yaml")
CACHE_TTL = 300  # 5 minutes


# Load the configuration
def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads the YAML configuration file.

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
# This returns a tuple of three loggers: (console_logger, error_logger, analytics_logger)
console_logger, error_logger, analytics_logger = get_loggers(
    CONFIG["log_file"],
    CONFIG.get("analytics", {}).get("analytics_log_file")  # path to analytics.log if present
)

# Check the necessary paths
def check_paths(paths: List[str], error_logger: Any) -> None:
    """
    Checks the existence and read access of provided paths.

    :param paths: List of paths to check.
    :param error_logger: Logger for error messages.
    """
    for path in paths:
        if not os.path.exists(path):
            print(f"Path {path} does not exist.", file=sys.stderr)
            sys.exit(1)
        if not os.access(path, os.R_OK):
            print(f"No read access to {path}.", file=sys.stderr)
            sys.exit(1)

check_paths([CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]], error_logger=logging.getLogger("error_logger"))

# Initialize analytics
# We pass analytics_logger to the Analytics class so it can log to analytics.log
analytics = Analytics(CONFIG, console_logger, error_logger, analytics_logger)

# Initialize the cache
fetch_cache: Dict[str, Tuple[List[Dict[str, str]], float]] = {}

# Add analytics decorator to functions
def get_decorator(event_type: str):
    return analytics.decorator(event_type)

# Use decorators for functions
@get_decorator("AppleScript Execution")
async def run_applescript_async(script_name: str, args: Optional[List[str]] = None) -> Optional[str]:
    """
    Executes an AppleScript asynchronously and returns its output.

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
    Parses the output from AppleScript and returns a list of tracks.

    :param raw_data: Raw string data from AppleScript.
    :return: List of track dictionaries.
    """
    if not raw_data:
        error_logger.error("No data to parse.")
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
    Groups tracks by artist.

    :param tracks: List of track dictionaries.
    :return: Dictionary grouped by artist names.
    """
    artists = {}
    for track in tracks:
        artist = track.get("artist", "Unknown")
        artists.setdefault(artist, []).append(track)
    return artists


@get_decorator("Determine Dominant Genre")
def determine_dominant_genre_for_artist(existing_tracks: List[Dict[str, str]]) -> str:
    """
    Determines the dominant genre for an artist based on existing track counts.

    :param existing_tracks: List of existing track dictionaries for an artist.
    :return: Dominant genre as a string.
    """
    genre_count = {}
    for track in existing_tracks:
        genre = track.get("genre") or "Unknown"
        genre_count[genre] = genre_count.get(genre, 0) + 1

    if not genre_count:
        return "Unknown"

    # Determine the genre with the largest number of tracks
    dominant_genre = max(genre_count, key=genre_count.get)
    return dominant_genre


@get_decorator("Check Music App Running")
def is_music_app_running() -> bool:
    """
    Checks if the Music.app is currently running.

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
    Removes parentheses and their contents if they contain specified keywords.

    :param name: The string to clean.
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
            if char == '(':
                stack.append(i)
            elif char == ')':
                if stack:
                    start = stack.pop()
                    end = i
                    pairs.append((start, end))

        logging.debug(f"Bracket pairs found: {pairs}")

        # Sort pairs in order
        pairs.sort()

        # Check each pair of brackets for keywords
        for start, end in reversed(pairs):
            content = name[start+1:end]
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
                        logging.info(f"Marking outer brackets ({outer_start}, {outer_end}) for removal as they contain inner brackets")

        # Remove the marked brackets, starting from the end of the line
        new_name = name
        for start, end in sorted(to_remove, reverse=True):
            logging.debug(f"Removing brackets from index {start} to {end}")
            new_name = new_name[:start] + new_name[end+1:]

        # Remove extra spaces
        new_name = re.sub(r'\s+', ' ', new_name).strip()
        logging.info(f"Cleaned name: '{new_name}'")

        return new_name
    except Exception as e:
        logging.error(f"Error in remove_parentheses_with_keywords: {e}")
        return name


@get_decorator("Clean Names")
def clean_names(artist: str, track_name: str, album_name: str) -> Tuple[str, str]:
    """
    Cleans track and album names by removing specific keywords and album suffixes,
    except for specified exceptions.

    :param artist: Artist name.
    :param track_name: Original track name.
    :param album_name: Original album name.
    :return: Tuple containing cleaned track name and cleaned album name.
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

    def clean_string(name: str) -> str:
        new_name = remove_parentheses_with_keywords(name, remaster_keywords)
        new_name = re.sub(r"\s+", " ", new_name).strip()
        return new_name if new_name else "Unknown"

    original_track_name = track_name
    original_album_name = album_name
    cleaned_track_name = clean_string(track_name)
    cleaned_album_name = clean_string(album_name)

    # Deleting album suffixes
    for suffix in album_suffixes:
        if cleaned_album_name.endswith(suffix):
            cleaned_album_name = cleaned_album_name[:-len(suffix)].strip()
            console_logger.info(f"Removed suffix '{suffix}' from album name. New album name: '{cleaned_album_name}'")

    console_logger.info(f"Original track name: '{original_track_name}' -> '{cleaned_track_name}'")
    console_logger.info(f"Original album name: '{original_album_name}' -> '{cleaned_album_name}'")

    return cleaned_track_name, cleaned_album_name


@get_decorator("Can Run Incremental")
def can_run_incremental(force_run: bool = False) -> bool:
    """
    Determines if an incremental update can be performed based on the last run time.

    :param force_run: If True, bypasses the incremental check.
    :return: True if the update can run, False otherwise.
    """
    last_file = CONFIG["last_incremental_run_file"]
    interval = CONFIG["incremental_interval_minutes"]
    if force_run:
        return True
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
            f"Last incremental run was at {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}. "
            f"Next run allowed after {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"({minutes_remaining} minutes remaining)."
        )
        return False


@get_decorator("Update Last Incremental Run")
def update_last_incremental_run() -> None:
    """
    Updates the last incremental run timestamp in the configuration file.
    """
    last_file = CONFIG["last_incremental_run_file"]
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
    Updates track properties asynchronously via AppleScript.

    :param track_id: ID of the track to update.
    :param new_track_name: New track name, if any.
    :param new_album_name: New album name, if any.
    :param new_genre: New genre, if any.
    :return: True if all updates were successful, False otherwise.
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
                console_logger.info(
                    f"Updated genre for {track_id} to {new_genre} (attempt {attempt}/{max_retries})"
                )
                genre_updated = True
                break
            else:
                console_logger.warning(
                    f"Attempt {attempt}/{max_retries} failed. Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)
        if not genre_updated:
            error_logger.error(f"Failed to update genre for track {track_id}")
            success = False

    return success


@get_decorator("Update Genres by Artist")
async def update_genres_by_artist_async(tracks: List[Dict[str, str]], last_run_time: datetime) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Updates track genres asynchronously, grouping them by artist.
    
    :param tracks: List of all track dictionaries.
    :param last_run_time: The datetime of the last incremental run.
    :return: Tuple containing list of updated tracks and list of changes.
    """
    grouped = group_tracks_by_artist(tracks)
    updated_tracks = []
    changes_log = []
    semaphore = asyncio.Semaphore(10)  # Limit of 10 simultaneous updates

    @get_decorator("Process Track")
    async def process_track(track: Dict[str, str], dom_genre: str) -> None:
        old_genre = track.get("genre", "Unknown")
        track_id = track.get("id", "")
        status = track.get("trackStatus", "unknown")

        if track_id and old_genre != dom_genre and status == "subscription":
            console_logger.info(
                f"Updating track {track_id} (Old Genre: {old_genre}, New Genre: {dom_genre})"
            )
            async with semaphore:
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
        # Divide tracks into existing and new ones
        existing_tracks = [
            track for track in artist_tracks
            if datetime.strptime(track.get("dateAdded"), "%Y-%m-%d %H:%M:%S") <= last_run_time
        ]
        new_tracks = [
            track for track in artist_tracks
            if datetime.strptime(track.get("dateAdded"), "%Y-%m-%d %H:%M:%S") > last_run_time
        ]

        if existing_tracks:
            dom_genre = determine_dominant_genre_for_artist(existing_tracks)
            console_logger.info(
                f"Artist: {artist}, Dominant Genre: {dom_genre} (based on {len(existing_tracks)} existing tracks), Total Tracks: {len(artist_tracks)}"
            )
            # Assigning a dominant genre to new tracks
            for track in new_tracks:
                if track.get("genre", "Unknown") != dom_genre and track.get("trackStatus", "unknown") == "subscription":
                    task = asyncio.create_task(process_track(track, dom_genre))
                    tasks.append(task)
        else:
            # If there are no existing tracks, determine the dominant genre based on all tracks
            dom_genre = determine_dominant_genre_for_artist(artist_tracks)
            console_logger.info(
                f"Artist: {artist}, Dominant Genre: {dom_genre} (based on all {len(artist_tracks)} tracks)"
            )
            for track in artist_tracks:
                if track.get("genre", "Unknown") != dom_genre and track.get("trackStatus", "unknown") == "subscription":
                    task = asyncio.create_task(process_track(track, dom_genre))
                    tasks.append(task)

    await asyncio.gather(*tasks)

    return updated_tracks, changes_log


@get_decorator("Process Artist Tracks")
async def process_artist_tracks_async(artist: str) -> None:
    """
    Asynchronously processes tracks for a specific artist: cleans names and updates them.

    :param artist: Name of the artist to process.
    """
    console_logger.info(f"Starting processing for artist: {artist}")
    tracks = await fetch_tracks_async(artist=artist)
    if not tracks:
        console_logger.warning(f"No tracks found for artist: {artist}")
        return

    updated_tracks = []
    changes_log = []

    for track in tracks:
        orig_name = track.get("name", "")
        orig_album = track.get("album", "")
        track_id = track.get("id", "")
        artist_name = track.get("artist", artist)

        console_logger.info(
            f"Processing track ID {track_id} - '{orig_name}' by '{artist_name}' from album '{orig_album}'"
        )

        cleaned_name, cleaned_album = clean_names(artist_name, orig_name, orig_album)

        new_tn = cleaned_name if cleaned_name != orig_name else None
        new_an = cleaned_album if cleaned_album != orig_album else None

        if new_tn or new_an:
            if await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
                if new_tn:
                    track["name"] = cleaned_name
                if new_an:
                    track["album"] = cleaned_album
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

    if updated_tracks:
        save_to_csv(updated_tracks, CONFIG["csv_output_file"], console_logger, error_logger)
        save_changes_report(changes_log, CONFIG["changes_report_file"], console_logger, error_logger)
        console_logger.info(
            f"Processed and updated {len(updated_tracks)} tracks for artist: {artist}"
        )
    else:
        console_logger.info(
            f"No track names or album names needed cleaning for artist: {artist}"
        )


@get_decorator("Fetch Tracks")
async def fetch_tracks_async(artist: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Asynchronously retrieves tracks for a specific artist or all tracks.
    Uses cache to reduce the number of AppleScript calls.

    :param artist: Name of the artist to fetch tracks for. If None, fetches all tracks.
    :return: List of track dictionaries.
    """
    cache_key = artist if artist else "ALL"
    now = time.time()
    if cache_key in fetch_cache:
        cached_result, cached_time = fetch_cache[cache_key]
        if now - cached_time < CACHE_TTL:
            console_logger.info(f"Returning cached tracks for '{cache_key}' from cache.")
            return cached_result

    # Getting tracks via AppleScript
    if artist:
        console_logger.info(f"Fetching tracks for artist: {artist}")
        raw_data = await run_applescript_async("fetch_tracks.applescript", [artist])
    else:
        console_logger.info("Fetching all tracks...")
        raw_data = await run_applescript_async("fetch_tracks.applescript")

    if raw_data:
        tracks = parse_tracks(raw_data)
        fetch_cache[cache_key] = (tracks, now)
        return tracks
    else:
        msg = "No data fetched"
        if artist:
            msg += f" for artist: {artist}."
        else:
            msg += " for all artists."
        console_logger.warning(msg)
        return []


async def main_async(args: argparse.Namespace) -> None:
    """
    Asynchronous main function to handle different commands.

    :param args: Parsed command-line arguments.
    """
    if args.command == "clean_artist":
        # Loading the artist's tracks
        artist = args.artist
        console_logger.info(f"Running in 'clean_artist' mode for artist='{artist}'")
        tracks = await fetch_tracks_async(artist=artist)
        if not tracks:
            console_logger.warning(f"No tracks found for artist: {artist}")
            return

        # Call the name cleaning process
        await process_artist_tracks_async(artist)
        
        # Calling the genre update. Assume that last_run_time = datetime.min, to treat all tracks as existing.
        last_run_time = datetime.min
        updated_tracks, changes_log = await update_genres_by_artist_async(tracks, last_run_time)
        if updated_tracks:
            # Saving results
            save_to_csv(tracks, CONFIG["csv_output_file"], console_logger, error_logger)
            save_changes_report(changes_log, CONFIG["changes_report_file"], console_logger, error_logger)
            console_logger.info(f"Updated {len(updated_tracks)} tracks with new genres (clean_artist).")
        else:
            console_logger.info("No tracks needed genre updates (clean_artist).")

        return
    else:
        # Execute standard logic
        force_run = args.force
        if not can_run_incremental(force_run=force_run):
            console_logger.info("Incremental interval not reached. Skipping.")
            return

        # Load the last startup time
        last_run_time_str = None
        last_run_file = CONFIG.get("last_incremental_run_file")
        if os.path.exists(last_run_file):
            with open(last_run_file, "r", encoding="utf-8") as f:
                last_run_time_str = f.read().strip()

        if last_run_time_str:
            try:
                last_run_time = datetime.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                console_logger.warning(f"Invalid date format in {last_run_file}. Proceeding as if no previous run.")
                last_run_time = datetime.min
        else:
            console_logger.info("Last incremental run time not found. Considering all tracks as existing.")
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

        # If there is a --force_run, clean up the names
        if force_run:
            updated_tracks = []
            changes_log = []
            semaphore = asyncio.Semaphore(10)  # Limit to 10 simultaneous cleanings

            @get_decorator("Clean Track")
            async def clean_track(track: Dict[str, str]) -> None:
                orig_name = track.get("name", "")
                orig_album = track.get("album", "")
                track_id = track.get("id", "")
                artist_name = track.get("artist", "Unknown")

                console_logger.info(
                    f"Processing track ID {track_id} - '{orig_name}' by '{artist_name}' from album '{orig_album}' (Force Cleaning)"
                )

                cleaned_name, cleaned_album = clean_names(artist_name, orig_name, orig_album)
                new_tn = cleaned_name if cleaned_name != orig_name else None
                new_an = cleaned_album if cleaned_album != orig_album else None

                if new_tn or new_an:
                    async with semaphore:
                        if await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an):
                            if new_tn:
                                track["name"] = cleaned_name
                            if new_an:
                                track["album"] = cleaned_album
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

            tasks = [asyncio.create_task(clean_track(track)) for track in all_tracks]
            await asyncio.gather(*tasks)

            if updated_tracks:
                save_to_csv(updated_tracks, CONFIG["csv_output_file"], console_logger, error_logger)
                save_changes_report(changes_log, CONFIG["changes_report_file"], console_logger, error_logger)
                console_logger.info(
                    f"Processed and updated {len(updated_tracks)} tracks with cleaning."
                )
            else:
                console_logger.info("No track names or album names needed cleaning.")

        # Renewing genres
        updated_tracks, changes_log = await update_genres_by_artist_async(all_tracks, last_run_time)
        if updated_tracks:
            save_to_csv(all_tracks, CONFIG["csv_output_file"], console_logger, error_logger)
            save_changes_report(changes_log, CONFIG["changes_report_file"], console_logger, error_logger)
            console_logger.info(f"Updated {len(updated_tracks)} tracks with new genres.")
            update_last_incremental_run()
        else:
            console_logger.info("No tracks needed genre updates.")
            update_last_incremental_run()


def main() -> None:
    """
    Entry point of the script. Parses arguments and starts the asynchronous main function.
    """
    parser = argparse.ArgumentParser(description="Music Genre Updater Script")
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for cleaning artist
    clean_artist_parser = subparsers.add_parser("clean_artist", help="Clean track and album names for a given artist")
    clean_artist_parser.add_argument("--artist", required=True, help="Artist name to clean")
    clean_artist_parser.add_argument("--force", action="store_true", help="Force the run, bypassing incremental checks")

    # Argument for force run
    parser.add_argument("--force", action="store_true", help="Force run the incremental update")

    args = parser.parse_args()

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        console_logger.info("Script interrupted by user.")
    except Exception as e:
        error_logger.error(f"An unexpected error occurred: {e}")
        # Generate analytics reports after executing the script
        analytics.generate_reports()
        sys.exit(1)

    # Generate analytics reports even if there's no exception
    analytics.generate_reports()

if __name__ == "__main__":
    main()