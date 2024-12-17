#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# music_genre_updater.py

import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

import yaml
import asyncio

from scripts.logger import get_loggers
from scripts.reports import save_to_csv, save_changes_report

CONFIG_PATH = "config.yaml"

# Checking for a configuration file
if not os.path.exists(CONFIG_PATH):
    print(f"Config file {CONFIG_PATH} does not exist.", file=sys.stderr)
    sys.exit(1)

if not os.access(CONFIG_PATH, os.R_OK):
    print(f"No read access to config file {CONFIG_PATH}.", file=sys.stderr)
    sys.exit(1)

# Loading the configuration
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# Check paths
paths_to_check = [CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]]
for p in paths_to_check:
    if not os.path.exists(p):
        print(f"Path {p} does not exist.", file=sys.stderr)
        sys.exit(1)
    if not os.access(p, os.R_OK):
        print(f"No read access to {p}.", file=sys.stderr)
        sys.exit(1)

LOG_FILE = CONFIG["log_file"]

# Initializing loggers
console_logger, error_logger = get_loggers(LOG_FILE)


# === Cache for fetch_tracks ===
fetch_cache = {}
CACHE_TTL = 300  # 5 minutes


async def run_applescript_async(script_name, args=None):
    # Executes AppleScript asynchronously and returns its output.
    script_path = os.path.join(CONFIG["apple_scripts_dir"], script_name)
    if not os.path.exists(script_path):
        error_logger.error(f"AppleScript not found: {script_path}")
        return None

    cmd = ["osascript", script_path]
    if args:
        cmd.extend(args)

    console_logger.info("Executing AppleScript async: " + " ".join(cmd))
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


def parse_tracks(raw_data):
    # Parsing AppleScript output and returns a list of tracks.
    if not raw_data:
        error_logger.error("No data to parse.")
        return []
    tracks = []
    rows = raw_data.strip().split("\n")
    for row in rows:
        fields = row.split("~|~")
        if len(fields) == 7:
            tracks.append(
                {
                    "id": fields[0].strip(),
                    "name": fields[1].strip(),
                    "artist": fields[2].strip(),
                    "album": fields[3].strip(),
                    "genre": fields[4].strip(),
                    "dateAdded": fields[5].strip(),
                    "trackStatus": fields[6].strip(),
                }
            )
        else:
            error_logger.error(f"Malformed track data: {row}")
    return tracks


async def fetch_tracks_async(artist=None):
    """
    Asynchronously retrieves tracks for a specific artist or all tracks.
    Uses cache to reduce the number of AppleScript calls.
    """
    # Checking the cache
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


def group_tracks_by_artist(tracks):
    # Groups tracks by artist.
    artists = {}
    for track in tracks:
        artist = track.get("artist", "Unknown")
        if artist not in artists:
            artists[artist] = []
        artists[artist].append(track)
    return artists


def determine_dominant_genre_for_artist(tracks):
    # Determines the dominant genre for an artist based on the number of tracks.
    genre_count = {}
    for track in tracks:
        genre = track.get("genre") or "Unknown"
        genre_count[genre] = genre_count.get(genre, 0) + 1
    if genre_count:
        return max(genre_count, key=genre_count.get)
    return "Unknown"


def is_music_app_running():
    # Checks if the Music.app is running.
    try:
        script = 'tell application "System Events" to (name of processes) contains "Music"'
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
        return result.stdout.strip().lower() == "true"
    except Exception as e:
        error_logger.error(f"Unable to check Music.app status: {e}")
        return False


def remove_parentheses_with_keywords(name, keywords):
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

    # Sort pairs in order
    pairs.sort()

    # Check each pair of brackets for keywords
    for start, end in reversed(pairs):
        content = name[start+1:end]
        # Check if the content contains a keyword
        if any(keyword in content.lower() for keyword in keyword_set):
            # Remove this pair of brackets
            to_remove.add((start, end))
            # Also delete all external pairs containing this pair
            for outer_start, outer_end in pairs:
                if outer_start < start and outer_end > end:
                    to_remove.add((outer_start, outer_end))

    # Remove the marked brackets, starting from the end of the line
    new_name = name
    for start, end in sorted(to_remove, reverse=True):
        new_name = new_name[:start] + new_name[end+1:]

    # Remove extra spaces
    new_name = re.sub(r'\s+', ' ', new_name).strip()
    return new_name


def clean_names(artist, track_name, album_name):
    # Clears track and album names, except as specified in the configuration.

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
        return (track_name.strip(), album_name.strip())

    remaster_keywords = CONFIG.get("cleaning", {}).get("remaster_keywords", ["remaster", "remastered"])

    def clean_string(name):
        new_name = remove_parentheses_with_keywords(name, remaster_keywords)
        new_name = re.sub(r"\s+", " ", new_name).strip()
        return new_name if new_name else "Unknown"

    original_track_name = track_name
    original_album_name = album_name
    cleaned_track_name = clean_string(track_name)
    cleaned_album_name = clean_string(album_name)

    console_logger.info(
        f"Original track name: '{original_track_name}' -> '{cleaned_track_name}'"
    )
    console_logger.info(
        f"Original album name: '{original_album_name}' -> '{cleaned_album_name}'"
    )

    return (cleaned_track_name, cleaned_album_name)


def can_run_incremental(force_run=False):
    # Checks if it is possible to perform an incremental update based on the last start time.
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
        console_logger.info(
            f"Last incremental run was at {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}. "
            f"Next run allowed after {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"({diff.seconds // 60} minutes remaining)."
        )
        return False


def update_last_incremental_run():
    # Updates the file with the time of the last incremental run.
    last_file = CONFIG["last_incremental_run_file"]
    with open(last_file, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


async def update_track_async(track_id, new_track_name=None, new_album_name=None, new_genre=None):
    # Updates track properties asynchronously via AppleScript.
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


async def update_genres_by_artist_async(tracks):
    # Updates track genres asynchronously, grouping them by artist.

    grouped = group_tracks_by_artist(tracks)
    updated_tracks = []
    changes_log = []
    semaphore = asyncio.Semaphore(10)  # Limit of 10 simultaneous updates

    async def process_track(track, dom_genre):
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

    tasks = []
    for artist, artist_tracks in grouped.items():
        dom_genre = determine_dominant_genre_for_artist(artist_tracks)
        console_logger.info(
            f"Artist: {artist}, Dominant Genre: {dom_genre}, Tracks: {len(artist_tracks)}"
        )
        for track in artist_tracks:
            task = asyncio.create_task(process_track(track, dom_genre))
            tasks.append(task)

    await asyncio.gather(*tasks)

    return (updated_tracks, changes_log)


async def process_artist_tracks_async(artist):
    # Asynchronously processes tracks for a specific artist: clears titles and updates them.
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
            res = await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an)
            if res:
                if new_tn:
                    track["name"] = cleaned_name
                if new_an:
                    track["album"] = cleaned_album
                changes_log.append(
                    {
                        "artist": artist_name,
                        "album": track.get("album", "Unknown"),
                        "track_name": orig_name,
                        "old_genre": track.get("genre", "Unknown"),
                        "new_genre": track.get("genre", "Unknown"),
                        "new_track_name": track.get("name", "Unknown"),
                    }
                )
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


async def main_async(args):
    # Asynchronous main script loop.
    if args.command == "clean_artist":
        await process_artist_tracks_async(args.artist)
        return
    else:
        # Execute standard logic
        force_run = args.force
        if not can_run_incremental(force_run=force_run):
            console_logger.info("Incremental interval not reached. Skipping.")
            return

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

            async def clean_track(track):
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
                        res = await update_track_async(track_id, new_track_name=new_tn, new_album_name=new_an)
                        if res:
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
        updated_tracks, changes_log = await update_genres_by_artist_async(all_tracks)
        if updated_tracks:
            save_to_csv(all_tracks, CONFIG["csv_output_file"], console_logger, error_logger)
            save_changes_report(changes_log, CONFIG["changes_report_file"], console_logger, error_logger)
            console_logger.info(f"Updated {len(updated_tracks)} tracks with new genres.")
            update_last_incremental_run()
        else:
            console_logger.info("No tracks needed genre updates.")
            update_last_incremental_run()


def main():
    # Parser of command line arguments and launch of an asynchronous main loop.
    parser = argparse.ArgumentParser(description="Music Genre Updater")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force the run, bypassing incremental checks",
    )
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    clean_artist_parser = subparsers.add_parser(
        "clean_artist", help="Clean track and album names for a given artist"
    )
    clean_artist_parser.add_argument(
        "--artist", required=True, help="Artist name to clean"
    )
    clean_artist_parser.add_argument(
        "--force",
        action="store_true",
        help="Force the run, bypassing incremental checks",
    )

    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()