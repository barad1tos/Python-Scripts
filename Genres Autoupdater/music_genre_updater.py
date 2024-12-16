#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import yaml

CONFIG_PATH = "config.yaml"


if not os.path.exists(CONFIG_PATH):
    print(f"Config file {CONFIG_PATH} does not exist.", file=sys.stderr)
    sys.exit(1)

if not os.access(CONFIG_PATH, os.R_OK):
    print(f"No read access to config file {CONFIG_PATH}.", file=sys.stderr)
    sys.exit(1)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)


# === Checking paths ===
paths_to_check = [CONFIG["music_library_path"], CONFIG["apple_scripts_dir"]]

for p in paths_to_check:
    if not os.path.exists(p):
        print(f"Path {p} does not exist.", file=sys.stderr)
        sys.exit(1)
    if not os.access(p, os.R_OK):
        print(f"No read access to {p}.", file=sys.stderr)
        sys.exit(1)


# === Logging Setup ===
RED = "\x1b[31m"
RESET = "\x1b[0m"


class ColorFormatter(logging.Formatter):
    def format(self, record):
        # Colorize ERROR messages
        if record.levelno == logging.ERROR:
            record.msg = f"{RED}{record.msg}{RESET}"
        return super().format(record)


LOG_FILE = CONFIG["log_file"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger()
for handler in logger.handlers:
    handler.setFormatter(ColorFormatter("%(asctime)s - %(levelname)s - %(message)s"))


# === Helper Functions ===


def run_applescript(script_name, args=None):
    # Run AppleScript and capture output
    script_path = os.path.join(CONFIG["apple_scripts_dir"], script_name)
    if not os.path.exists(script_path):
        logging.error(f"AppleScript not found: {script_path}")
        return None

    cmd = ["osascript", script_path]
    if args:
        cmd.extend(args)

    logging.info("Executing AppleScript: " + " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"AppleScript failed: {result.stderr.strip()}")
            return None
        return result.stdout.strip()
    except Exception as e:
        logging.error(f"Error running AppleScript '{script_name}': {e}")
        return None


def parse_tracks(raw_data):
    # Parse track info from raw data
    if not raw_data:
        logging.error("No data to parse.")
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
            logging.error(f"Malformed track data: {row}")
    return tracks


def fetch_tracks(artist=None):
    # Fetch tracks for a specific artist or all
    if artist:
        logging.info(f"Fetching tracks for artist: {artist}")
        raw_data = run_applescript("fetch_tracks.applescript", [artist])
    else:
        logging.info("Fetching all tracks...")
        raw_data = run_applescript("fetch_tracks.applescript")

    if raw_data:
        return parse_tracks(raw_data)
    else:
        msg = "No data fetched"
        if artist:
            msg += f" for artist: {artist}."
        else:
            msg += " for all artists."
        logging.warning(msg)
        return []


def save_to_csv(tracks, file_path):
    # Save track info to CSV
    logging.info(f"Saving tracks to CSV: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "id",
                "name",
                "artist",
                "album",
                "genre",
                "dateAdded",
                "trackStatus",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tracks)
        logging.info(f"Tracks saved to {file_path}.")
    except Exception as e:
        logging.error(f"Failed to save CSV: {e}")


def save_changes_report(changes, file_path):
    # Save the changes log to CSV
    logging.info(f"Saving changes report to {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "artist",
                "album",
                "track_name",
                "old_genre",
                "new_genre",
                "new_track_name",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            # Sort changes by artist
            changes_sorted = sorted(changes, key=lambda x: x["artist"])
            writer.writerows(changes_sorted)
        logging.info("Changes report saved successfully.")
    except Exception as e:
        logging.error(f"Failed to save changes report: {e}")


def group_tracks_by_artist(tracks):
    # Group tracks by artist
    artists = {}
    for track in tracks:
        artist = track.get("artist", "Unknown")
        if artist not in artists:
            artists[artist] = []
        artists[artist].append(track)
    return artists


def determine_dominant_genre_for_artist(tracks):
    # Determine the dominant genre for an artist
    genre_count = {}
    for track in tracks:
        genre = track.get("genre") or "Unknown"
        genre_count[genre] = genre_count.get(genre, 0) + 1
    if genre_count:
        return max(genre_count, key=genre_count.get)
    return "Unknown"


def retry_update_genre(track_id, new_genre):
    """Retry updating the genre of a track."""
    max_retries = CONFIG.get("max_retries", 3)
    delay = CONFIG.get("retry_delay_seconds", 2)
    for attempt in range(1, max_retries + 1):
        result = run_applescript("update_genre.applescript", [track_id, new_genre])
        if result and "Success" in result:
            logging.info(
                f"Updated genre for track {track_id} to {new_genre} (attempt {attempt}/{max_retries})"
            )
            return True
        else:
            logging.warning(
                f"Attempt {attempt}/{max_retries} failed. Retrying in {delay}s..."
            )
            time.sleep(delay)
        return False


def remove_parentheses_with_keywords(name, keywords):
    # Remove parentheses containing any of the keywords
    pattern_str = r"\([^)]*\b(?:" + "|".join(keywords) + r")\b[^)]*\)"
    pattern = re.compile(pattern_str, flags=re.IGNORECASE)
    old_name = None
    while old_name != name:
        old_name = name
        name = re.sub(pattern, "", name)
    return name


def remove_all_parentheses(name):
    # Remove all parentheses and their content
    pattern = re.compile(r"\([^)]*\)")
    old_name = None
    while old_name != name:
        old_name = name
        name = re.sub(pattern, "", name)
    return name


def clean_names(artist, track_name, album_name):
    # Clean track and album names except if in exceptions
    logging.info(
        f"clean_names called with: artist='{artist}', track_name='{track_name}', "
        f"album_name='{album_name}'"
    )

    exceptions = CONFIG.get("exceptions", {}).get("track_cleaning", [])
    is_exception = any(
        exc.get("artist", "").lower() == artist.lower()
        and exc.get("album", "").lower() == album_name.lower()
        for exc in exceptions
    )
    if is_exception:
        logging.info(
            f"No cleaning applied due to exceptions for artist '{artist}', "
            f"album '{album_name}'."
        )
        return track_name.strip(), album_name.strip()

    remaster_keywords = CONFIG.get("cleaning", {}).get(
        "remaster_keywords", ["remaster", "remastered"]
    )

    def clean_string(name):
        # Step 1: remove parentheses with remaster keywords
        new_name = remove_parentheses_with_keywords(name, remaster_keywords)
        # Step 2: remove all parentheses
        new_name = remove_all_parentheses(new_name)
        # Remove extra spaces
        new_name = re.sub(r"\s+", " ", new_name).strip()
        return new_name if new_name else "Unknown"

    original_track_name = track_name
    original_album_name = album_name
    cleaned_track_name = clean_string(track_name)
    cleaned_album_name = clean_string(album_name)

    logging.info(
        f"Original track name: '{original_track_name}' -> '{cleaned_track_name}'"
    )
    logging.info(
        f"Original album name: '{original_album_name}' -> '{cleaned_album_name}'"
    )

    return cleaned_track_name, cleaned_album_name


def can_run_incremental(force_run=False):
    # Check if script can run based on last incremental run time
    last_file = CONFIG["last_incremental_run_file"]
    interval = CONFIG["incremental_interval_minutes"]
    if force_run:
        return True
    if not os.path.exists(last_file):
        logging.info("Last incremental run file not found. Proceeding.")
        return True
    with open(last_file, "r", encoding="utf-8") as f:
        last_run_str = f.read().strip()
    try:
        last_run_time = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logging.error(f"Invalid date in {last_file}. Proceeding with execution.")
        return True

    next_run_time = last_run_time + timedelta(minutes=interval)
    now = datetime.now()

    if now >= next_run_time:
        return True
    else:
        diff = next_run_time - now
        logging.info(
            f"Last incremental run was at {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}. "
            f"Next run allowed after {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"({diff.seconds // 60} minutes remaining)."
        )
        return False


def update_last_incremental_run():
    # Update the last incremental run time
    last_file = CONFIG["last_incremental_run_file"]
    with open(last_file, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def is_music_app_running():
    # Check if Music app is running
    try:
        # Use AppleScript to check the status of Music.app
        script = (
            'tell application "System Events" to (name of processes) contains "Music"'
        )
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True
        )
        return result.stdout.strip().lower() == "true"
    except Exception as e:
        logging.error(f"Unable to check Music.app status: {e}")
        return False


def backup_media_library():
    # Backup the media library
    source_dir = CONFIG["music_library_path"]
    backup_dir = CONFIG["backup_dir"]
    backup_path = os.path.join(backup_dir, "Music Library_backup.musiclibrary")

    # Check if the Music app is running
    if not is_music_app_running():
        logging.error("Music.app is not running. Please open it first.")
        sys.exit(1)
    else:
        logging.info("Music.app is running correctly.")

    # Ensure the backup directory exists
    try:
        os.makedirs(backup_dir, exist_ok=True)
        logging.info(f"Backup directory verified: {backup_dir}")
    except Exception as e:
        logging.error(f"Failed to create backup dir {backup_dir}: {e}")
        sys.exit(1)

    # Get the last modification time of the media library
    try:
        media_mtime = os.path.getmtime(source_dir)
        media_dt = datetime.fromtimestamp(media_mtime)
        logging.info(
            "Last modification date of the media library: "
            f"{media_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        logging.error(f"Could not get media library date: {e}")
        sys.exit(1)

    # Check if a backup already exists and its modification date
    if os.path.exists(backup_path):
        try:
            backup_mtime = os.path.getmtime(backup_path)
            backup_dt = datetime.fromtimestamp(backup_mtime)
            logging.info(f"Last backup date: {backup_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logging.error(f"Unable to retrieve last backup date: {e}")
            sys.exit(1)

        # Compare modification dates
        if media_mtime <= backup_mtime:
            logging.info("No changes since last backup. No new backup needed.")
            return
        else:
            logging.info("Media changed since last backup. Creating new one.")
            # Remove the existing backup directory
            try:
                shutil.rmtree(backup_path)
                logging.info(f"Previous backup removed: {backup_path}")
            except Exception as e:
                logging.error(f"Failed to remove previous backup {backup_path}: {e}")
                sys.exit(1)
    else:
        logging.info("No existing backup found. Creating new backup.")

    # Create a new backup by copying the media library directory
    try:
        shutil.copytree(source_dir, backup_path)
        logging.info(f"Backup created successfully: {backup_path}")
    except Exception as e:
        logging.error(f"Failed to create backup {backup_path}: {e}")
        sys.exit(1)


def update_track(track_id, new_track_name=None, new_album_name=None, new_genre=None):
    # Update track properties with one AppleScript: update_property.applescript
    if not track_id:
        logging.error("No track_id provided.")
        return False

    success = True

    if new_track_name:
        res = run_applescript(
            "update_property.applescript", [track_id, "name", new_track_name]
        )
        if not res or "Success" not in res:
            logging.error(f"Failed to update track name for {track_id}")
            success = False

    if new_album_name:
        res = run_applescript(
            "update_property.applescript", [track_id, "album", new_album_name]
        )
        if not res or "Success" not in res:
            logging.error(f"Failed to update album name for {track_id}")
            success = False

    if new_genre:
        max_retries = CONFIG.get("max_retries", 3)
        delay = CONFIG.get("retry_delay_seconds", 2)
        genre_updated = False
        for attempt in range(1, max_retries + 1):
            res = run_applescript(
                "update_property.applescript", [track_id, "genre", new_genre]
            )
            if res and "Success" in res:
                logging.info(
                    f"Updated genre for {track_id} to {new_genre} "
                    f"(attempt {attempt}/{max_retries})"
                )
                genre_updated = True
                break
            else:
                logging.warning(
                    f"Attempt {attempt}/{max_retries} failed. Retrying in "
                    f"{delay}s..."
                )
                time.sleep(delay)
        if not genre_updated:
            logging.error(f"Failed to update genre for {track_id}")
            success = False

    return success


def update_genres_by_artist(tracks):
    # Update genres for tracks grouped by artist
    grouped = group_tracks_by_artist(tracks)
    updated_tracks = []
    changes_log = []
    for artist, artist_tracks in grouped.items():
        dom_genre = determine_dominant_genre_for_artist(artist_tracks)
        logging.info(
            f"Artist: {artist}, Dominant Genre: {dom_genre}, "
            f"Tracks: {len(artist_tracks)}"
        )
        for track in artist_tracks:
            old_genre = track.get("genre", "Unknown")
            track_id = track.get("id", "")
            status = track.get("trackStatus", "unknown")

            if track_id and old_genre != dom_genre and status == "subscription":
                logging.info(
                    f"Updating track {track_id} (Old Genre: {old_genre}, "
                    f"New Genre: {dom_genre})"
                )
                if update_track(track_id, new_genre=dom_genre):
                    track["genre"] = dom_genre
                    changes_log.append(
                        {
                            "artist": artist,
                            "album": track.get("album", "Unknown"),
                            "track_name": track.get("name", "Unknown"),
                            "old_genre": old_genre,
                            "new_genre": dom_genre,
                            "new_track_name": track.get("name", "Unknown"),
                        }
                    )
                    updated_tracks.append(track)
                else:
                    logging.error(f"Failed to update genre for track {track_id}")
    return updated_tracks, changes_log


def process_artist_tracks(artist):
    # Process tracks for a given artist, clean names, and save changes
    logging.info(f"Starting processing for artist: {artist}")
    tracks = fetch_tracks(artist=artist)
    if not tracks:
        logging.warning(f"No tracks found for artist: {artist}")
        return

    updated_tracks = []
    changes_log = []

    for track in tracks:
        orig_name = track.get("name", "")
        orig_album = track.get("album", "")
        track_id = track.get("id", "")
        artist_name = track.get("artist", artist)

        logging.info(
            f"Processing track ID {track_id} - '{orig_name}' by '{artist_name}' "
            f"from album '{orig_album}'"
        )

        cleaned_name, cleaned_album = clean_names(artist_name, orig_name, orig_album)

        new_tn = cleaned_name if cleaned_name != orig_name else None
        new_an = cleaned_album if cleaned_album != orig_album else None

        if new_tn or new_an:
            res = update_track(track_id, new_track_name=new_tn, new_album_name=new_an)
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
                logging.error(f"Failed to update track ID {track_id}")
        else:
            logging.info(f"No cleaning needed for track '{orig_name}'")

    if updated_tracks:
        save_to_csv(updated_tracks, CONFIG["csv_output_file"])
        save_changes_report(changes_log, CONFIG["changes_report_file"])
        logging.info(
            f"Processed and updated {len(updated_tracks)} tracks for "
            f"artist: {artist}"
        )
    else:
        logging.info(
            f"No track names or album names needed cleaning for artist: {artist}"
        )


def main():
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

    backup_parser = subparsers.add_parser(
        "backup", help="Backup the music library"
    )

    args = parser.parse_args()

    if args.command == "clean_artist":
        process_artist_tracks(args.artist)
        return
    elif args.command == "backup":
        backup_media_library()
        return
    else:
        # Default action (no sub-command)
        backup_media_library()
        force_run = args.force
        if not can_run_incremental(force_run=force_run):
            logging.info("Incremental interval not reached. Skipping.")
            return

        test_artists = CONFIG.get("test_artists", [])
        if test_artists:
            all_tracks = []
            for art in test_artists:
                art_tracks = fetch_tracks(art)
                all_tracks.extend(art_tracks)
        else:
            # Fetch all tracks if no test artists
            all_tracks = fetch_tracks()

        if not all_tracks:
            logging.warning("No tracks fetched.")
            return
        
        # Add name cleaning if --force and no command
        # Similar to process_artist_tracks, but for all tracks
        if force_run:
            updated_tracks = []
            changes_log = []
            for track in all_tracks:
                orig_name = track.get("name", "")
                orig_album = track.get("album", "")
                track_id = track.get("id", "")
                artist_name = track.get("artist", "Unknown")

                logging.info(
                    f"Processing track ID {track_id} - '{orig_name}' by '{artist_name}' "
                    f"from album '{orig_album}' (Force Cleaning)"
                )

                cleaned_name, cleaned_album = clean_names(artist_name, orig_name, orig_album)

                new_tn = cleaned_name if cleaned_name != orig_name else None
                new_an = cleaned_album if cleaned_album != orig_album else None

                if new_tn or new_an:
                    res = update_track(track_id, new_track_name=new_tn, new_album_name=new_an)
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
                        logging.error(f"Failed to update track ID {track_id}")
                else:
                    logging.info(f"No cleaning needed for track '{orig_name}'")

            if updated_tracks:
                save_to_csv(updated_tracks, CONFIG["csv_output_file"])
                save_changes_report(changes_log, CONFIG["changes_report_file"])
                logging.info(
                    f"Processed and updated {len(updated_tracks)} tracks with cleaning."
                )
            else:
                logging.info("No track names or album names needed cleaning.")

        # After cleaning, we update the genres as before
        updated_tracks, changes_log = update_genres_by_artist(all_tracks)
        if updated_tracks:
            save_to_csv(all_tracks, CONFIG["csv_output_file"])
            save_changes_report(changes_log, CONFIG["changes_report_file"])
            logging.info(f"Updated {len(updated_tracks)} tracks with new genres.")
            update_last_incremental_run()
        else:
            logging.info("No tracks needed genre updates.")
            update_last_incremental_run()


if __name__ == "__main__":
    main()
