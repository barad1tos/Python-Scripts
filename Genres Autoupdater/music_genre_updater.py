#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import time
import logging
import subprocess
from threading import Thread
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

CONFIG_PATH = "config.json"

if not os.path.exists(CONFIG_PATH):
    print(f"Config file {CONFIG_PATH} does not exist.", file=sys.stderr)
    sys.exit(1)

if not os.access(CONFIG_PATH, os.R_OK):
    print(f"No read access to config file {CONFIG_PATH}.", file=sys.stderr)
    sys.exit(1)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# === Checking paths ===
paths_to_check = [
    CONFIG["music_library_path"],
    CONFIG["apple_scripts_dir"]
]

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
        if record.levelno == logging.ERROR:
            record.msg = f"{RED}{record.msg}{RESET}"
        return super().format(record)

LOG_FILE = CONFIG["log_file"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger()
for handler in logger.handlers:
    handler.setFormatter(ColorFormatter("%(asctime)s - %(levelname)s - %(message)s"))

# === Helper Functions ===
def run_applescript(script_name, args=None):
    """Run an AppleScript with optional arguments and capture its output."""
    script_path = os.path.join(CONFIG["apple_scripts_dir"], script_name)
    if not os.path.exists(script_path):
        logging.error(f"AppleScript not found: {script_path}")
        return None

    cmd = ["osascript", script_path]
    if args:
        cmd.extend(args)

    logging.info(f"Executing AppleScript: {' '.join(cmd)}")
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
    """Parse track information from raw data."""
    if not raw_data:
        logging.error("No data to parse.")
        return []

    tracks = []
    rows = raw_data.strip().split("\n")
    for row in rows:
        fields = row.split("!!!")
        if len(fields) == 6:
            tracks.append({
                "id": fields[0].strip(),
                "name": fields[1].strip(),
                "artist": fields[2].strip(),
                "album": fields[3].strip(),
                "genre": fields[4].strip(),
                "dateAdded": fields[5].strip(),
            })
        else:
            logging.error(f"Malformed track data: {row}")
    return tracks

def fetch_tracks(artist=None):
    """Fetch tracks for a specific artist or all tracks."""
    if artist:
        logging.info(f"Fetching tracks for artist: {artist}")
        raw_data = run_applescript("fetch_tracks.applescript", [artist])
    else:
        logging.info("Fetching all tracks...")
        raw_data = run_applescript("fetch_tracks.applescript")
    
    if raw_data:
        return parse_tracks(raw_data)
    else:
        logging.warning(f"No data fetched for artist: {artist if artist else 'all artists'}.")
        return []

def save_to_csv(tracks, file_path):
    """Save track information to a CSV file."""
    logging.info(f"Saving tracks to CSV: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tracks)
        logging.info(f"Tracks saved successfully to {file_path}.")
    except Exception as e:
        logging.error(f"Failed to save CSV: {e}")

def group_tracks_by_artist(tracks):
    """Group tracks by artist."""
    artists = {}
    for track in tracks:
        artist = track.get("artist", "Unknown")
        if artist not in artists:
            artists[artist] = []
        artists[artist].append(track)
    return artists

def determine_dominant_genre_for_artist(tracks):
    """Determine the dominant genre for an artist."""
    genre_count = {}
    for track in tracks:
        genre = track.get("genre", "Unknown")
        genre_count[genre] = genre_count.get(genre, 0) + 1
    return max(genre_count, key=genre_count.get) if genre_count else "Unknown"

def retry_update_genre(track_id, new_genre):
    """Retry updating the genre of a track."""
    max_retries = CONFIG.get("max_retries", 3)
    delay = CONFIG.get("retry_delay_seconds", 2)
    for attempt in range(1, max_retries + 1):
        result = run_applescript("update_genre.applescript", [track_id, new_genre])
        if result and "Success" in result:
            logging.info(f"Updated genre for track {track_id} to {new_genre} (attempt {attempt}/{max_retries})")
            return True
        else:
            logging.warning(f"Attempt {attempt}/{max_retries} failed. Retrying in {delay}s...")
            time.sleep(delay)
    return False

def update_genres_by_artist(tracks):
    """Update genres for tracks grouped by artist."""
    grouped = group_tracks_by_artist(tracks)
    updated_tracks = []
    for artist, artist_tracks in grouped.items():
        dominant_genre = determine_dominant_genre_for_artist(artist_tracks)
        logging.info(f"Artist: {artist}, Dominant Genre: {dominant_genre}, Tracks: {len(artist_tracks)}")
        for track in artist_tracks:
            old_genre = track.get("genre", "Unknown")
            track_id = track.get("id", "")
            if track_id and old_genre != dominant_genre:
                logging.info(f"Updating track {track_id} (Old Genre: {old_genre}, New Genre: {dominant_genre})")
                if retry_update_genre(track_id, dominant_genre):
                    track["genre"] = dominant_genre
                    updated_tracks.append(track)
                else:
                    logging.error(f"Failed to update genre for track {track_id}")
    return updated_tracks

def can_run_incremental(force_run=False):
    """Check if the script can run based on the last incremental run time."""
    LAST_INCREMENTAL_RUN_FILE = CONFIG["last_incremental_run_file"]
    INCREMENTAL_INTERVAL_MINUTES = CONFIG["incremental_interval_minutes"]
    if force_run:
        return True
    if not os.path.exists(LAST_INCREMENTAL_RUN_FILE):
        return True
    with open(LAST_INCREMENTAL_RUN_FILE, "r", encoding="utf-8") as f:
        last_run_str = f.read().strip()
    try:
        last_run_time = datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logging.error(f"Invalid date in {LAST_INCREMENTAL_RUN_FILE}")
        return True
    return (datetime.now() - last_run_time) > timedelta(minutes=INCREMENTAL_INTERVAL_MINUTES)

def update_last_incremental_run():
    """Update the last incremental run time to the current time."""
    LAST_INCREMENTAL_RUN_FILE = CONFIG["last_incremental_run_file"]
    with open(LAST_INCREMENTAL_RUN_FILE, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def main():
    """Main function to orchestrate the script."""
    force_run = "--force" in sys.argv
    if not can_run_incremental(force_run=force_run):
        logging.info("Last incremental run was less than the configured interval ago. Skipping execution.")
        return

    test_artists = CONFIG.get("test_artists", [])
    all_tracks = []

    if test_artists:
        for artist in test_artists:
            artist_tracks = fetch_tracks(artist)
            all_tracks.extend(artist_tracks)
    else:
        all_tracks = fetch_tracks()

    if not all_tracks:
        logging.warning("No tracks fetched.")
        return

    updated_tracks = update_genres_by_artist(all_tracks)
    if updated_tracks:
        save_to_csv(all_tracks, CONFIG["csv_output_file"])
        logging.info(f"Updated {len(updated_tracks)} tracks with new genres.")
        update_last_incremental_run()
    else:
        logging.info("No tracks needed genre updates.")
        update_last_incremental_run()

if __name__ == "__main__":
    main()