#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json
import logging
import subprocess
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import time

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

# === Helper Functions ===
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
    handlers=[RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3), logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger()
for handler in logger.handlers:
    handler.setFormatter(ColorFormatter("%(asctime)s - %(levelname)s - %(message)s"))

# === Main part ===
def get_current_mode():
    now = datetime.now()
    hour = now.hour
    # EXECUTION_MODE: day (10-23), night (00-09), always
    mode = CONFIG.get("execution_mode", "always")
    if mode == "day":
        # run only if hour between 10 and 23
        return 'day' if 10 <= hour <= 23 else 'off'
    elif mode == "night":
        # run only if hour between 0 and 9 (i.e. 00:00-09:59)
        return 'night' if 0 <= hour <= 9 else 'off'
    else:
        return 'always'

def can_run_incremental(force_run=False):
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
    LAST_INCREMENTAL_RUN_FILE = CONFIG["last_incremental_run_file"]
    with open(LAST_INCREMENTAL_RUN_FILE, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def run_applescript(script_name, args=None):
    script_path = os.path.join(CONFIG["apple_scripts_dir"], script_name)
    if not os.path.exists(script_path):
        logging.error(f"AppleScript not found: {script_path}")
        return None

    cmd = ["osascript", script_path]
    if args:
        cmd.extend(args)

    logging.info(f"Executing AppleScript command: {cmd}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(
                f"AppleScript '{script_name}' failed with code {result.returncode}. stderr: {result.stderr}"
            )
            return None
        output = result.stdout.strip()
        if not output:
            logging.warning(f"AppleScript '{script_name}' returned no data.")
        else:
            logging.info(f"AppleScript '{script_name}' executed successfully.")
        # Можемо зробити паузу після кожного скрипта
        time.sleep(CONFIG.get("pause_between_scripts",1))
        return output
    except Exception as e:
        logging.error(f"Error running AppleScript '{script_name}': {e}")
        return None

def parse_tracks(raw_data):
    if not raw_data:
        logging.error("No raw data to parse.")
        return []

    tracks = []
    rows = raw_data.strip().split("\n")
    for row in rows:
        fields = row.split("~||~")
        if len(fields) == 6:
            track = {
                "id": fields[0].strip(),
                "name": fields[1].strip(),
                "artist": fields[2].strip(),
                "album": fields[3].strip(),
                "genre": fields[4].strip(),
                "dateAdded": fields[5].strip(),
            }
            tracks.append(track)
        else:
            # If the string does not contain exactly 6 fields - log an error
            logging.error(f"Malformed track data: {row}")
    return tracks

def fetch_tracks():
    logging.info("Fetching tracks from Apple Music...")
    test_artists = CONFIG.get("test_artists", [])
    all_tracks = []
    if test_artists:
        for artist in test_artists:
            logging.info(f"Fetching tracks for artist: {artist}")
            raw_data = run_applescript("fetch_tracks.applescript", [artist])
            if raw_data:
                artist_tracks = parse_tracks(raw_data)
                all_tracks.extend(artist_tracks)
            else:
                logging.warning(f"No data returned for artist {artist}.")
    else:
        raw_data = run_applescript("fetch_tracks.applescript", [])
        if raw_data:
            all_tracks = parse_tracks(raw_data)
        else:
            logging.warning("No data returned for all tracks.")

    return all_tracks

def save_to_csv(tracks, file_path):
    logging.info(f"Saving tracks to CSV file: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tracks)
        logging.info("CSV file saved successfully.")
    except Exception as e:
        logging.error(f"Failed to save CSV: {e}")

def group_tracks_by_artist(tracks):
    artists = {}
    for t in tracks:
        artist = t.get("artist","Unknown")
        if artist not in artists:
            artists[artist] = []
        artists[artist].append(t)
    return artists

def determine_dominant_genre_for_artist(tracks):
    genre_count = {}
    for t in tracks:
        g = t.get("genre","Unknown")
        genre_count[g] = genre_count.get(g, 0) + 1
    return max(genre_count, key=genre_count.get) if genre_count else "Unknown"

def retry_update_genre(track_id, new_genre):
    max_retries = CONFIG.get("max_retries", 3)
    delay = CONFIG.get("retry_delay_seconds", 2)
    for attempt in range(1, max_retries + 1):
        result = run_applescript("update_genre.applescript", [track_id, new_genre])
        if result and "Success" in result:
            logging.info(f"Updated genre for track {track_id} to {new_genre} (attempt {attempt}/{max_retries})")
            return True
        else:
            logging.warning(f"Attempt {attempt}/{max_retries} failed to update genre for track {track_id}. Retrying in {delay}s...")
            time.sleep(delay)
    return False

def update_genres_by_artist(tracks):
    grouped = group_tracks_by_artist(tracks)
    updated_tracks = []
    for artist, artist_tracks in grouped.items():
        dominant_genre = determine_dominant_genre_for_artist(artist_tracks)
        logging.info(f"Artist: {artist}, Dominant Genre: {dominant_genre}, Tracks: {len(artist_tracks)}")
        for tr in artist_tracks:
            old_genre = tr.get("genre","Unknown")
            track_id = tr.get("id","")
            if track_id and old_genre != dominant_genre:
                logging.info(f"Updating track {track_id} (Artist: {artist}, Old: {old_genre}, New: {dominant_genre})")
                if retry_update_genre(track_id, dominant_genre):
                    tr["genre"] = dominant_genre
                    updated_tracks.append(tr)
                else:
                    logging.error(f"Failed to update genre for track {track_id} after multiple attempts")
    return updated_tracks

def process_tracks_in_batches(tracks):
    # Using the BATCH_SIZE
    BATCH_SIZE = CONFIG.get("batch_size", 1000)
    PAUSE_BETWEEN_BATCHES = CONFIG.get("pause_between_batches", 5)
    total = len(tracks)
    if total == 0:
        return []
    updated_all = []
    for i in range(0, total, BATCH_SIZE):
        batch = tracks[i:i+BATCH_SIZE]
        logging.info(f"Processing batch {i//BATCH_SIZE+1} with {len(batch)} tracks")
        updated = update_genres_by_artist(batch)
        updated_all.extend(updated)
        # after the batch, wait for PAUSE_BETWEEN_BATCHES seconds
        if (i + BATCH_SIZE) < total:
            logging.info(f"Waiting {PAUSE_BETWEEN_BATCHES}s before next batch...")
            time.sleep(PAUSE_BETWEEN_BATCHES)
    return updated_all

def main():
    force_run = "--force" in sys.argv
    mode = get_current_mode()
    if mode == 'off':
        logging.info("Current mode prevents running at this time. Skipping.")
        return

    if can_run_incremental(force_run=force_run):
        tracks = fetch_tracks()
        if not tracks:
            logging.warning("No tracks fetched.")
        else:
            updated_tracks = process_tracks_in_batches(tracks)
            if updated_tracks:
                save_to_csv(tracks, CONFIG["csv_output_file"])
                logging.info(f"Updated {len(updated_tracks)} tracks with new genres.")
            else:
                logging.info("No tracks needed genre updates.")
        update_last_incremental_run()
    else:
        logging.info("Last incremental run was less than configured interval ago. Skipping.")

if __name__ == "__main__":
    main()