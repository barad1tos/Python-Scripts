import os
import json
import time
import logging
import csv
from datetime import datetime
from logging.handlers import RotatingFileHandler

# === Configuration ===
MUSIC_LIBRARY_PATH = '/Users/romanborodavkin/Music/Music/Music Library.musiclibrary'
LOG_FILE = '/Users/romanborodavkin/Music/Music/music_genre_updater.log'
STATE_FILE = "/Users/romanborodavkin/Music/Music/track_state.csv"
APPLE_SCRIPTS_DIR = '/Users/romanborodavkin/Library/Mobile Documents/com~apple~CloudDocs/3. Git/Own/Apple Scripts'
CSV_OUTPUT_FILE = "/Users/romanborodavkin/Music/Music/track_list.csv"

# Timeouts
PAUSE_BETWEEN_BATCHES = 5
PAUSE_BETWEEN_SCRIPTS = 1

# Execution modes
EXECUTION_MODE = "always"  # Options: "day", "night", or "always"

# Testing on a specific artist
TEST_ARTIST = "" # Specify an artist for testing (None for the whole library)

# Sequential processing
BATCH_SIZE = 1000 # Number of tracks in one batch

# Logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

# === Helper Functions ===

def prevent_sleep():
    os.system("caffeinate -dims &")
    logging.info("System sleep prevention enabled.")

def allow_sleep():
    os.system("killall caffeinate")
    logging.info("System sleep prevention disabled.")

def run_applescript(script_name, args=None):
    script_path = os.path.join(APPLE_SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        logging.error(f"AppleScript not found: {script_path}")
        return None

    command = f"osascript '{script_path}'"
    if args:
        command += " " + " ".join(['"{}"'.format(arg.replace("|", "\\|")) for arg in args])

    logging.debug(f"Executing AppleScript command: {command}")
    try:
        result = os.popen(command).read().strip()
        if result:
            logging.info(f"AppleScript '{script_name}' executed successfully.")
        else:
            logging.warning(f"AppleScript '{script_name}' returned no data.")
        return result
    except Exception as e:
        logging.error(f"Error executing AppleScript '{script_name}': {e}")
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
            logging.error(f"Malformed track data: {row}")
    return tracks

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

def fetch_tracks():
    logging.info("Fetching tracks from Apple Music...")
    raw_data = run_applescript("fetch_tracks.applescript", [TEST_ARTIST] if TEST_ARTIST else [])
    if raw_data:
        logging.debug(f"Raw data preview: {raw_data[:500]}")
    return parse_tracks(raw_data)

def load_library_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                reader = csv.DictReader(f)
                return list(reader)
        except Exception as e:
            logging.error(f"Failed to load state: {e}")
    return []

def save_state(state):
    save_to_csv(state, STATE_FILE)

def group_tracks_by_artist(tracks):
    """Групує треки за виконавцем."""
    artists = {}
    for track in tracks:
        artist = track["artist"]
        if artist not in artists:
            artists[artist] = []
        artists[artist].append(track)
    return artists

def determine_dominant_genre_for_artist(tracks):
    """Визначає домінуючий жанр у межах треків одного виконавця."""
    genre_count = {}
    for track in tracks:
        genre = track["genre"]
        if genre not in genre_count:
            genre_count[genre] = 0
        genre_count[genre] += 1
    # Повертаємо жанр із найбільшою частотою
    return max(genre_count, key=genre_count.get)

def update_genres_by_artist(tracks):
    """Оновлює жанри треків у межах одного виконавця."""
    grouped_tracks = group_tracks_by_artist(tracks)
    updated_tracks = []

    for artist, artist_tracks in grouped_tracks.items():
        dominant_genre = determine_dominant_genre_for_artist(artist_tracks)
        logging.info(f"Artist: {artist}, Dominant Genre: {dominant_genre}")

        for track in artist_tracks:
            if track["genre"] != dominant_genre:
                track_id = track["id"]
                track["genre"] = dominant_genre
                result = run_applescript("update_genre.applescript", [track_id, dominant_genre])
                if "Success" in result:
                    logging.info(f"Updated genre for track {track_id} to {dominant_genre}")
                    updated_tracks.append(track)
                else:
                    logging.error(f"Failed to update genre for track {track_id}: {result}")

    return updated_tracks

def update_genres(tracks, dominant_genre):
    updated_tracks = []
    for track in tracks:
        if track["genre"] != dominant_genre:
            track_id = track["id"]
            track["genre"] = dominant_genre
            result = run_applescript("update_genre.applescript", [track_id, dominant_genre])
            if "Success" in result:
                logging.info(f"Updated genre for track {track_id} to {dominant_genre}")
                updated_tracks.append(track)
            else:
                logging.error(f"Failed to update genre for track {track_id}: {result}")
    return updated_tracks

def run_incremental_check():
    logging.info("Starting incremental check...")
    tracks = fetch_tracks()
    if not tracks:
        logging.warning("No tracks fetched.")
        return

    updated_tracks = update_genres_by_artist(tracks)

    if updated_tracks:
        save_to_csv(tracks, CSV_OUTPUT_FILE)
        logging.info(f"Updated {len(updated_tracks)} tracks with new genres.")
    else:
        logging.info("No tracks needed genre updates.")

def main():
    prevent_sleep()
    try:
        run_incremental_check()
    finally:
        allow_sleep()

if __name__ == "__main__":
    main()