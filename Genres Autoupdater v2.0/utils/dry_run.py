#!/usr/bin/env python3

"""
Dry Run Module

This module provides a dry run simulation for cleaning and genre updates.
It fetches tracks directly using AppleScript, simulates cleaning and genre updates,
and saves the changes to CSV files. It also generates a unified report combining
both cleaning and genre update changes for comprehensive review before actual
implementation.
"""

import asyncio
import csv
import os
import sys

from typing import Dict, List

import yaml

# Add path to music_genre_updater and scripts
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_dir, "..")
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from music_genre_updater import CONFIG_PATH, clean_names, determine_dominant_genre_for_artist
from services.dependencies_service import DependencyContainer
from utils.logger import get_full_log_path, get_loggers

# Load the configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "..", "my-config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# Initialize the dependency container
DEPS = DependencyContainer(CONFIG_PATH)

# Loggers (optional, but can be used)
console_logger, error_logger, analytics_logger = get_loggers(CONFIG)

# Steps to CSV for dry-run (for cleaning and genre)
DRY_RUN_CLEANING_CSV = get_full_log_path(CONFIG, "dry_run_cleaning_file", "csv/dry_run_cleaning.csv")
DRY_RUN_GENRE_CSV = get_full_log_path(CONFIG, "dry_run_genre_file", "csv/dry_run_genre_update.csv")


async def fetch_tracks_direct(artist: str = None) -> List[Dict[str, str]]:
    """
    Directly fetch tracks using AppleScript, bypassing the dependency container.
    
    This is a simplified version used only in dry_run.py.
    """
    console_logger.info(f"Direct fetch of tracks for {'all artists' if artist is None else artist}")
    
    script_path = os.path.join(CONFIG["apple_scripts_dir"], "fetch_tracks.applescript")
    
    if not os.path.exists(script_path):
        console_logger.error(f"AppleScript file not found: {script_path}")
        return []
    
    cmd = ["osascript", script_path]
    if artist:
        cmd.append(artist)
    
    try:
        console_logger.info(f"Executing: {' '.join(cmd)}")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for completion with long timeout
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=900)
        
        if stderr:
            error_str = stderr.decode('utf-8').strip()
            console_logger.warning(f"AppleScript stderr: {error_str}")
        
        if process.returncode != 0:
            console_logger.error(f"AppleScript failed with exit code {process.returncode}")
            return []
        
        raw_data = stdout.decode('utf-8').strip()
        
        if not raw_data:
            console_logger.error("Empty response from AppleScript")
            return []
        
        # Parse the results manually
        tracks = []
        for line in raw_data.split("\n"):
            if not line.strip():
                continue
                
            fields = line.split("~|~")
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
                # Add old_year and new_year if they exist
                if len(fields) > 7:
                    track["old_year"] = fields[7].strip()
                if len(fields) > 8:
                    track["new_year"] = fields[8].strip()
                else:
                    track["new_year"] = ""
                
                tracks.append(track)
        
        console_logger.info(f"Successfully parsed {len(tracks)} tracks")
        return tracks
        
    except asyncio.TimeoutError:
        console_logger.error("AppleScript execution timed out after 15 minutes")
        return []
    except Exception as e:
        console_logger.error(f"Error fetching tracks: {e}")
        return []


async def simulate_cleaning() -> List[Dict[str, str]]:
    """
    Simulate cleaning of track/album names without applying changes.
    Returns a list of simulated cleaning changes, each with relevant fields.

    Returns:
        List[Dict[str, str]]: List of dictionaries describing the 'cleaning' changes.
    """
    simulated_changes = []
    
    # Use direct fetch instead of fetch_tracks_async
    tracks = await fetch_tracks_direct()
    
    if not tracks:
        console_logger.error("Failed to fetch tracks for dry run simulation!")
        return []
        
    console_logger.info(f"Simulating cleaning for {len(tracks)} tracks")
    
    for track in tracks:
        # Skip tracks with prerelease status
        if track.get("trackStatus", "").lower() in ("prerelease", "no longer available"):
            continue
        original_name = track.get("name", "")
        original_album = track.get("album", "")
        cleaned_name, cleaned_album = clean_names(track.get("artist", "Unknown"), original_name, original_album)
        if cleaned_name != original_name or cleaned_album != original_album:
            simulated_changes.append({
                "change_type": "cleaning",
                "track_id": track.get("id", ""),
                "artist": track.get("artist", "Unknown"),
                "original_name": original_name,
                "cleaned_name": cleaned_name,
                "original_album": original_album,
                "cleaned_album": cleaned_album,
                "dateAdded": track.get("dateAdded", ""),
            })
    return simulated_changes


async def simulate_genre_update() -> List[Dict[str, str]]:
    """
    Simulate genre updates without applying changes.
    Returns a list of simulated 'genre_update' changes.

    Returns:
        List[Dict[str, str]]: List of dictionaries describing the 'genre_update' changes.
    """
    simulated_changes = []
    
    # Use direct fetch instead of fetch_tracks_async
    tracks = await fetch_tracks_direct()
    
    if not tracks:
        console_logger.error("Failed to fetch tracks for genre update simulation!")
        return []
    
    # Skip tracks with unusable status
    tracks = [t for t in tracks if t.get("trackStatus", "").lower() not in ("prerelease", "no longer available")]
    
    # Group tracks by artist
    artists: Dict[str, List[Dict[str, str]]] = {}
    for track in tracks:
        artist = track.get("artist", "Unknown")
        artists.setdefault(artist, []).append(track)
    
    # We determine the dominant genre for each artist
    for artist, artist_tracks in artists.items():
        dominant_genre = determine_dominant_genre_for_artist(artist_tracks)
        for track in artist_tracks:
            current_genre = track.get("genre", "Unknown")
            if current_genre != dominant_genre:
                simulated_changes.append({
                    "change_type": "genre_update",
                    "track_id": track.get("id", ""),
                    "artist": artist,
                    "album": track.get("album", "Unknown"),
                    "track_name": track.get("name", ""),
                    "original_genre": current_genre,
                    "simulated_genre": dominant_genre,
                    "dateAdded": track.get("dateAdded", ""),
                })
    return simulated_changes


def save_cleaning_csv(changes: List[Dict[str, str]], file_path: str) -> None:
    """
    Save cleaning changes to a dedicated CSV file.

    Args:
        changes (List[Dict[str, str]]): The list of 'cleaning' changes.
        file_path (str): Full path to the CSV file.
    """
    fieldnames = [
        "change_type",
        "track_id",
        "artist",
        "original_name",
        "cleaned_name",
        "original_album",
        "cleaned_album",
        "dateAdded",
    ]
    console_logger.info(f"Saving cleaning update changes to {file_path}")
    csv_dir = os.path.dirname(file_path)
    if csv_dir and not os.path.exists(csv_dir):
        os.makedirs(csv_dir, exist_ok=True)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in changes:
            writer.writerow(row)
    console_logger.info(f"Cleaning changes saved: {len(changes)} rows.")


def save_genre_csv(changes: List[Dict[str, str]], file_path: str) -> None:
    """
    Save genre update changes to a dedicated CSV file.

    Args:
        changes (List[Dict[str, str]]): The list of 'genre_update' changes.
        file_path (str): Full path to the CSV file.
    """
    fieldnames = [
        "change_type",
        "track_id",
        "artist",
        "album",
        "track_name",
        "original_genre",
        "simulated_genre",
        "dateAdded",
    ]
    console_logger.info(f"Saving genre changes to {file_path}")
    csv_dir = os.path.dirname(file_path)
    if csv_dir and not os.path.exists(csv_dir):
        os.makedirs(csv_dir, exist_ok=True)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in changes:
            writer.writerow(row)
    console_logger.info(f"Genre update changes saved: {len(changes)} rows.")


async def main():
    """
    Main function for dry run simulation:
    1) simulate cleaning changes,
    2) simulate genre update changes,
    3) save them into a combined CSV file.
    """
    cleaning_changes, genre_changes = await asyncio.gather(
        simulate_cleaning(), simulate_genre_update()
    )

    # We use a new feature for a combined report
    from utils.logger import get_full_log_path  # Import from the correct module
    from utils.reports import save_unified_dry_run

    # We get the path for the combined report from the configuration
    dry_run_report_file = get_full_log_path(CONFIG, "dry_run_report_file", "csv/dry_run_combined.csv")
    
    # We save the combined report
    save_unified_dry_run(
        cleaning_changes, 
        genre_changes, 
        dry_run_report_file, 
        console_logger, 
        error_logger
    )

    console_logger.info(f"Dry run simulation completed with {len(cleaning_changes)} cleaning changes and {len(genre_changes)} genre changes.")


if __name__ == "__main__":
    asyncio.run(main())