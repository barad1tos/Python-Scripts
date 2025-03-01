#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dry Run Module

This module simulates upcoming changes (cleaning and genre updates)
without applying them, and outputs the simulated changes in a CSV report.
The report is overwritten each time.
"""

import sys
import os
import asyncio
import csv
import logging

from datetime import datetime
from typing import Dict, List

import yaml

from logging.handlers import RotatingFileHandler

# Add parent directory to sys.path to allow importing music_genre_updater module
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_dir, "..")
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from scripts.logger import get_full_log_path, get_loggers
from music_genre_updater import fetch_tracks_async, clean_names, determine_dominant_genre_for_artist

# Load configuration from my-config.yaml
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "..", "my-config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# Use config to determine where to store the dry run CSV report and log file
DRY_RUN_REPORT_CSV = get_full_log_path(CONFIG, "dry_run_report_file", "main logic/dry_run_report.csv")
dry_run_log_file = get_full_log_path(CONFIG, "dry_run_log_file", "main logic/dry_run.log")

# Initialize dry run logger
dry_run_logger = logging.getLogger("dry_run_logger")

if not dry_run_logger.handlers:
    dry_run_logger.setLevel(logging.INFO)
    fh = RotatingFileHandler(dry_run_log_file, maxBytes=CONFIG["logging"].get("max_bytes", 5000000), backupCount=CONFIG["logging"].get("backup_count", 3), encoding="utf-8")

    from scripts.logger import ColoredFormatter
    fh.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s", include_separator=True))
    dry_run_logger.addHandler(fh)
    dry_run_logger.propagate = False

async def simulate_cleaning() -> List[Dict[str, str]]:
    """
    Simulate cleaning of track and album names without applying changes.
    Returns a list of simulated cleaning changes.
    """
    simulated_changes = []
    tracks = await fetch_tracks_async()
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
    Returns a list of simulated genre update changes.
    """
    simulated_changes = []
    tracks = await fetch_tracks_async()
    # Skip tracks with prerelease or no longer available status
    tracks = [track for track in tracks if track.get("trackStatus", "").lower() not in ("prerelease", "no longer available")]
    
    # Group tracks by artist
    artists: Dict[str, List[Dict[str, str]]] = {}
    for track in tracks:
        artist = track.get("artist", "Unknown")
        artists.setdefault(artist, []).append(track)
    # For each artist, determine the dominant genre using the new logic
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

async def main():
    """
    Main function for dry run simulation.
    """
    cleaning_changes = await simulate_cleaning()
    genre_changes = await simulate_genre_update()
    all_changes = cleaning_changes + genre_changes
    # Sort changes by artist and dateAdded
    all_changes.sort(key=lambda x: (x.get("artist", ""), x.get("dateAdded", "")))
    
    # Compute the union of all keys across change dictionaries
    all_keys = set()
    for change in all_changes:
        all_keys.update(change.keys())
    # Sort fieldnames for consistency
    fieldnames = sorted(all_keys)
    
    # Ensure every change dict has all keys (fill missing with empty string)
    for change in all_changes:
        for key in fieldnames:
            if key not in change:
                change[key] = ""
    
    with open(DRY_RUN_REPORT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_changes)
    print(f"Dry run simulation completed. Report saved to {DRY_RUN_REPORT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())