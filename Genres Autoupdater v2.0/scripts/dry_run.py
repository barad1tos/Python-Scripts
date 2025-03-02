#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dry Run Module

Simulates upcoming changes (cleaning and genre updates)
without applying them, outputting each type of changes
into a separate CSV report.

The CSV files are overwritten each time.
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

# Add path to music_genre_updater and scripts
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_dir, "..")
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from scripts.logger import get_full_log_path, get_loggers
from music_genre_updater import fetch_tracks_async, clean_names, determine_dominant_genre_for_artist

# Load the configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "..", "my-config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# Loggers (optional, but can be used)
console_logger, error_logger, analytics_logger = get_loggers(CONFIG)

# Steps to CSV for dry-run (for cleaning ta genre)
DRY_RUN_CLEANING_CSV = get_full_log_path(CONFIG, "dry_run_cleaning_file", "csv/dry_run_cleaning.csv")
DRY_RUN_GENRE_CSV = get_full_log_path(CONFIG, "dry_run_genre_file", "csv/dry_run_genre_update.csv")


async def simulate_cleaning() -> List[Dict[str, str]]:
    """
    Simulate cleaning of track/album names without applying changes.
    Returns a list of simulated cleaning changes, each with relevant fields.

    Returns:
        List[Dict[str, str]]: List of dictionaries describing the 'cleaning' changes.
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
    Returns a list of simulated 'genre_update' changes.

    Returns:
        List[Dict[str, str]]: List of dictionaries describing the 'genre_update' changes.
    """
    simulated_changes = []
    tracks = await fetch_tracks_async()
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
    console_logger.info(f"Saving cleaning changes to {file_path}")
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
    console_logger.info(f"Saving genre update changes to {file_path}")
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
    3) save them into two separate CSV files.
    """
    cleaning_changes = await simulate_cleaning()
    genre_changes = await simulate_genre_update()

    save_cleaning_csv(cleaning_changes, DRY_RUN_CLEANING_CSV)
    save_genre_csv(genre_changes, DRY_RUN_GENRE_CSV)

    console_logger.info("Dry run simulation completed.")


if __name__ == "__main__":
    asyncio.run(main())