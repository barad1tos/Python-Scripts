#!/usr/bin/env python3

"""
Reports Module

Provides functions for CSV/HTML report generation and track data management
for music library operations. Handles both file and console output formats.

Main features:
- Track list management and synchronization with persistent storage
- Album cache extraction and integration with track data
- Consolidated change reports (genre, year, metadata changes)
- Analytics reporting with HTML visualization
- Console-friendly formatted output for force mode
- Unified dry run reporting for simulated operations
- CSV import/export with data integrity preservation
"""

import csv
import logging
import os
import time

from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from services.cache_service import CacheService
from utils.logger import ensure_directory, get_full_log_path

def _save_csv(
    data: List[Dict[str, str]],
    fieldnames: List[str],
    file_path: str,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
    data_type: str
) -> None:
    """
    Save the provided data to a CSV file.

    Checks if the target directory for the CSV file exists, and creates it if not.

    :param data: List of dictionaries to save to the CSV file.
    :param fieldnames: List of field names for the CSV file.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :param data_type: Type of data being saved (e.g., "tracks", "changes report").
    """
    ensure_directory(os.path.dirname(file_path))
    console_logger.info(f"Saving {data_type} to CSV: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            # Filter each row to include only keys present in fieldnames
            for row in data:
                filtered_row = {field: row.get(field, "") for field in fieldnames}
                writer.writerow(filtered_row)
        console_logger.info(f"{data_type.capitalize()} saved to {file_path} ({len(data)} entries).")
    except Exception as e:
        error_logger.error(f"Failed to save {data_type}: {e}")

def save_to_csv(
    tracks: List[Dict[str, str]],
    file_path: str,
    console_logger: Optional[logging.Logger] = None,
    error_logger: Optional[logging.Logger] = None
) -> None:
    """
    Save the list of track dictionaries to a CSV file.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
    
    # Updated fieldnames to ensure we capture all necessary fields
    fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded", "trackStatus", "old_year", "new_year"]
    _save_csv(tracks, fieldnames, file_path, console_logger, error_logger, "tracks")

def save_unified_changes_report(
    changes: List[Dict[str, str]], 
    file_path: str, 
    console_logger: logging.Logger, 
    error_logger: logging.Logger,
    force_mode: bool = False
) -> None:
    """
    Save consolidated changes report combining genre, year, and other changes.
    In force mode, prints changes to console instead of saving to file.

    Args:
        changes: List of dictionaries with change data
        file_path: Path to the CSV file
        console_logger: Logger for console output
        error_logger: Logger for error output
        force_mode: If True, prints to console instead of saving to file
    """
    # Define fields for the report
    fieldnames = [
        "change_type",  # New field to indicate type: "genre", "year", "name", etc.
        "artist", 
        "album", 
        "track_name", 
        "old_genre", 
        "new_genre", 
        "old_year",
        "new_year",
        "old_track_name",
        "new_track_name",
        "old_album_name",
        "new_album_name",
        "timestamp"
    ]
    
    # Sort changes by artist and album for readability
    changes_sorted = sorted(changes, key=lambda x: (x.get("artist", "Unknown"), x.get("album", "Unknown")))
    
    # For force mode, print a formatted report to console
    if force_mode:
        console_logger.info("📋 Changes Report:")
        console_logger.info("-" * 80)
        
        # Group changes by type for better presentation
        changes_by_type = {}
        for change in changes_sorted:
            change_type = change.get("change_type", "other")
            if change_type not in changes_by_type:
                changes_by_type[change_type] = []
            changes_by_type[change_type].append(change)
        
        # Print each change type with its own header
        for change_type, type_changes in changes_by_type.items():
            console_logger.info(f"\n🔄 {change_type.upper()} Changes ({len(type_changes)}):")
            
            # Print table header for this type
            if change_type == "genre":
                console_logger.info(f"{'Artist':<30} {'Album':<30} {'Track':<30} {'Old → New'}")
                console_logger.info("-" * 100)
                for change in type_changes:
                    artist = change.get("artist", "")[:28] + ".." if len(change.get("artist", "")) > 30 else change.get("artist", "")
                    album = change.get("album", "")[:28] + ".." if len(change.get("album", "")) > 30 else change.get("album", "")
                    track = change.get("track_name", "")[:28] + ".." if len(change.get("track_name", "")) > 30 else change.get("track_name", "")
                    old_genre = change.get("old_genre", "")
                    new_genre = change.get("new_genre", "")
                    console_logger.info(f"{artist:<30} {album:<30} {track:<30} {old_genre} → {new_genre}")
            
            elif change_type == "year":
                console_logger.info(f"{'Artist':<30} {'Album':<40} {'Old → New'}")
                console_logger.info("-" * 80)
                for change in type_changes:
                    artist = change.get("artist", "")[:28] + ".." if len(change.get("artist", "")) > 30 else change.get("artist", "")
                    album = change.get("album", "")[:38] + ".." if len(change.get("album", "")) > 40 else change.get("album", "")
                    old_year = change.get("old_year", "")
                    new_year = change.get("new_year", "")
                    console_logger.info(f"{artist:<30} {album:<40} {old_year} → {new_year}")
            
            elif change_type == "name":
                console_logger.info(f"{'Artist':<30} {'Track/Album':<40} {'Old → New'}")
                console_logger.info("-" * 80)
                for change in type_changes:
                    artist = change.get("artist", "")[:28] + ".." if len(change.get("artist", "")) > 30 else change.get("artist", "")
                    if change.get("old_track_name"):
                        item_type = "Track"
                        old_name = change.get("old_track_name", "")
                        new_name = change.get("new_track_name", "")
                    else:
                        item_type = "Album"
                        old_name = change.get("old_album_name", "")
                        new_name = change.get("new_album_name", "")
                    
                    old_name = old_name[:38] + ".." if len(old_name) > 40 else old_name
                    new_name = new_name[:38] + ".." if len(new_name) > 40 else new_name
                    console_logger.info(f"{artist:<30} {item_type:<10} {old_name} → {new_name}")
            
            # If we have an unknown change type, show generic info
            else:
                for change in type_changes:
                    console_logger.info(f"Change for {change.get('artist', '')} - {change.get('album', '')} - {change.get('track_name', '')}")
        
        console_logger.info(f"\nTotal: {len(changes)} changes")
        return
    
    # For normal mode, save to CSV file
    _save_csv(changes_sorted, fieldnames, file_path, console_logger, error_logger, "changes report")

def save_changes_report(
    changes: List[Dict[str, str]],
    file_path: str,
    console_logger: Optional[logging.Logger] = None,
    error_logger: Optional[logging.Logger] = None,
    force_mode: bool = False
) -> None:
    """
    Save the list of change dictionaries to a CSV file.
    Enhanced version that supports force mode for console output.

    :param changes: List of dictionaries with change data.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :param force_mode: If True, prints to console instead of saving to file.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
    
    # Add change_type if missing (backward compatibility)
    for change in changes:
        if "change_type" not in change:
            if "new_genre" in change and change.get("new_genre"):
                change["change_type"] = "genre"
            elif "new_year" in change and change.get("new_year"):
                change["change_type"] = "year"
            elif "new_track_name" in change or "new_album_name" in change:
                change["change_type"] = "name"
            else:
                change["change_type"] = "other"
    
    # Use the unified changes report function
    save_unified_changes_report(changes, file_path, console_logger, error_logger, force_mode)

def save_unified_dry_run(
    cleaning_changes: List[Dict[str, str]],
    genre_changes: List[Dict[str, str]],
    file_path: str,
    console_logger: logging.Logger,
    error_logger: logging.Logger
) -> None:
    """
    Save unified dry run report combining cleaning and genre changes.

    Args:
        cleaning_changes: List of dictionaries with cleaning changes
        genre_changes: List of dictionaries with genre changes
        file_path: Path to the CSV file
        console_logger: Logger for console output
        error_logger: Logger for error output
    """
    # Define fields for the combined report
    fieldnames = [
        "change_type",  # "cleaning" or "genre_update"
        "track_id",
        "artist",
        "album",
        "track_name", 
        "original_name",
        "cleaned_name",
        "original_album",
        "cleaned_album",
        "original_genre",
        "simulated_genre",
        "dateAdded"
    ]
    
    # Prepare combined data
    combined_changes = []
    
    # Add cleaning changes
    for change in cleaning_changes:
        change_copy = change.copy()
        if "track_name" not in change_copy and "original_name" in change_copy:
            # Map original_name to track_name for consistency
            change_copy["track_name"] = change_copy["original_name"]
        combined_changes.append(change_copy)
    
    # Add genre changes
    for change in genre_changes:
        combined_changes.append(change)
    
    # Sort changes by artist and album
    combined_changes.sort(key=lambda x: (x.get("artist", "Unknown"), x.get("album", "Unknown")))
    
    # Save to CSV
    _save_csv(combined_changes, fieldnames, file_path, console_logger, error_logger, "dry run report")

def load_track_list(csv_path: str) -> Dict[str, Dict[str, str]]:
    """
    Load the track list from the CSV file into a dictionary. The track ID is used as the key.
    
    :param csv_path: Path to the CSV file.
    :return: Dictionary of track dictionaries.
    """
    track_map = {}
    if not os.path.exists(csv_path):
        return track_map
    logger = logging.getLogger("console_logger")
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tid = row.get("id", "").strip()
                if tid:
                    track_map[tid] = {
                        "id": tid,
                        "name": row.get("name", "").strip(),
                        "artist": row.get("artist", "").strip(),
                        "album": row.get("album", "").strip(),
                        "genre": row.get("genre", "").strip(),
                        "dateAdded": row.get("dateAdded", "").strip(),
                        "trackStatus": row.get("trackStatus", "").strip(),
                        "old_year": row.get("old_year", "").strip(),
                        "new_year": row.get("new_year", "").strip(),
                    }
    except Exception as e:
        logger.error(f"Could not read track_list.csv: {e}")
    return track_map

def load_track_list_with_cache(csv_path: str, cache_path: str, console_logger: logging.Logger) -> Dict[str, Dict[str, str]]:
    """
    Load the track list from CSV and integrate album cache data.
    
    Args:
        csv_path: Path to the track list CSV file
        cache_path: Path to the album cache CSV file
        console_logger: Logger for console output
        
    Returns:
        Dictionary of track dictionaries with integrated cache data
    """
    track_map = {}
    
    # Step 1: Load the album cache into a dictionary for lookup
    album_years = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    artist = row.get("artist", "").strip().lower()
                    album = row.get("album", "").strip().lower()
                    year = row.get("year", "").strip()
                    if artist and album and year:
                        key = f"{artist}|||{album}"
                        album_years[key] = year
            console_logger.info(f"Loaded {len(album_years)} album years from cache")
        except Exception as e:
            console_logger.error(f"Failed to load album cache: {e}")
    
    # Step 2: Load the track list
    if not os.path.exists(csv_path):
        return track_map
        
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tid = row.get("id", "").strip()
                if tid:
                    # Create track dictionary
                    track = {
                        "id": tid,
                        "name": row.get("name", "").strip(),
                        "artist": row.get("artist", "").strip(),
                        "album": row.get("album", "").strip(),
                        "genre": row.get("genre", "").strip(),
                        "dateAdded": row.get("dateAdded", "").strip(),
                        "trackStatus": row.get("trackStatus", "").strip(),
                        "old_year": row.get("old_year", "").strip(),
                        "new_year": row.get("new_year", "").strip(),
                    }
                    
                    # Look up cached year if not already in track
                    if not track["new_year"]:
                        artist_lower = track["artist"].lower()
                        album_lower = track["album"].lower()
                        key = f"{artist_lower}|||{album_lower}"
                        if key in album_years:
                            track["new_year"] = album_years[key]
                            
                    track_map[tid] = track
        console_logger.info(f"Loaded {len(track_map)} tracks from track list")
    except Exception as e:
        console_logger.error(f"Failed to load track list: {e}")
        
    return track_map

def extract_album_cache_from_tracks(
    tracks: List[Dict[str, str]], 
    output_path: str, 
    console_logger: logging.Logger, 
    error_logger: logging.Logger
) -> None:
    """
    Extract album cache data from tracks and save to a CSV file.
    This helps maintain backward compatibility with existing code.
    
    Args:
        tracks: List of track dictionaries
        output_path: Path to save the CSV file
        console_logger: Logger for console output
        error_logger: Logger for error output
    """
    fieldnames = ["artist", "album", "year"]
    album_years = {}
    
    # Extract unique artist/album combinations with years
    for track in tracks:
        artist = track.get("artist", "").strip()
        album = track.get("album", "").strip()
        year = track.get("new_year", "").strip()
        
        if artist and album and year:
            key = f"{artist.lower()}|||{album.lower()}"
            album_years[key] = {
                "artist": artist.lower(),
                "album": album.lower(),
                "year": year
            }
    
    # Save to CSV
    ensure_directory(os.path.dirname(output_path))
    console_logger.info(f"Saving {len(album_years)} album years to {output_path}")
    
    try:
        with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for entry in album_years.values():
                writer.writerow(entry)
                
        console_logger.info(f"Album cache saved with {len(album_years)} entries")
    except Exception as e:
        error_logger.error(f"Failed to save album cache: {e}")

async def sync_track_list_with_current(
    all_tracks: List[Dict[str, str]],
    csv_path: str,
    cache_service: CacheService,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
    partial_sync: bool = False
) -> None:
    """
    Synchronizes the current track list with the data in a CSV file.
    
    This function ensures that album years are stored correctly between runs.
    For each album (key: "artist|album"), if a record exists with a non-empty new_year,
    the album is considered processed and its tracks are skipped.
    
    :param all_tracks: List of track dictionaries to sync.
    :param csv_path: Path to the CSV file.
    :param cache_service: CacheService instance for album year caching.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :param partial_sync: Whether to perform a partial sync (only update new_year if missing).
    """
    console_logger.info(f"Starting sync: fetched {len(all_tracks)} tracks; CSV file: {csv_path}")
    # Loading an existing database
    csv_map = load_track_list(csv_path)
    console_logger.info(f"CSV currently contains {len(csv_map)} tracks before sync.")

    # Identification of albums that are already a year old
    processed_albums = {}
    for track in csv_map.values():
        artist = track.get("artist", "").strip()
        album = track.get("album", "").strip()
        key = f"{artist}|{album}"
        new_year = track.get("new_year", "").strip()
        if new_year:
            processed_albums[key] = new_year
            
            # Additionally save to CSV cache if the value is missing there
            try:
                cached_year = await cache_service.get_album_year_from_cache(artist, album)
                if not cached_year and new_year:
                    await cache_service.store_album_year_in_cache(artist, album, new_year)
                    console_logger.debug(f"Added missing year {new_year} to cache for '{artist} - {album}'")
            except Exception as e:
                error_logger.error(f"Error syncing year to cache for {artist} - {album}: {e}")

    # Update or add tracks
    added_or_updated_count = 0
    current_map = {}
    
    # Preparing new data
    for tr in all_tracks:
        tid = tr.get("id", "").strip()
        if not tid:
            continue
            
        # Make sure that the old_year and new_year fields exist
        tr.setdefault("old_year", "")
        tr.setdefault("new_year", "")
        
        # Check if the album has already been processed (has a year)
        artist = tr.get("artist", "").strip()
        album = tr.get("album", "").strip()
        album_key = f"{artist}|{album}"
        
        # If the album already has a year in the database and this is a partial synchronization,
        # take the year from the database to preserve data integrity
        if partial_sync and album_key in processed_albums:
            tr["new_year"] = processed_albums[album_key]
            
        # Add a track to the current data set
        current_map[tid] = {
            "id": tid,
            "name": tr.get("name", "").strip(),
            "artist": artist,
            "album": album,
            "genre": tr.get("genre", "").strip(),
            "dateAdded": tr.get("dateAdded", "").strip(),
            "trackStatus": tr.get("trackStatus", "").strip(),
            "old_year": tr.get("old_year", "").strip(),
            "new_year": tr.get("new_year", "").strip(),
        }
    
    # Update or add data to CSV-map
    for tid, new_data in current_map.items():
        old_data = csv_map.get(tid)
        if not old_data:
            # Add a new track
            csv_map[tid] = new_data
            added_or_updated_count += 1
        else:
            # Update an existing track
            changed = False
            for field in ["name", "artist", "album", "genre", "dateAdded", "trackStatus", "old_year", "new_year"]:
                if old_data.get(field) != new_data[field]:
                    old_data[field] = new_data[field]
                    changed = True
            if changed:
                added_or_updated_count += 1
                
    console_logger.info(f"Added/Updated {added_or_updated_count} tracks in CSV.")
    
    # Generate the final list and write to CSV
    final_list = list(csv_map.values())
    console_logger.info(f"Final CSV track count after sync: {len(final_list)}")
    fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded", "trackStatus", "old_year", "new_year"]
    _save_csv(final_list, fieldnames, csv_path, console_logger, error_logger, "tracks")
    
    # Also update the album cache file for backward compatibility
    cache_path = os.path.join(os.path.dirname(csv_path), "..", "csv", "cache_albums.csv")
    extract_album_cache_from_tracks(final_list, cache_path, console_logger, error_logger)

def save_html_report(
    events: List[Dict[str, Any]],
    call_counts: Dict[str, int],
    success_counts: Dict[str, int],
    decorator_overhead: Dict[str, float],
    config: Dict[str, Any],
    console_logger: Optional[logging.Logger] = None,
    error_logger: Optional[logging.Logger] = None,
    group_successful_short_calls: bool = False,
    force_mode: bool = False
) -> None:
    """
    Generate an HTML report from the provided analytics data.
    The report includes a summary of function call counts, success rates, and decorator overhead,
    as well as detailed event data and grouped short successful calls.

    :param events: List of event dictionaries.
    :param call_counts: Dictionary of function call counts.
    :param success_counts: Dictionary of successful function call counts.
    :param decorator_overhead: Dictionary of decorator overhead times.
    :param config: Configuration dictionary.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :param group_successful_short_calls: Whether to group short successful calls in the report.
    :param force_mode: Whether the script is running in force mode.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
        
    # Additional logging for diagnostics
    console_logger.info(f"Starting HTML report generation with {len(events)} events, {len(call_counts)} function counts")
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    logs_base_dir = config.get("logs_base_dir", ".")
    reports_dir = os.path.join(logs_base_dir, "analytics")
    os.makedirs(reports_dir, exist_ok=True)
    
    # Getting the path for the HTML file based on run mode
    if force_mode:
        report_file = os.path.join(reports_dir, "analytics_full.html")
    else:
        report_file = os.path.join(reports_dir, "analytics_incremental.html")
    
    console_logger.debug(f"Will save HTML report to: {report_file}")
    
    # Setting colors and thresholds
    duration_thresholds = config.get("analytics", {}).get("duration_thresholds", {"short_max": 2, "medium_max": 5, "long_max": 10})
    colors = config.get("analytics", {}).get("colors", {"short": "#90EE90", "medium": "#D3D3D3", "long": "#FFB6C1"})
    
    # Check for data availability
    if not events and not call_counts:
        console_logger.warning("No analytics data available for report - creating empty template")
        # Create an empty template with a message
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Analytics Report for {date_str}</title>
    <style>
        table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 0.95em;
        }}
        th, td {{
            border: 1px solid #dddddd;
            text-align: left;
            padding: 6px;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        .error {{
            background-color: #ffcccc;
        }}
    </style>
</head>
<body>
    <h2>Analytics Report for {date_str}</h2>
    <p><strong>No analytics data was collected during this run.</strong></p>
    <p>Possible reasons:</p>
    <ul>
        <li>Script executed in dry-run mode without analytics collection</li>
        <li>No decorated functions were called</li>
        <li>Decorator failed to log events</li>
    </ul>
</body>
</html>"""
        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write(html_content)
            console_logger.info(f"Empty analytics HTML report saved to {report_file}.")
            return
        except Exception as e:
            error_logger.error(f"Failed to save empty HTML report: {e}")
            return
    
    # Function for determining the color
    def get_color(duration: float) -> str:
        if duration <= duration_thresholds.get("short_max", 2):
            return ""
        elif duration <= duration_thresholds.get("medium_max", 5):
            return colors.get("medium", "#D3D3D3")
        elif duration <= duration_thresholds.get("long_max", 10):
            return colors.get("long", "#FFB6C1")
        else:
            return colors.get("long", "#FFB6C1")
    
    # Data preparation
    grouped_short_success = {}
    big_or_fail_events = []
    short_max = duration_thresholds.get("short_max", 2)
    
    # Grouping short successful calls
    if group_successful_short_calls:
        for ev in events:
            try:
                duration = ev["Duration (s)"]
                success = ev["Success"]
                
                if (not success) or (duration > short_max):
                    big_or_fail_events.append(ev)
                else:
                    key = (ev["Function"], ev["Event Type"])
                    if key not in grouped_short_success:
                        grouped_short_success[key] = {"count": 0, "total_duration": 0.0}
                    grouped_short_success[key]["count"] += 1
                    grouped_short_success[key]["total_duration"] += duration
            except KeyError as e:
                error_logger.error(f"Missing key in event data: {e}, event: {ev}")
                big_or_fail_events.append(ev)  # Add an event if it has no data
    else:
        big_or_fail_events = events
    
    # Beginning of HTML creation
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Analytics Report for {date_str}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
        }}
        h2, h3 {{
            color: #333;
            border-bottom: 1px solid #ddd;
            padding-bottom: 10px;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            font-size: 0.95em;
            margin-bottom: 20px;
        }}
        th, td {{
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }}
        th {{
            background-color: #f2f2f2;
            position: sticky;
            top: 0;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .error {{
            background-color: #ffcccc;
        }}
        .summary {{
            background-color: #e6f3ff;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .run-type {{
            font-weight: bold;
            color: #0066cc;
        }}
    </style>
</head>
<body>
    <h2>Analytics Report for {date_str}</h2>
    <div class="summary">
        <p class="run-type">Run type: {'Full scan' if force_mode else 'Incremental update'}</p>
        <p><strong>Total functions:</strong> {len(call_counts)}</p>
        <p><strong>Total events:</strong> {len(events)}</p>
        <p><strong>Success rate:</strong> {sum(success_counts.values())/sum(call_counts.values())*100 if sum(call_counts.values()) else 0:.1f}%</p>
    </div>
    
    <h3>Grouped Short & Successful Calls</h3>
    <table>
        <tr>
            <th>Function</th>
            <th>Event Type</th>
            <th>Count</th>
            <th>Avg Duration (s)</th>
            <th>Total Duration (s)</th>
        </tr>"""
    
    # Adding groups of successful calls
    if group_successful_short_calls and grouped_short_success:
        for (fun, evt), val in grouped_short_success.items():
            cnt = val["count"]
            total_dur = val["total_duration"]
            avg_dur = round(total_dur / cnt, 4) if cnt > 0 else 0
            html_content += f"""
        <tr>
            <td>{fun}</td>
            <td>{evt}</td>
            <td>{cnt}</td>
            <td>{avg_dur}</td>
            <td>{round(total_dur, 4)}</td>
        </tr>"""
    else:
        html_content += """
        <tr><td colspan="5">No short successful calls found or grouping disabled.</td></tr>"""
    
    # Adding detailed calls
    html_content += """
    </table>
    <h3>Detailed Calls (Errors or Long Calls)</h3>
    <table>
        <tr>
            <th>Function</th>
            <th>Event Type</th>
            <th>Start Time</th>
            <th>End Time</th>
            <th>Duration (s)</th>
            <th>Success</th>
        </tr>"""
    
    if big_or_fail_events:
        for ev in big_or_fail_events:
            try:
                duration = ev["Duration (s)"]
                color = get_color(duration)
                success = "Yes" if ev["Success"] else "No"
                row_class = "error" if not ev["Success"] else ""
                
                html_content += f"""
        <tr class="{row_class}">
            <td>{ev.get('Function', 'Unknown')}</td>
            <td>{ev.get('Event Type', 'Unknown')}</td>
            <td>{ev.get('Start Time', 'Unknown')}</td>
            <td>{ev.get('End Time', 'Unknown')}</td>
            <td{f' style="background-color: {color};"' if color else ''}>{duration}</td>
            <td>{success}</td>
        </tr>"""
            except KeyError as e:
                error_logger.error(f"Error formatting event: {e}, event data: {ev}")
    else:
        html_content += """
        <tr><td colspan="6">No detailed calls to display.</td></tr>"""
    
    # Adding totals
    html_content += """
    </table>
    <h3>Summary</h3>
    <table>
        <tr>
            <th>Function</th>
            <th>Call Count</th>
            <th>Success Count</th>
            <th>Success Rate (%)</th>
            <th>Total Decorator Overhead (s)</th>
        </tr>"""
    
    if call_counts:
        for function, count in call_counts.items():
            succ = success_counts.get(function, 0)
            success_rate = (succ / count * 100) if count else 0
            overhead = decorator_overhead.get(function, 0)
            
            html_content += f"""
        <tr>
            <td>{function}</td>
            <td>{count}</td>
            <td>{succ}</td>
            <td>{success_rate:.2f}</td>
            <td>{round(overhead, 4)}</td>
        </tr>"""
    else:
        html_content += """
        <tr><td colspan="5">No function calls recorded.</td></tr>"""
    
    html_content += """
    </table>
</body>
</html>"""
    
    # Saving the report
    try:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        console_logger.info(f"Analytics HTML report saved to {report_file}.")
    except Exception as e:
        error_logger.error(f"Failed to save HTML report: {e}")