#!/usr/bin/env python3

"""Reports Module.

Handles CSV/HTML report generation and track data management for music library operations.
Provides both file operations and formatted console output with support for caching.

Main features:
- Track list management with persistent storage and synchronization
- Consolidated reporting for genres, years, and track/album naming
- Interactive console output with color formatting
- CSV data handling with validation
- Unified interface for actual and simulated operations
- HTML analytics with performance metrics

Key functions:
- save_to_csv: Save track metadata to CSV files
- save_unified_changes_report: Generate formatted change reports
- load_track_list: Load and validate track data
- sync_track_list_with_current: Sync data between runs
- save_unified_dry_run: Create reports for simulations
- save_html_report: Generate HTML analytics

Note: Uses CacheService for album year caching.
"""

import csv
import logging
import os

from collections import defaultdict
from datetime import datetime
from typing import Any

from services.cache_service import CacheService
from utils.logger import ensure_directory, get_full_log_path

class Color:
    """ANSI color codes for console output."""

    RED = "\033[31m"
    YELLOW = "\033[33m"
    GREEN = "\033[32m"
    RESET = "\033[0m"

class ChangeType:
    """Enumeration of change types."""

    GENRE = "genre"
    YEAR = "year"
    NAME = "name"
    OTHER = "other"

class Key:
    """Enumeration of key names for CSV fields."""

    CHANGE_TYPE = "change_type"
    ARTIST = "artist"
    ALBUM = "album"
    TRACK_NAME = "track_name"
    OLD_GENRE = "old_genre"
    NEW_GENRE = "new_genre"
    OLD_YEAR = "old_year"
    NEW_YEAR = "new_year"
    OLD_TRACK_NAME = "old_track_name"
    NEW_TRACK_NAME = "new_track_name"
    OLD_ALBUM_NAME = "old_album_name"
    NEW_ALBUM_NAME = "new_album_name"
    TIMESTAMP = "timestamp"

class Format:
    """Enumeration of formatting constants."""

    COL_WIDTH_30 = 30
    COL_WIDTH_40 = 40
    COL_WIDTH_38 = 38
    COL_WIDTH_10 = 10
    SEPARATOR_80 = 80
    SEPARATOR_100 = 100
    TRUNCATE_SUFFIX = ".."
    ARROW = "→"
    HEADER_OLD_NEW = "Old → New"
    HEADER_ITEM_TYPE = "Item Type"
    HEADER_ITEM_NAME = "Item Name"
    ITEM_TYPE_TRACK = "Track"
    ITEM_TYPE_ALBUM = "Album"
    ITEM_TYPE_OTHER = "Other"

class Misc:
    """Enumeration of miscellaneous constants."""

    CHANGES_REPORT_TYPE = "changes report"
    EMOJI_REPORT = "📋"
    EMOJI_CHANGE = "🔄"
    UNKNOWN = "Unknown"
    UNKNOWN_ARTIST = "Unknown Artist"
    UNKNOWN_ALBUM = "Unknown Album"
    UNKNOWN_TRACK = "Unknown Track"


def _save_csv(
    data: list[dict[str, str]],
    fieldnames: list[str],
    file_path: str,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
    data_type: str,
) -> None:
    """Save the provided data to a CSV file.

    Checks if the target directory for the CSV file exists, and creates it if not.
    Uses atomic write pattern with a temporary file.

    :param data: List of dictionaries to save to the CSV file.
    :param fieldnames: List of field names for the CSV file.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :param data_type: Type of data being saved (e.g., "tracks", "changes report").
    """
    ensure_directory(os.path.dirname(file_path))
    console_logger.info(f"Saving {data_type} to CSV: {file_path}")

    temp_file_path = f"{file_path}.tmp"

    try:
        with open(temp_file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            # Filter each row to include only keys present in fieldnames
            for row in data:
                filtered_row = {field: row.get(field, "") for field in fieldnames}
                writer.writerow(filtered_row)

        # Atomic rename
        # On Windows, os.replace provides atomic replacement if the destination exists
        # On POSIX, os.rename is atomic
        os.replace(temp_file_path, file_path)

        console_logger.info(
            f"{data_type.capitalize()} saved to {file_path} ({len(data)} entries)."
        )
    except Exception as e:
        error_logger.error(f"Failed to save {data_type}: {e}")
        # Clean up temporary file in case of error
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except OSError as cleanup_e:
                error_logger.warning(
                    f"Failed to remove temporary file {temp_file_path}: {cleanup_e}"
                )


def save_to_csv(
    tracks: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger | None = None,
    error_logger: logging.Logger | None = None,
) -> None:
    """Save the list of track dictionaries to a CSV file."""
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    # Updated fieldnames to ensure we capture all necessary fields
    # These fields should match the structure expected by load_track_list
    fieldnames = [
        "id",
        "name",
        "artist",
        "album",
        "genre",
        "dateAdded",
        "trackStatus",
        "old_year",
        "new_year",
    ]
    _save_csv(tracks, fieldnames, file_path, console_logger, error_logger, "tracks")


def save_unified_changes_report(
    changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
) -> None:
    """Print a dynamically sized, formatted summary of changes to the console.

    AND save the full details to a CSV file.
    """
    if not changes:
        console_logger.info("No changes to report.")
        return

    changes_sorted = sorted(
        changes, key=lambda x: (x.get(Key.ARTIST, "Unknown"), x.get(Key.ALBUM, "Unknown"))
    )

    console_logger.info(f"\n{Misc.EMOJI_REPORT} Changes Summary:")

    changes_by_type: dict[str, list[dict[str, str]]] = defaultdict(list)
    for change in changes_sorted:
        change_type_val = change.get(Key.CHANGE_TYPE, "other")
        changes_by_type[change_type_val].append(change)

    for change_type, type_changes in changes_by_type.items():
        if not type_changes:
            continue

        console_logger.info(
            f"\n{Misc.EMOJI_CHANGE} {change_type.upper().replace('_', ' ')} Changes ({len(type_changes)}):"
        )

        # 1. Define headers and data keys for the table
        headers_map = {}
        if "genre" in change_type:
            headers_map = {
                "Artist": "artist",
                "Album": "album",
                "Track": "track_name",
                "Old Genre": "original_genre",
                "New Genre": "new_genre"
            }
        elif "year" in change_type:
            headers_map = {
                "Artist": "artist",
                "Album": "album",
                "Old Year": "old_year",
                "New Year": "new_year"
            }

        elif "cleaning" in change_type:
            headers_map = {
                "Artist": "artist",
                "Original Album": "original_album",
                "Cleaned Album": "cleaned_album",
                "Original Track": "original_name",
                "Cleaned Track": "cleaned_name",
            }

        elif "name" in change_type:
            headers_map = {
                "Artist": "artist",
                "Item Type": "item_type",
                "Old Name": "old_name",
                "New Name": "new_name"
            }

        if not headers_map:
            for change in type_changes:
                console_logger.info(f"  Other Change: {change}")
            continue

        # 2. Calculate maximum width for each column
        col_widths = {header: len(header) for header in headers_map.keys()}
        for change in type_changes:
            for header, data_key in headers_map.items():
                # Handle special cases for composite fields
                if data_key == "item_type":
                    cell_value = "Track" if "old_track_name" in change or "new_track_name" in change else "Album"
                elif data_key == "old_name":
                    cell_value = change.get("old_track_name") or change.get("old_album_name", "")
                elif data_key == "new_name":
                    cell_value = change.get("new_track_name") or change.get("new_album_name", "")
                else:
                    cell_value = str(change.get(data_key, ""))

                col_widths[header] = max(col_widths[header], len(cell_value))

        # 3. Print header with calculated width
        padding = 2
        header_line = " | ".join([f"{name.upper():<{col_widths[name] + padding}}" for name in headers_map.keys()])
        console_logger.info(header_line)
        console_logger.info("-" * len(header_line))

        # 4. Print rows of the table
        for change in type_changes:
            row_values = []
            for header, data_key in headers_map.items():
                if data_key == "item_type":
                    cell_value = "Track" if "old_track_name" in change or "new_track_name" in change else "Album"
                elif data_key == "old_name":
                    cell_value = change.get("old_track_name") or change.get("old_album_name", "")
                elif data_key == "new_name":
                    cell_value = change.get("new_track_name") or change.get("new_album_name", "")
                else:
                    cell_value = str(change.get(data_key, ""))
                row_values.append(f"{cell_value:<{col_widths[header] + padding}}")
            console_logger.info(" | ".join(row_values))

    console_logger.info("-" * Format.SEPARATOR_100)

    # --- Part 2: Always save report to CSV ---
    fieldnames = [
        Key.CHANGE_TYPE, Key.ARTIST, Key.ALBUM, Key.TRACK_NAME,
        Key.OLD_GENRE, Key.NEW_GENRE, Key.OLD_YEAR, Key.NEW_YEAR,
        Key.OLD_TRACK_NAME, Key.NEW_TRACK_NAME, Key.OLD_ALBUM_NAME,
        Key.NEW_ALBUM_NAME, Key.TIMESTAMP,
    ]

    ensure_directory(os.path.dirname(file_path), error_logger)
    _save_csv(
        changes_sorted,
        fieldnames,
        file_path,
        console_logger,
        error_logger,
        "changes report",
    )


def save_changes_report(
    changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger | None = None,
    error_logger: logging.Logger | None = None,
    add_timestamp: bool = False,
) -> None:
    """Save the list of change dictionaries to a CSV file.

    By default, it overwrites the specified file. If `add_timestamp` is True,
    it appends a timestamp to the filename to preserve previous reports.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

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

    final_path = file_path
    if add_timestamp:
        base, ext = os.path.splitext(file_path)
        timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_path = f"{base}_{timestamp_suffix}{ext}"

    save_unified_changes_report(
        changes, final_path, console_logger, error_logger
    )


def save_changes_csv(
    changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger | None = None,
    error_logger: logging.Logger | None = None,
    add_timestamp: bool = False,
) -> None:
    """Compatibility wrapper for saving change reports in CSV format."""
    save_changes_report(
        changes,
        file_path,
        console_logger,
        error_logger,
        add_timestamp,
    )


def save_unified_dry_run(
    cleaning_changes: list[dict[str, str]],
    genre_changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
) -> None:
    """Save unified dry run report combining cleaning and genre changes.

    Args:
        cleaning_changes: List of dictionaries with cleaning changes
        genre_changes: List of dictionaries with genre changes
        file_path: Path to the CSV file
        console_logger: Logger for console output
        error_logger: Logger for error output

    """
    # Define fields for the combined report
    fieldnames = [
        "change_type",
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
        "original_year",
        "simulated_year",
        "dateAdded",
        "timestamp",
    ]

    # Prepare combined changes list
    combined_changes = []

    # Add cleaning changes
    for change in cleaning_changes:
        change_copy = change.copy()
        # Ensure change_type is set for cleaning changes
        change_copy["change_type"] = "cleaning"
        # Map original_name to track_name for consistency if needed (based on original change dict structure)
        if "track_name" not in change_copy and "original_name" in change_copy:
            change_copy["track_name"] = change_copy["original_name"]
        combined_changes.append(change_copy)

    # Add genre and year changes
    all_other_changes = genre_changes
    for change in all_other_changes:
        change_copy = change.copy()

        # Determine change type if not set
        if "change_type" not in change_copy:
            if "new_genre" in change_copy:
                change_copy["change_type"] = "genre_update"
            elif "new_year" in change_copy:
                change_copy["change_type"] = "year_update"

        # Process genre changes
        if change_copy.get("change_type") == "genre_update":
            if "new_genre" in change_copy:
                change_copy["simulated_genre"] = change_copy.pop("new_genre")
            if "old_genre" in change_copy:
                change_copy["original_genre"] = change_copy.pop("old_genre")

        # Process year changes
        elif change_copy.get("change_type") == "year_update":
            if "new_year" in change_copy:
                change_copy["simulated_year"] = change_copy.pop("new_year")
            if "old_year" in change_copy:
                change_copy["original_year"] = change_copy.pop("old_year")

        combined_changes.append(change_copy)

    # Sort changes by artist and album
    combined_changes.sort(
        key=lambda x: (x.get("artist", "Unknown"), x.get("album", "Unknown"))
    )

    # Save to CSV
    # Ensure directory exists before saving
    ensure_directory(os.path.dirname(file_path), error_logger)
    _save_csv(
        combined_changes,
        fieldnames,
        file_path,
        console_logger,
        error_logger,
        "dry run report",
    )


def load_track_list(csv_path: str) -> dict[str, dict[str, str]]:
    """Load the track list from the CSV file into a dictionary. The track ID is used as the key.

    Reads columns: id, name, artist, album, genre, dateAdded, trackStatus, old_year, new_year.

    :param csv_path: Path to the CSV file.
    :return: Dictionary of track dictionaries.
    """
    track_map: dict[str, dict[str, str]] = {}
    if not os.path.exists(csv_path):
        return track_map
    logger = logging.getLogger(
        "console_logger"
    )  # Use console_logger for loading info/errors
    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Define expected fieldnames to read, including old_year and new_year
            expected_fieldnames = [
                "id",
                "name",
                "artist",
                "album",
                "genre",
                "dateAdded",
                "trackStatus",
                "old_year",
                "new_year",
            ]
            # Check if the CSV header matches expected fieldnames
            if reader.fieldnames is None:
                logger.warning(f"CSV file {csv_path} is empty or has no header.")
                return track_map  # Return empty map if no header

            if not all(field in reader.fieldnames for field in expected_fieldnames):
                logger.warning(
                    f"CSV header in {csv_path} does not match expected fieldnames. "
                    f"Expected: {expected_fieldnames}, Found: {reader.fieldnames}. "
                    f"Attempting to load with available fields."
                )
                # Adjust expected fieldnames to only include those found in the CSV header
                actual_fieldnames = reader.fieldnames if reader.fieldnames else []
                fields_to_read = [
                    field for field in expected_fieldnames if field in actual_fieldnames
                ]
            else:
                fields_to_read = (
                    expected_fieldnames  # Use all expected fields if header matches
                )

            for row in reader:
                tid = row.get("id", "").strip()
                if tid:
                    # Create track dictionary, getting values for fields_to_read
                    track = {
                        field: row.get(field, "").strip() for field in fields_to_read
                    }
                    # Ensure all expected fields are present in the track dictionary, even if empty
                    for field in expected_fieldnames:
                        track.setdefault(field, "")
                    track_map[tid] = track

        logger.info(f"Loaded {len(track_map)} tracks from track_list.csv.")
    except Exception as e:
        logger.error(f"Could not read track_list.csv: {e}")
    return track_map


async def sync_track_list_with_current(
    all_tracks: list[dict[str, str]],
    csv_path: str,
    cache_service: CacheService,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
    partial_sync: bool = False,
) -> None:
    """Synchronize the current track list with the data in a CSV file.

    Args:
        all_tracks: List of track dictionaries to sync.
        csv_path: Path to the CSV file.
        cache_service: CacheService instance for album year caching.
        console_logger: Logger for console output.
        error_logger: Logger for error output.
        partial_sync: Whether to perform a partial sync (only update new_year if missing).

    """
    console_logger.info(
        f"Starting sync: fetched {len(all_tracks)} tracks; CSV file: {csv_path}"
    )
    # Loading an existing database from the main track list CSV
    csv_map = load_track_list(csv_path)
    console_logger.info(f"CSV currently contains {len(csv_map)} tracks before sync.")

    # Identification of albums that are already processed (have a year in the CSV)
    # We still need this to respect partial_sync logic
    processed_albums_in_csv = {}
    for track in csv_map.values():
        artist = track.get("artist", "").strip()
        album = track.get("album", "").strip()
        new_year = track.get("new_year", "").strip()
        if artist and album and new_year:
            # Use the same key generation logic as CacheService for consistency
            album_key = cache_service._generate_album_key(artist, album)
            processed_albums_in_csv[album_key] = new_year

    # Update or add tracks
    added_or_updated_count = 0
    current_map = {}  # Map of tracks from the current run

    # Preparing new data based on fetched tracks
    for tr in all_tracks:
        tid = tr.get("id", "").strip()
        if not tid:
            continue

        # Ensure that the old_year and new_year fields exist in the track dictionary
        # This is important before trying to access or update them
        tr.setdefault("old_year", "")
        tr.setdefault("new_year", "")

        # Check if the album has already been processed (has a year in the *CSV*)
        # and if this is a partial synchronization.
        artist = tr.get("artist", "").strip()
        album = tr.get("album", "").strip()
        # Use the same key generation logic as CacheService for consistency
        album_key = cache_service._generate_album_key(artist, album)

        # If the album already has a year in the database (CSV) and this is a partial synchronization,
        # take the year from the database to preserve data integrity.
        # This prevents overwriting a potentially correct year from a previous full run
        # with an empty or less reliable year from a new incremental fetch.
        if partial_sync and album_key in processed_albums_in_csv:
            tr["new_year"] = processed_albums_in_csv[album_key]
            # Also ensure this year is in the CacheService's in-memory cache if it's not already
            # This helps keep the CacheService's in-memory data consistent with the CSV
            try:
                cached_year = await cache_service.get_album_year_from_cache(
                    artist, album
                )
                if not cached_year or cached_year != tr["new_year"]:
                    await cache_service.store_album_year_in_cache(
                        artist, album, tr["new_year"]
                    )
                    # console_logger.debug(f"Synced year {tr['new_year']} from CSV to cache for '{artist} - {album}'") # Optional debug log
            except Exception as e:
                error_logger.error(
                    f"Error syncing year from CSV to cache for {artist} - {album}: {e}"
                )

        # Add the track data from the current run to the map
        current_map[tid] = {
            "id": tid,
            "name": tr.get("name", "").strip(),
            "artist": artist,  # Use the stripped artist name
            "album": album,  # Use the stripped album name
            "genre": tr.get("genre", "").strip(),
            "dateAdded": tr.get("dateAdded", "").strip(),
            "trackStatus": tr.get("trackStatus", "").strip(),
            "old_year": tr.get("old_year", "").strip(),
            "new_year": tr.get(
                "new_year", ""
            ).strip(),  # Use the new_year value, potentially updated by partial_sync logic above
        }

    # Update or add data to CSV-map based on current_map
    for tid, new_data in current_map.items():
        old_data = csv_map.get(tid)
        if not old_data:
            # Add a new track
            csv_map[tid] = new_data
            added_or_updated_count += 1
        else:
            # Update an existing track if any relevant field has changed
            changed = False
            # List of fields to check for changes when updating an existing track
            fields_to_check = [
                "name",
                "artist",
                "album",
                "genre",
                "dateAdded",
                "trackStatus",
                "old_year",
                "new_year",
            ]
            for field in fields_to_check:
                # Use .get() with a default to handle cases where a field might be missing in old_data
                if old_data.get(field, "") != new_data.get(field, ""):
                    old_data[field] = new_data.get(
                        field, ""
                    )  # Update the field in the old_data dict
                    changed = True
            if changed:
                added_or_updated_count += 1

    console_logger.info(f"Added/Updated {added_or_updated_count} tracks in CSV.")

    # Generate the final list from the updated csv_map and write to CSV
    final_list = list(csv_map.values())
    console_logger.info(f"Final CSV track count after sync: {len(final_list)}")
    # Define the fieldnames for the output CSV file
    fieldnames = [
        "id",
        "name",
        "artist",
        "album",
        "genre",
        "dateAdded",
        "trackStatus",
        "old_year",
        "new_year",
    ]
    _save_csv(final_list, fieldnames, csv_path, console_logger, error_logger, "tracks")

    # Removed the call to extract_album_cache_from_tracks
    # The CacheService is now responsible for its own persistence.


# Removed load_track_list_with_cache as its logic is now handled by CacheService or main flow
# def load_track_list_with_cache(...): pass # REMOVED


# Removed extract_album_cache_from_tracks as its logic is now handled by CacheService
# def extract_album_cache_from_tracks(...): pass # REMOVED


def save_html_report(
    events: list[dict[str, Any]],
    call_counts: dict[str, int],
    success_counts: dict[str, int],
    decorator_overhead: dict[str, float],
    config: dict[str, Any],
    console_logger: logging.Logger | None = None,
    error_logger: logging.Logger | None = None,
    group_successful_short_calls: bool = False,
    force_mode: bool = False,
) -> None:
    """Generate an HTML report from the provided analytics data.

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
    console_logger.info(
        f"Starting HTML report generation with {len(events)} events, {len(call_counts)} function counts"
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    logs_base_dir = config.get("logs_base_dir", ".")
    reports_dir = os.path.join(logs_base_dir, "analytics")
    os.makedirs(reports_dir, exist_ok=True)

    # Getting the path for the HTML file based on run mode
    # Use get_full_log_path for consistency
    report_file = get_full_log_path(
        config,
        "analytics_html_report_file",
        os.path.join(
            "analytics",
            "analytics_full.html" if force_mode else "analytics_incremental.html",
        ),
    )

    console_logger.debug(f"Will save HTML report to: {report_file}")

    # Setting colors and thresholds
    duration_thresholds = config.get("analytics", {}).get(
        "duration_thresholds", {"short_max": 2, "medium_max": 5, "long_max": 10}
    )
    # Removed colors as per user's plan
    # colors = config.get("analytics", {}).get("colors", {"short": "#90EE90", "medium": "#D3D3D3", "long": "#FFB6C1"})

    # Check for data availability
    if not events and not call_counts:
        console_logger.warning(
            "No analytics data available for report - creating empty template"
        )
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
            # Ensure directory exists before saving
            os.makedirs(os.path.dirname(report_file), exist_ok=True)
            with open(report_file, "w", encoding="utf-8") as f:
                f.write(html_content)
            console_logger.info(f"Empty analytics HTML report saved to {report_file}.")
            return
        except Exception as e:
            error_logger.error(f"Failed to save empty HTML report: {e}")
            return

    # Function for determining the color (simplified as colors removed)
    def get_duration_category(duration: float) -> str:
        if duration <= duration_thresholds.get("short_max", 2):
            return "short"
        elif duration <= duration_thresholds.get("medium_max", 5):
            return "medium"
        elif duration <= duration_thresholds.get("long_max", 10):
            return "long"
        else:
            return "long"  # Or a separate category for very long

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

                # Group only successful calls that are within the 'short' threshold
                if success and duration <= short_max:
                    key = (
                        ev.get("Function", "Unknown"),
                        ev.get("Event Type", "Unknown"),
                    )
                    if key not in grouped_short_success:
                        grouped_short_success[key] = {"count": 0, "total_duration": 0.0}
                    grouped_short_success[key]["count"] += 1
                    grouped_short_success[key]["total_duration"] += duration
                else:
                    # Add events that are not short and successful (i.e., failed or long)
                    big_or_fail_events.append(ev)

            except KeyError as e:
                error_logger.error(
                    f"Missing key in event data during grouping: {e}, event: {ev}"
                )
                big_or_fail_events.append(ev)  # Add an event if it has missing data

    else:
        # If grouping is disabled, all events go to the detailed list
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
        /* Optional: Add classes for duration categories if you want to style them */
        .duration-short {{ background-color: #e0ffe0; }} /* Light green */
        .duration-medium {{ background-color: #fffacd; }} /* Lemon Chiffon */
        .duration-long {{ background-color: #ffb0b0; }} /* Light red */

    </style>
</head>
<body>
    <h2>Analytics Report for {date_str}</h2>
    <div class="summary">
        <p class="run-type">Run type: {"Full scan" if force_mode else "Incremental update"}</p>
        <p><strong>Total functions:</strong> {len(call_counts)}</p>
        <p><strong>Total events:</strong> {len(events)}</p>
        <p><strong>Success rate:</strong> {sum(success_counts.values()) / sum(call_counts.values()) * 100 if sum(call_counts.values()) else 0:.1f}%</p>
    <h3>Grouped Short & Successful Calls</h3>
    <table>
        <tr>
            <th>Function</th>
            <th>Event Type</th>
            <th>Count</th>
            <th>Avg Duration (s)</th>
            <th>Total Duration (s)</th>
        </tr>"""  # noqa: E501

    # Adding groups of successful calls
    if group_successful_short_calls and grouped_short_success:
        # Sort grouped items by function name
        for (fun, evt), val in sorted(grouped_short_success.items()):
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
    <h3>Detailed Calls (Errors or Long/Medium Calls)</h3>
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
        # Sort detailed events by start time
        for ev in sorted(big_or_fail_events, key=lambda x: x.get("Start Time", "")):
            try:
                duration = ev.get("Duration (s)", 0)
                success = ev.get("Success", False)
                row_class = "error" if not success else ""  # Class for error rows

                # Determine duration category for potential styling
                duration_category = get_duration_category(duration)
                # Add a class based on duration category if not an error row
                if row_class == "":
                    row_class = f"duration-{duration_category}"

                success_display = "Yes" if success else "No"

                html_content += f"""
        <tr class="{row_class}">
            <td>{ev.get("Function", "Unknown")}</td>
            <td>{ev.get("Event Type", "Unknown")}</td>
            <td>{ev.get("Start Time", "Unknown")}</td>
            <td>{ev.get("End Time", "Unknown")}</td>
            <td>{duration}</td>
            <td>{success_display}</td>
        </tr>"""
            except KeyError as e:
                error_logger.error(
                    f"Error formatting event for detailed list: {e}, event data: {ev}"
                )
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
        # Sort summary by function name
        for function, count in sorted(call_counts.items()):
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
        # Ensure directory exists before saving (redundant if get_full_log_path does it, but safe)
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        console_logger.info(f"Analytics HTML report saved to {report_file}.")
    except Exception as e:
        error_logger.error(f"Failed to save HTML report: {e}")


def save_detailed_dry_run_report(
    changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
) -> None:
    """Generate a detailed HTML report with separate tables for each change type."""
    if not changes:
        console_logger.info("No changes to report for dry run.")
        return

    # Group changes by type
    changes_by_type = defaultdict(list)
    for change in changes:
        change_type = change.get("change_type", "unknown").replace("_", " ").title()
        changes_by_type[change_type].append(change)

    # Generate HTML
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Dry Run Report</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                margin: 10px;
                background-color: #f9f9f9;
                color: #333;
            }
            h2 { color: #1a1a1a; border-bottom: 2px solid #ddd; padding-bottom: 5px; }
            table { border-collapse: collapse; width: 100%; margin-bottom: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); table-layout: auto; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; white-space: nowrap; }
            thead { background-color: #e9ecef; }
            th { font-weight: 600; }
            tbody tr:nth-child(even) { background-color: #f2f2f2; }
            tbody tr:hover { background-color: #e9e9e9; }
            .container { max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 8px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Dry Run Simulation Report</h1>
    """

    # Create table for each change type
    for change_type, change_list in changes_by_type.items():
        if not change_list:
            continue

        html += f"<h2>{change_type} ({len(change_list)} potential changes)</h2>"

        # Strictly defined columns for each report type for reliability
        header_map = {
            "Cleaning": ["artist", "original_name", "cleaned_name", "original_album", "cleaned_album"],
            "Genre Update": ["artist", "album", "track_name", "original_genre", "new_genre"],
            "Year Update": ["artist", "album", "track_name", "original_year", "simulated_year"]
        }

        # Get the correct list of keys for the current report type.
        # If the type is unknown, fallback to the old behavior as a backup option.
        headers = header_map.get(change_type, [h for h in change_list[0].keys() if h not in ['change_type', 'timestamp', 'track_id', 'dateAdded']])

        html += "<table><thead><tr>"
        for header in headers:
            # Create readable column headers
            html += f"<th>{header.replace('_', ' ').title()}</th>"
        html += "</tr></thead><tbody>"

        # Fill table with data
        for item in change_list:
            html += "<tr>"
            # Go through the fixed list of headers
            for header_key in headers:
                value = item.get(header_key, "")
                html += f"<td>{value}</td>"
            html += "</tr>"

        html += "</tbody></table>"

    html += """
        </div>
    </body>
    </html>
    """

    # Save HTML file
    try:
        ensure_directory(os.path.dirname(file_path))
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        console_logger.info(f"Successfully generated detailed dry run HTML report at: {file_path}")
    except Exception as e:
        error_logger.error(f"Failed to save detailed dry run HTML report: {e}")
