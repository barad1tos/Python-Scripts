#!/usr/bin/env python3

"""Reports Module.

Provides functions for CSV/HTML report generation and track data management
for music library operations. Handles both file operations and formatted console output.

Main features:
- Track list saving, loading, and synchronization with persistent storage
- Consolidated change reporting for genres, years, and track/album naming
- Console-friendly formatted reporting for interactive mode
- CSV data persistence with field validation and filtering
- Unified reporting interface for actual and simulated operations
- HTML performance analytics with visualization and event tracking
- Support for both incremental and full synchronization modes

Key functions:
- save_to_csv: Saves track metadata to structured CSV files
- save_unified_changes_report: Generates comprehensive change reports with console formatting
- load_track_list: Loads track data from CSV with field validation
- sync_track_list_with_current: Synchronizes track data between application runs, using CacheService for album years
- save_unified_dry_run: Creates consolidated reports for simulated operations
- save_html_report: Creates detailed HTML analytics with performance metrics

Note: Album year caching and metadata integration is now handled by CacheService
"""

import csv
import logging
import os

from datetime import datetime
from typing import Any

from services.cache_service import CacheService
from utils.logger import (
    ensure_directory,
    get_full_log_path,
)

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
    force_mode: bool = False,
) -> None:
    """Save consolidated changes report combining genre, year, and other changes.

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
        Key.CHANGE_TYPE,  # New field to indicate type: genre, year, name, etc.
        Key.ARTIST,
        Key.ALBUM,
        Key.TRACK_NAME,
        Key.OLD_GENRE,
        Key.NEW_GENRE,
        Key.OLD_YEAR,
        Key.NEW_YEAR,
        Key.OLD_TRACK_NAME,
        Key.NEW_TRACK_NAME,
        Key.OLD_ALBUM_NAME,
        Key.NEW_ALBUM_NAME,
        Key.TIMESTAMP,
    ]

    # Sort changes by artist and album for readability
    changes_sorted = sorted(
        changes, key=lambda x: (x.get(Key.ARTIST, Misc.UNKNOWN), x.get(Key.ALBUM, Misc.UNKNOWN))
    )

    # For force mode, print a formatted report to console
    if force_mode:
        console_logger.info(f"{Misc.EMOJI_REPORT} Changes Report:")
        console_logger.info("-" * Format.SEPARATOR_80)

        # Group changes by type for better presentation
        changes_by_type: dict[str, list[dict[str, str]]] = {}
        for change in changes_sorted:
            change_type = change.get(Key.CHANGE_TYPE, ChangeType.OTHER)
            if change_type not in changes_by_type:
                changes_by_type[change_type] = []
            changes_by_type[change_type].append(change)

        # For year changes, filter to only show where old_year != new_year
        # This filtering logic might be better placed where changes are generated,
        # but keeping it here for console report consistency with previous code.
        if ChangeType.YEAR in changes_by_type:
            changes_by_type[ChangeType.YEAR] = [
                change
                for change in changes_by_type[ChangeType.YEAR]
                if change.get(Key.OLD_YEAR, "") != change.get(Key.NEW_YEAR, "")
            ]

        # Print each change type with its own header
        for change_type, type_changes in changes_by_type.items():
            # Only print section if there are changes of this type
            if not type_changes:
                continue

            console_logger.info(
                f"\n{Misc.EMOJI_CHANGE} {change_type.upper()} Changes ({len(type_changes)}):"
            )

            # Print table header for this type
            if change_type == ChangeType.GENRE:
                console_logger.info(
                    f"{'Artist':<{Format.COL_WIDTH_30}} {'Album':<{Format.COL_WIDTH_30}} {'Track':<{Format.COL_WIDTH_30}} {Format.HEADER_OLD_NEW}"
                )
                console_logger.info("-" * Format.SEPARATOR_100)
                for change in type_changes:
                    artist = (
                        change.get(Key.ARTIST, "")[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX
                        if len(change.get(Key.ARTIST, "")) > Format.COL_WIDTH_30
                        else change.get(Key.ARTIST, "")
                    )
                    album = (
                        change.get(Key.ALBUM, "")[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX
                        if len(change.get(Key.ALBUM, "")) > Format.COL_WIDTH_30
                        else change.get(Key.ALBUM, "")
                    )
                    track = (
                        change.get(Key.TRACK_NAME, "")[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX
                        if len(change.get(Key.TRACK_NAME, "")) > Format.COL_WIDTH_30
                        else change.get(Key.TRACK_NAME, "")
                    )
                    old_genre = change.get(Key.OLD_GENRE, "")
                    new_genre = change.get(Key.NEW_GENRE, "")
                    console_logger.info(
                        f"{artist:<{Format.COL_WIDTH_30}} {album:<{Format.COL_WIDTH_30}} {track:<{Format.COL_WIDTH_30}} {old_genre} {Format.ARROW} {new_genre}"  # noqa: E501
                    )

            elif change_type == ChangeType.YEAR:
                # Already filtered for actual year changes above
                console_logger.info(f"{'Artist':<{Format.COL_WIDTH_30}} {'Album':<{Format.COL_WIDTH_40}} {Format.HEADER_OLD_NEW}")
                console_logger.info("-" * Format.SEPARATOR_80)
                for change in type_changes:
                    artist = (
                        change.get(Key.ARTIST, "")[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX
                        if len(change.get(Key.ARTIST, "")) > Format.COL_WIDTH_30
                        else change.get(Key.ARTIST, "")
                    )
                    album = (
                        change.get(Key.ALBUM, "")[:Format.COL_WIDTH_38] + Format.TRUNCATE_SUFFIX
                        if len(change.get(Key.ALBUM, "")) > Format.COL_WIDTH_40
                        else change.get(Key.ALBUM, "")
                    )
                    old_year = change.get(Key.OLD_YEAR, "")
                    new_year = change.get(Key.NEW_YEAR, "")
                    year_display = f"{Color.YELLOW}{old_year} {Format.ARROW} {new_year}{Color.RESET}"
                    console_logger.info(f"{artist:<{Format.COL_WIDTH_30}} {album:<{Format.COL_WIDTH_40}} {year_display}")

            elif change_type == ChangeType.NAME:
                console_logger.info(
                    f"{'Artist':<{Format.COL_WIDTH_30}} {Format.HEADER_ITEM_TYPE:<{Format.COL_WIDTH_10}} {Format.HEADER_ITEM_NAME:<{Format.COL_WIDTH_30}} {Format.HEADER_OLD_NEW}"  # noqa: E501
                )  # Added Item Type column
                console_logger.info("-" * Format.SEPARATOR_100)  # Adjusted separator length
                for change in type_changes:
                    artist = (
                        change.get(Key.ARTIST, "")[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX
                        if len(change.get(Key.ARTIST, "")) > Format.COL_WIDTH_30
                        else change.get(Key.ARTIST, "")
                    )
                    if (
                        change.get(Key.OLD_TRACK_NAME) is not None
                        or change.get(Key.NEW_TRACK_NAME) is not None
                    ):  # Check if it's a track name change
                        item_type = Format.ITEM_TYPE_TRACK
                        old_name = change.get(Key.OLD_TRACK_NAME, "")
                        new_name = change.get(Key.NEW_TRACK_NAME, "")
                        item_name_display = change.get(
                            Key.TRACK_NAME, ""
                        )  # Display the track name being changed
                    elif (
                        change.get(Key.OLD_ALBUM_NAME) is not None
                        or change.get(Key.NEW_ALBUM_NAME) is not None
                    ):  # Check if it's an album name change
                        item_type = Format.ITEM_TYPE_ALBUM
                        old_name = change.get(Key.OLD_ALBUM_NAME, "")
                        new_name = change.get(Key.NEW_ALBUM_NAME, "")
                        item_name_display = change.get(
                            Key.ALBUM, ""
                        )  # Display the album name being changed
                    else:
                        item_type = Format.ITEM_TYPE_OTHER
                        old_name = ""
                        new_name = ""
                        item_name_display = ""  # No specific item name for 'other' type

                    item_name_display = (
                        item_name_display[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX
                        if len(item_name_display) > Format.COL_WIDTH_30
                        else item_name_display
                    )  # Truncate item name display
                    old_name_display = (
                        old_name[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX if len(old_name) > Format.COL_WIDTH_30 else old_name
                    )  # Truncate old name display
                    new_name_display = (
                        new_name[:Format.COL_WIDTH_30 - 2] + Format.TRUNCATE_SUFFIX if len(new_name) > Format.COL_WIDTH_30 else new_name
                    )  # Truncate new name display

                    console_logger.info(
                        f"{artist:<{Format.COL_WIDTH_30}} {item_type:<{Format.COL_WIDTH_10}} {item_name_display:<{Format.COL_WIDTH_30}} {old_name_display} {Format.ARROW} {new_name_display}"  # noqa: E501
                    )

            # If we have an unknown change type, show generic info
            else:  # change_type == ChangeType.OTHER
                for change in type_changes:
                    # Attempt to print some identifying info for 'other' changes
                    artist = change.get(Key.ARTIST, Misc.UNKNOWN_ARTIST)
                    album = change.get(Key.ALBUM, Misc.UNKNOWN_ALBUM)
                    track_name = change.get(Key.TRACK_NAME, Misc.UNKNOWN_TRACK)
                    console_logger.info(
                        f"Other change for: Artist='{artist}', Album='{album}', Track='{track_name}' - Details: {change}"
                    )

        console_logger.info(f"\nTotal: {len(changes)} changes")
    else:
        # For normal mode, save to CSV file
        # Ensure directory exists before saving
        ensure_directory(os.path.dirname(file_path), error_logger)
        _save_csv(
            changes_sorted,
            fieldnames,
            file_path,
            console_logger,
            error_logger,
            Misc.CHANGES_REPORT_TYPE,
        )


def save_changes_report(
    changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger | None = None,
    error_logger: logging.Logger | None = None,
    force_mode: bool = False,
    add_timestamp: bool = False,
) -> None:
    """Save the list of change dictionaries to a CSV file.

    This wrapper ensures backward compatibility with the older report format.
    By default, it overwrites the specified file. If `add_timestamp` is True,
    it appends a timestamp to the filename to preserve previous reports.

    Args:
        changes: List of dictionaries with change data.
        file_path: Base path to the CSV file.
        console_logger: Logger for console output.
        error_logger: Logger for error output.
        force_mode: If ``True``, prints to console instead of saving to file.
        add_timestamp: If ``True``, a timestamp will be appended to the
            file name. Defaults to ``False``.

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
        changes, final_path, console_logger, error_logger, force_mode
    )


def save_changes_csv(
    changes: list[dict[str, str]],
    file_path: str,
    console_logger: logging.Logger | None = None,
    error_logger: logging.Logger | None = None,
    force_mode: bool = False,
    add_timestamp: bool = False,
) -> None:
    """Compatibility wrapper for saving change reports in CSV format."""
    save_changes_report(
        changes,
        file_path,
        console_logger,
        error_logger,
        force_mode,
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
        "change_type",  # "cleaning" or "genre_update"
        "track_id",
        "artist",
        "album",
        "track_name",
        "original_name",  # Original track name before cleaning (from cleaning_changes)
        "cleaned_name",  # Cleaned track name (from cleaning_changes)
        "original_album",  # Original album name before cleaning (from cleaning_changes)
        "cleaned_album",  # Cleaned album name (from cleaning_changes)
        "original_genre",  # Original genre (from genre_changes)
        "simulated_genre",  # Simulated genre (from genre_changes)
        "dateAdded",  # Date added from the track data (might be in cleaning_changes or genre_changes)
        "timestamp",  # Timestamp of when the change was logged (should be in change dicts)
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

    # Add genre changes
    for change in genre_changes:
        change_copy = change.copy()
        # Ensure change_type is set for genre changes
        change_copy["change_type"] = "genre_update"
        # Map new_genre to simulated_genre for consistency with fieldnames
        if "new_genre" in change_copy:
            change_copy["simulated_genre"] = change_copy.pop("new_genre")
        # Map old_genre to original_genre for consistency with fieldnames
        if "old_genre" in change_copy:
            change_copy["original_genre"] = change_copy.pop("old_genre")

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
