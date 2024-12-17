# reports.py
# This module provides functions for generating CSV reports

import csv
import logging
from typing import List, Dict, Optional
from logging import Logger

def _save_csv(
    data: List[Dict[str, str]],
    fieldnames: List[str],
    file_path: str,
    console_logger: Logger,
    error_logger: Logger,
    data_type: str
) -> None:
    """
    Helper function to save data to a CSV file.

    :param data: List of dictionaries containing the data to write.
    :param fieldnames: List of field names for the CSV.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for informational messages.
    :param error_logger: Logger for error messages.
    :param data_type: Description of the data being saved (e.g., 'tracks', 'changes report').
    """
    console_logger.info(f"Saving {data_type} to CSV: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        console_logger.info(f"{data_type.capitalize()} saved to {file_path}.")
    except (IOError, csv.Error) as e:
        error_logger.error(f"Failed to save {data_type}: {e}")

def save_to_csv(
    tracks: List[Dict[str, str]],
    file_path: str,
    console_logger: Optional[Logger] = None,
    error_logger: Optional[Logger] = None
) -> None:
    """
    Saves a list of tracks to a CSV file.

    :param tracks: List of track dictionaries to save.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for informational messages.
    :param error_logger: Logger for error messages.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    fieldnames = [
        "id",
        "name",
        "artist",
        "album",
        "genre",
        "dateAdded",
        "trackStatus",
    ]
    _save_csv(tracks, fieldnames, file_path, console_logger, error_logger, "tracks")

def save_changes_report(
    changes: List[Dict[str, str]],
    file_path: str,
    console_logger: Optional[Logger] = None,
    error_logger: Optional[Logger] = None
) -> None:
    """
    Saves a list of changes to a CSV report file.

    :param changes: List of change dictionaries to save.
    :param file_path: Path to the CSV report file.
    :param console_logger: Logger for informational messages.
    :param error_logger: Logger for error messages.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    fieldnames = [
        "artist",
        "album",
        "track_name",
        "old_genre",
        "new_genre",
        "new_track_name",
    ]
    changes_sorted = sorted(changes, key=lambda x: x.get("artist", "Unknown"))
    _save_csv(changes_sorted, fieldnames, file_path, console_logger, error_logger, "changes report")