# reports.py
# This module provides functions for generating CSV reports

import csv
import logging

def save_to_csv(tracks, file_path, console_logger=None, error_logger=None):
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    console_logger.info(f"Saving tracks to CSV: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "id",
                "name",
                "artist",
                "album",
                "genre",
                "dateAdded",
                "trackStatus",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tracks)
        console_logger.info(f"Tracks saved to {file_path}.")
    except Exception as e:
        error_logger.error(f"Failed to save CSV: {e}")

def save_changes_report(changes, file_path, console_logger=None, error_logger=None):
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    console_logger.info(f"Saving changes report to {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "artist",
                "album",
                "track_name",
                "old_genre",
                "new_genre",
                "new_track_name",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            changes_sorted = sorted(changes, key=lambda x: x["artist"])
            writer.writerows(changes_sorted)
        console_logger.info("Changes report saved successfully.")
    except Exception as e:
        error_logger.error(f"Failed to save changes report: {e}")