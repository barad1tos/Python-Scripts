# reports.py
# This module provides functions for generating CSV and HTML reports

import os
import csv
import logging
from typing import List, Dict, Optional, Any
from logging import Logger
from datetime import datetime


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


def save_html_report(
    events: List[Dict[str, Any]],
    call_counts: Dict[str, int],
    success_counts: Dict[str, int],
    decorator_overhead: Dict[str, float],
    config: Dict[str, Any],
    console_logger: Optional[Logger] = None,
    error_logger: Optional[Logger] = None
) -> None:
    """
    Saves analytics events to an HTML report, with color-coding based on duration.

    :param events: List of event dictionaries.
    :param call_counts: Dictionary of function call counts.
    :param success_counts: Dictionary of function success counts.
    :param decorator_overhead: Dictionary of decorator overhead per function.
    :param config: Configuration dictionary.
    :param console_logger: Logger for informational messages.
    :param error_logger: Logger for error messages.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    html_output_dir = config.get("analytics", {}).get("reports", {}).get("html_output_dir", "reports_html")
    os.makedirs(html_output_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    report_file = os.path.join(html_output_dir, f"analytics_{date_str}.html")

    # Define color thresholds from config
    duration_thresholds = config.get("analytics", {}).get("duration_thresholds", {
        "short_max": 2,
        "medium_max": 5,
        "long_max": 10
    })
    colors = config.get("analytics", {}).get("colors", {
        "short": "#90EE90",    # Light Green
        "medium": "#D3D3D3",   # Light Gray
        "long": "#FFB6C1"      # Light Red (Pink)
    })

    # Function to determine color based on duration
    def get_color(duration: float) -> str:
        if duration <= duration_thresholds.get("short_max", 2):
            return colors.get("short", "#90EE90")
        elif duration <= duration_thresholds.get("medium_max", 5):
            return colors.get("medium", "#D3D3D3")
        else:
            return colors.get("long", "#FFB6C1")

    # Start building HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Analytics Report for {date_str}</title>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h2>Analytics Report for {date_str}</h2>
        <table>
            <tr>
                <th>Function</th>
                <th>Event Type</th>
                <th>Start Time</th>
                <th>End Time</th>
                <th>Duration (s)</th>
                <th>Success</th>
            </tr>
    """

    for event in events:
        duration = event["Duration (s)"]
        color = get_color(duration)
        success = "Yes" if event["Success"] else "No"
        html_content += f"""
            <tr>
                <td>{event['Function']}</td>
                <td>{event['Event Type']}</td>
                <td>{event['Start Time']}</td>
                <td>{event['End Time']}</td>
                <td style="background-color: {color};">{duration}</td>
                <td>{success}</td>
            </tr>
        """

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
            </tr>
    """

    for function, count in call_counts.items():
        success = success_counts.get(function, 0)
        success_rate = (success / count * 100) if count > 0 else 0
        overhead = decorator_overhead.get(function, 0)
        html_content += f"""
            <tr>
                <td>{function}</td>
                <td>{count}</td>
                <td>{success}</td>
                <td>{success_rate:.2f}</td>
                <td>{round(overhead, 4)}</td>
            </tr>
        """

    html_content += """
        </table>
    </body>
    </html>
    """

    # Save HTML to file
    try:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        console_logger.info(f"Analytics HTML report saved to {report_file}.")
    except IOError as e:
        error_logger.error(f"Failed to save HTML report: {e}")


def load_track_list(csv_path: str) -> Dict[str, Dict[str, str]]:
    """
    Loads the track_list.csv file into a dictionary.

    :param csv_path: Path to the track_list.csv file.
    :return: Dictionary of track data.
    """
    track_map: Dict[str, Dict[str, str]] = {}
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
                    }
    except Exception as e:
        logger.error(f"Could not read track_list.csv: {e}")
    return track_map


def sync_track_list_with_current(
    all_tracks: List[Dict[str, str]],
    csv_path: str,
    console_logger: Logger,
    error_logger: Logger
) -> None:
    """
    Synchronizes track_list.csv with the current list of tracks from AppleScript (FULL sync of all fields).

    1) Loads the current CSV into a dict (csv_map).
    2) Removes any tracks from csv_map that are not in all_tracks.
    3) Adds/updates tracks from all_tracks (including name, artist, album, genre, dateAdded, trackStatus).
    4) Writes the final merged dict back to track_list.csv.

    :param all_tracks: List of dictionaries representing all tracks from AppleScript.
    :param csv_path: Path to the track_list.csv file.
    :param console_logger: Logger for informational messages.
    :param error_logger: Logger for error messages.
    """
    csv_map = load_track_list(csv_path)

    # Create a dictionary of current tracks (from all_tracks)
    current_map: Dict[str, Dict[str, str]] = {}
    for tr in all_tracks:
        tid = tr.get("id", "").strip()
        if not tid:
            continue
        current_map[tid] = {
            "id": tid,
            "name": tr.get("name", "").strip(),
            "artist": tr.get("artist", "").strip(),
            "album": tr.get("album", "").strip(),
            "genre": tr.get("genre", "").strip(),
            "dateAdded": tr.get("dateAdded", "").strip(),
            "trackStatus": tr.get("trackStatus", "").strip(),
        }

    # 1) Remove tracks that no longer exist
    removed_count = 0
    for old_tid in list(csv_map.keys()):
        if old_tid not in current_map:
            del csv_map[old_tid]
            removed_count += 1

    # 2) Add/Update all fields if changed
    added_or_updated_count = 0
    for tid, new_data in current_map.items():
        old_data = csv_map.get(tid)
        if not old_data:
            # Completely new track
            csv_map[tid] = new_data
            added_or_updated_count += 1
        else:
            changed = False
            # Full synchronization of all fields
            for field in ["name", "artist", "album", "genre", "dateAdded", "trackStatus"]:
                if old_data.get(field) != new_data[field]:
                    old_data[field] = new_data[field]
                    changed = True
            if changed:
                added_or_updated_count += 1

    console_logger.info(
        f"Syncing track_list.csv (FULL) with up-to-date AppleScript tracks: "
        f"Removed {removed_count}, Added/Updated {added_or_updated_count}."
    )

    final_list = list(csv_map.values())
    save_to_csv(final_list, csv_path, console_logger, error_logger)