"""
Reports Module

Provides functions for generating CSV/HTML reports and handling track list sync.
"""

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
    Save a list of tracks to a CSV file.

    :param tracks: List of track dictionaries to save.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for info messages.
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
    Save a list of changes to a CSV report file.

    :param changes: List of change dictionaries.
    :param file_path: Path to the CSV report file.
    :param console_logger: Logger for info messages.
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
    error_logger: Optional[Logger] = None,
    group_successful_short_calls: bool = False
) -> None:
    """
    Save analytics events to an HTML report, color-coding by duration,
    and optionally grouping short+successful calls.

    :param events: List of event dictionaries.
    :param call_counts: Dict of function call counts.
    :param success_counts: Dict of function success counts.
    :param decorator_overhead: Dict of decorator overhead times.
    :param config: Configuration dict with analytics settings.
    :param console_logger: Logger for info messages.
    :param error_logger: Logger for error messages.
    :param group_successful_short_calls: Whether to group short+successful calls to reduce line count.
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

    # 1) Split events into “long or error” vs. “short & successful”
    grouped_short_success = {}
    big_or_fail_events = []
    short_max = duration_thresholds.get("short_max", 2)

    if group_successful_short_calls:
        for ev in events:
            duration = ev["Duration (s)"]
            success = ev["Success"]
            if (not success) or (duration > short_max):
                # Error or long call => detail
                big_or_fail_events.append(ev)
            else:
                # short + success => group
                key = (ev["Function"], ev["Event Type"])
                if key not in grouped_short_success:
                    grouped_short_success[key] = {
                        "count": 0,
                        "total_duration": 0.0
                    }
                grouped_short_success[key]["count"] += 1
                grouped_short_success[key]["total_duration"] += duration
    else:
        # No grouping => everything is detailed
        big_or_fail_events = events

    # Build HTML
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

        <h3>Grouped Short & Successful Calls</h3>
        <table>
            <tr>
                <th>Function</th>
                <th>Event Type</th>
                <th>Count</th>
                <th>Avg Duration (s)</th>
                <th>Total Duration (s)</th>
            </tr>
    """

    # 2) Grouped table
    if group_successful_short_calls:
        for (fun, evt), val in grouped_short_success.items():
            cnt = val["count"]
            total_dur = val["total_duration"]
            avg_dur = round(total_dur / cnt, 4)
            html_content += f"""
            <tr>
                <td>{fun}</td>
                <td>{evt}</td>
                <td>{cnt}</td>
                <td>{avg_dur}</td>
                <td>{round(total_dur, 4)}</td>
            </tr>
            """
    else:
        html_content += """
            <tr><td colspan="5">No grouping enabled.</td></tr>
        """

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
            </tr>
    """

    # 3) Detailed table for big_or_fail_events
    for ev in big_or_fail_events:
        duration = ev["Duration (s)"]
        color = get_color(duration)
        success = "Yes" if ev["Success"] else "No"
        row_class = "error" if not ev["Success"] else ""
        html_content += f"""
            <tr class="{row_class}">
                <td>{ev['Function']}</td>
                <td>{ev['Event Type']}</td>
                <td>{ev['Start Time']}</td>
                <td>{ev['End Time']}</td>
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

    # 4) Summary statistics
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
            </tr>
        """

    html_content += """
        </table>
    </body>
    </html>
    """

    # 5) Save HTML to file
    try:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        console_logger.info(f"Analytics HTML report saved to {report_file}. (Optimized)")
    except IOError as e:
        error_logger.error(f"Failed to save HTML report: {e}")


def load_track_list(csv_path: str) -> Dict[str, Dict[str, str]]:
    """
    Load track_list.csv into a dictionary keyed by track ID.

    :param csv_path: Path to the CSV file.
    :return: Dict of track data.
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


# Modified sync_track_list_with_current in reports.py
def sync_track_list_with_current(
    all_tracks: List[Dict[str, str]],
    csv_path: str,
    console_logger: Logger,
    error_logger: Logger,
    partial_sync: bool = False
) -> None:
    """
    Synchronize track_list.csv with the given list of tracks (all_tracks).

    If partial_sync=False (default), this performs a FULL sync:
      1) Remove tracks from CSV that are NOT in all_tracks
      2) Add/update any changed/added tracks
      3) Rewrite CSV fully

    If partial_sync=True, this only merges changes/additions
    without removing tracks that do not appear in all_tracks.

    Additional logging is provided for track counts.
    Also verifies that the fetched track count matches the cache before sync.
    """
    # Log initial counts
    console_logger.info(f"Starting sync: fetched {len(all_tracks)} tracks; CSV file: {csv_path}")

    # Verify that the cache is valid (using the "ALL" key)
    from music_genre_updater import fetch_cache  # import global cache
    if "ALL" in fetch_cache:
        cached_tracks, _ = fetch_cache["ALL"]
        if len(all_tracks) != len(cached_tracks):
            console_logger.error(
                f"Track count mismatch: fetched {len(all_tracks)} tracks but cache has {len(cached_tracks)}. Aborting sync."
            )
            return
        else:
            console_logger.info("Cache verification passed: track counts match.")

    csv_map = load_track_list(csv_path)
    console_logger.info(f"CSV currently contains {len(csv_map)} tracks before sync.")

    if not partial_sync:
        removed_count = 0
        fetched_ids = {tr.get("id", "").strip() for tr in all_tracks if tr.get("id", "").strip()}
        for old_tid in list(csv_map.keys()):
            if old_tid not in fetched_ids:
                del csv_map[old_tid]
                removed_count += 1
        console_logger.info(f"Removed {removed_count} tracks from CSV that are missing in fetched data.")

    added_or_updated_count = 0
    current_map = {}
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
    for tid, new_data in current_map.items():
        old_data = csv_map.get(tid)
        if not old_data:
            csv_map[tid] = new_data
            added_or_updated_count += 1
        else:
            changed = False
            for field in ["name", "artist", "album", "genre", "dateAdded", "trackStatus"]:
                if old_data.get(field) != new_data[field]:
                    old_data[field] = new_data[field]
                    changed = True
            if changed:
                added_or_updated_count += 1

    console_logger.info(f"Added/Updated {added_or_updated_count} tracks in CSV.")
    final_list = list(csv_map.values())
    console_logger.info(f"Final CSV track count after sync: {len(final_list)}")

    _save_csv(final_list, ["id", "name", "artist", "album", "genre", "dateAdded", "trackStatus"], csv_path, console_logger, error_logger, "tracks")