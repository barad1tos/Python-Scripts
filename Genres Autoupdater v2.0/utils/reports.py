#!/usr/bin/env python3

"""
Reports Module

Provides functions for generating CSV and HTML reports, as well as track list synchronization.
HTML reports are stored in a fixed file (e.g., analytics/reports/analytics.html) under logs_base_dir,
so that each run overwrites the report rather than creating new files.
"""

import csv
import logging
import os

from datetime import datetime
from typing import Any, Dict, List, Optional

from services.cache_service import CacheService

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

    Args:
        data (List[Dict[str, str]]): List of dictionaries representing the data.
        fieldnames (List[str]): List of field names (CSV header).
        file_path (str): Full path to the CSV file.
        console_logger (logging.Logger): Logger for informational messages.
        error_logger (logging.Logger): Logger for error messages.
        data_type (str): A descriptive name for the data type (used in logging).
    """
    csv_dir = os.path.dirname(file_path)
    if csv_dir and not os.path.exists(csv_dir):
        console_logger.info(f"Creating CSV directory: {csv_dir}")
        os.makedirs(csv_dir, exist_ok=True)

    console_logger.info(f"Saving {data_type} to CSV: {file_path}")
    try:
        with open(file_path, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            # Filter each row to include only keys present in fieldnames
            for row in data:
                filtered_row = {field: row.get(field, "") for field in fieldnames}
                writer.writerow(filtered_row)
        console_logger.info(f"{data_type.capitalize()} saved to {file_path}.")
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

    Args:
        tracks (List[Dict[str, str]]): List of track dictionaries.
        file_path (str): Path to the CSV file. (No timestamp substitution is applied.)
        console_logger (Optional[logging.Logger]): Logger for informational messages.
        error_logger (Optional[logging.Logger]): Logger for error messages.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
    fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded", "trackStatus"]
    _save_csv(tracks, fieldnames, file_path, console_logger, error_logger, "tracks")

def save_changes_report(
    changes: List[Dict[str, str]],
    file_path: str,
    console_logger: Optional[logging.Logger] = None,
    error_logger: Optional[logging.Logger] = None
) -> None:
    """
    Save the changes report to a CSV file.

    Args:
        changes (List[Dict[str, str]]): List of dictionaries representing changes.
        file_path (str): Path to the CSV file. (No timestamp substitution.)
        console_logger (Optional[logging.Logger]): Logger for informational messages.
        error_logger (Optional[logging.Logger]): Logger for error messages.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
    # Updated fieldnames to support additional year_updated and new_year keys
    fieldnames = ["artist", "album", "track_name", "old_genre", "new_genre", "new_track_name", "year_updated", "new_year"]
    changes_sorted = sorted(changes, key=lambda x: x.get("artist", "Unknown"))
    _save_csv(changes_sorted, fieldnames, file_path, console_logger, error_logger, "changes report")

def save_html_report(
    events: List[Dict[str, Any]],
    call_counts: Dict[str, int],
    success_counts: Dict[str, int],
    decorator_overhead: Dict[str, float],
    config: Dict[str, Any],
    console_logger: Optional[logging.Logger] = None,
    error_logger: Optional[logging.Logger] = None,
    group_successful_short_calls: bool = False
) -> None:
    """
    Generate an HTML report from the provided analytics data.
    
    The HTML report is written to a fixed file (e.g., analytics/reports/analytics.html),
    so that each run overwrites the previous report.
    Args:
        events (List[Dict[str, Any]]): List of event dictionaries.
        call_counts (Dict[str, int]): Mapping of function names to call counts.
        success_counts (Dict[str, int]): Mapping of function names to success counts.
        decorator_overhead (Dict[str, float]): Mapping of function names to decorator overhead.
        config (Dict[str, Any]): Configuration dictionary.
        console_logger (Optional[logging.Logger]): Logger for informational messages.
        error_logger (Optional[logging.Logger]): Logger for error messages.
        group_successful_short_calls (bool, optional): If True, group successful short calls.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")

    date_str = datetime.now().strftime("%Y-%m-%d")
    logs_base_dir = config.get("logs_base_dir", ".")
    reports_dir = os.path.join(logs_base_dir, "analytics", "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    html_template = config["logging"].get("html_report_file", "analytics/reports/analytics.html")
    report_file = os.path.join(logs_base_dir, html_template)
    
    duration_thresholds = config.get("analytics", {}).get("duration_thresholds", {"short_max": 2, "medium_max": 5, "long_max": 10})
    colors = config.get("analytics", {}).get("colors", {"short": "#90EE90", "medium": "#D3D3D3", "long": "#FFB6C1"})

    def get_color(duration: float) -> str:
        if duration <= duration_thresholds.get("short_max", 2):
            return ""
        elif duration <= duration_thresholds.get("medium_max", 5):
            return colors.get("medium", "#D3D3D3")
        elif duration <= duration_thresholds.get("long_max", 10):
            return colors.get("long", "#FFB6C1")
        else:
            return colors.get("long", "#FFB6C1")

    grouped_short_success = {}
    big_or_fail_events = []
    short_max = duration_thresholds.get("short_max", 2)
    if group_successful_short_calls:
        for ev in events:
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
    else:
        big_or_fail_events = events

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
    <h3>Grouped Short & Successful Calls</h3>
    <table>
        <tr>
            <th>Function</th>
            <th>Event Type</th>
            <th>Count</th>
            <th>Avg Duration (s)</th>
            <th>Total Duration (s)</th>
        </tr>"""
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
        </tr>"""
    else:
        html_content += """
        <tr><td colspan="5">No grouping enabled.</td></tr>"""
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
            <td{f' style="background-color: {color};"' if color else ''}>{duration}</td>
            <td>{success}</td>
        </tr>"""
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
    html_content += """
    </table>
</body>
</html>"""
    try:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        console_logger.info(f"Analytics HTML report saved to {report_file}.")
    except Exception as e:
        error_logger.error(f"Failed to save HTML report: {e}")

def load_track_list(csv_path: str) -> Dict[str, Dict[str, str]]:
    """
    Load track data from the CSV file into a dictionary.
    
    Args:
        csv_path (str): Path to the CSV file.
        
    Returns:
        Dict[str, Dict[str, str]]: A dictionary mapping track IDs to track data.
        
    Example:
        >>> track_map = load_track_list("csv/track_list.csv")
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
                    }
    except Exception as e:
        logger.error(f"Could not read track_list.csv: {e}")
    return track_map

async def sync_track_list_with_current(
    all_tracks: List[Dict[str, str]],
    csv_path: str,
    cache_service: CacheService,
    console_logger: logging.Logger,
    error_logger: logging.Logger,
    partial_sync: bool = False
) -> None:
    """
    Synchronize the current list of tracks with the data in the CSV file.
    """
    console_logger.info(f"Starting sync: fetched {len(all_tracks)} tracks; CSV file: {csv_path}")
    cached_tracks = None
    
    # Correct use of cache_service
    if cache_service:
        try:
            # Call get_async with the correct key
            cached_tracks = await cache_service.get_async("ALL")
        except AttributeError:
            error_logger.warning("Cache service doesn't have get_async method, skipping cache check")
        except Exception as e:
            error_logger.error(f"Error accessing cache: {e}")
    
    csv_map = load_track_list(csv_path)
    console_logger.info(f"CSV currently contains {len(csv_map)} tracks before sync.")
    
    # Check cache if available
    if cached_tracks is not None:
        if len(cached_tracks) != len(all_tracks):
            console_logger.warning("Cached tracks count does not match fetched tracks count. Proceeding with sync update.")
        else:
            console_logger.info("Cached verification passed successfully: track counts match.")
            
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