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

    :param data: List of dictionaries to save to the CSV file.
    :param fieldnames: List of field names for the CSV file.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :param data_type: Type of data being saved (e.g., "tracks", "changes report").
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
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
    # Updated fieldnames include the new columns
    fieldnames = ["id", "name", "artist", "album", "genre", "dateAdded", "trackStatus", "old_year", "new_year"]
    _save_csv(tracks, fieldnames, file_path, console_logger, error_logger, "tracks")

def save_changes_report(
    changes: List[Dict[str, str]],
    file_path: str,
    console_logger: Optional[logging.Logger] = None,
    error_logger: Optional[logging.Logger] = None
) -> None:
    """
    Save the list of change dictionaries to a CSV file.

    :param changes: List of dictionaries with change data.
    :param file_path: Path to the CSV file.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
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
    """
    if console_logger is None:
        console_logger = logging.getLogger("console_logger")
    if error_logger is None:
        error_logger = logging.getLogger("error_logger")
        
    # Additional logging for diagnostics
    console_logger.info(f"Starting HTML report generation with {len(events)} events, {len(call_counts)} function counts")
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    logs_base_dir = config.get("logs_base_dir", ".")
    reports_dir = os.path.join(logs_base_dir, "analytics", "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    # Getting the path for an HTML file
    html_template = config.get("logging", {}).get("html_report_file", "analytics/reports/analytics.html")
    report_file = os.path.join(logs_base_dir, html_template)
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