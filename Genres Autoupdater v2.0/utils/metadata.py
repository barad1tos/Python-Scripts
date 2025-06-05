#!/usr/bin/env python3

"""Metadata Helpers Module.

Provides utility functions for parsing, cleaning, and processing music track metadata.
These functions are designed to be independent of specific service instances
and can be used across different parts of the application.

Functions:
    - parse_tracks: Parses raw AppleScript output into structured track dictionaries.
    - group_tracks_by_artist: Groups track dictionaries by artist name.
    - determine_dominant_genre_for_artist: Determines the most likely genre for an artist.
    - remove_parentheses_with_keywords: Removes specified parenthetical content from strings.
    - clean_names: Applies cleaning rules to track and album names.
    - is_music_app_running: Checks if Music.app is currently running.
"""

import logging
import re
import subprocess  # trunk-ignore(bandit/B404)

from collections import defaultdict
from datetime import datetime
from enum import IntEnum, auto
from typing import Any

class TrackField(IntEnum):
    """Enumeration of track data field indices with auto-incrementing values."""

    ID = 0
    NAME = auto()
    ARTIST = auto()
    ALBUM = auto()
    GENRE = auto()
    DATE_ADDED = auto()
    TRACK_STATUS = auto()
    OLD_YEAR = auto()
    NEW_YEAR = auto()


# Minimum required fields (TRACK_STATUS index + 1)
MIN_REQUIRED_FIELDS = TrackField.TRACK_STATUS + 1

def parse_tracks(raw_data: str, error_logger: logging.Logger) -> list[dict[str, str]]:
    """Parse the raw data from AppleScript into a list of track dictionaries.

    Uses the Record Separator (U+001E) as the field delimiter.

    :param raw_data: Raw string data from AppleScript.
    :param error_logger: Logger for error output.
    :return: List of track dictionaries.
    """
    field_separator = "\x1e"

    if not raw_data:
        error_logger.error("No data fetched from AppleScript.")
        return []

    error_logger.debug(
        f"parse_tracks: Input raw_data (first 500 chars): {raw_data[:500]}..."
    )

    tracks = []
    # Split raw data into rows using newline character
    rows = raw_data.strip().split("\n")
    for row in rows:
        # Skip empty rows that might result from trailing newline
        if not row:
            continue

        # Split each row into fields using the new field separator
        fields = row.split(field_separator)

        # The AppleScript is assumed to output at least MIN_REQUIRED_FIELDS fields for standard properties
        # and potentially more for old_year and new_year.
        if len(fields) >= MIN_REQUIRED_FIELDS:
            # Create track with required fields
            track = {
                "id": fields[TrackField.ID].strip(),
                "name": fields[TrackField.NAME].strip(),
                "artist": fields[TrackField.ARTIST].strip(),
                "album": fields[TrackField.ALBUM].strip(),
                "genre": fields[TrackField.GENRE].strip(),
                "dateAdded": fields[TrackField.DATE_ADDED].strip(),
                "trackStatus": fields[TrackField.TRACK_STATUS].strip(),
            }

            # Add optional fields if they exist
            for field in [TrackField.OLD_YEAR, TrackField.NEW_YEAR]:
                field_name = field.name.lower().replace('_', '')
                track[field_name] = fields[field].strip() if len(fields) > field else ""

            tracks.append(track)
        else:
            # Log malformed row for debugging
            error_logger.warning("Malformed track data row skipped: %s", row)
            error_logger.debug(
                f"parse_tracks: Finished parsing. Found {len(tracks)} tracks."
            )

    return tracks


def group_tracks_by_artist(
    tracks: list[dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    """Group tracks by artist name into a dictionary for efficient processing.

    Uses standard Python collections, no external dependencies or logging needed internally.

    :param tracks: List of track dictionaries.
    :return: Dictionary mapping artist names to lists of their tracks.
    """
    # Use defaultdict for efficient grouping without checking for key existence
    artists = defaultdict(list)
    for track in tracks:
        # Ensure artist key exists, default to "Unknown" if missing
        artist = track.get("artist", "Unknown")
        artists[artist].append(track)
    # Return defaultdict directly without converting to dict for better performance
    return artists


def determine_dominant_genre_for_artist(
    artist_tracks: list[dict[str, str]], error_logger: logging.Logger
) -> str:
    """Determine the dominant genre for an artist based on the earliest genre of their track.

    Does not require injected services, but logs errors.

    Algorithm:
        1. Finds the earliest track for each album (by dateAdded).
        2. Determines the earliest album (by dateAdded of its earliest track).
        3. Uses the genre of the earliest track in that album as the dominant genre.

    :param artist_tracks: List of track dictionaries for a single artist.
    :param error_logger: Logger for error output.
    :return: The dominant genre string, or "Unknown" on error or if no tracks.
    """
    if not artist_tracks:
        return "Unknown"
    try:
        # Find the earliest track for each album
        album_earliest: dict[str, dict[str, str]] = {}
        for track in artist_tracks:
            # Ensure album and dateAdded keys exist
            album = track.get("album", "Unknown")
            date_added_str = track.get(
                "dateAdded", "1900-01-01 00:00:00"
            )  # Provide a default date string for safety
            try:
                track_date = datetime.strptime(date_added_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # Handle cases with invalid date format by using a very old date
                error_logger.warning(
                    f"Invalid date format in track data for determining dominant genre: '{date_added_str}'. Using default old date."
                )
                track_date = datetime.strptime(
                    "1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
                )

            if album not in album_earliest:
                album_earliest[album] = track
            else:
                existing_date_str = album_earliest[album].get(
                    "dateAdded", "1900-01-01 00:00:00"
                )
                try:
                    existing_date = datetime.strptime(
                        existing_date_str, "%Y-%m-%d %H:%M:%S"
                    )
                except ValueError:
                    error_logger.warning(
                        f"Invalid date format for existing album earliest track in cache: '{existing_date_str}'. Using default old date."
                    )
                    existing_date = datetime.strptime(
                        "1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
                    )

                if track_date < existing_date:
                    album_earliest[album] = track

        # From these tracks, find the earliest album (by the date of addition of its earliest track)
        # Ensure album_earliest is not empty before finding min
        if not album_earliest:
            return "Unknown"

        earliest_album_track = min(
            album_earliest.values(),
            key=lambda t: datetime.strptime(
                t.get("dateAdded", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"
            ),
        )

        dominant_genre = earliest_album_track.get("genre")

        return (
            dominant_genre or "Unknown"
        )  # Return genre or "Unknown" if genre is None/empty
    except Exception as e:  # Catch broader exceptions during processing
        error_logger.error(
            "Error in determine_dominant_genre_for_artist: %s", e, exc_info=True
        )
        return "Unknown"


def remove_parentheses_with_keywords(
    name: str,
    keywords: list[str],
    console_logger: logging.Logger,
    error_logger: logging.Logger,
) -> str:
    """Remove parentheses and their content if they contain any of the specified keywords.

    Handles nested parentheses correctly. Logs debug info and errors.

    :param name: The string to clean.
    :param keywords: List of keywords to trigger removal.
    :param console_logger: Logger for console output (for debug).
    :param error_logger: Logger for error output.
    :return: Cleaned string.
    """
    console_logger.debug(
        "remove_parentheses_with_keywords called with name='%s' and keywords=%s",
        name,
        keywords,
    )

    if not name or not keywords:
        return name

    # Convert keywords to lowercase for case-insensitive comparison
    keyword_set = {k.lower() for k in keywords}

    # Iteratively clean brackets until no more matches are found
    prev_name = ""
    current_name = name

    while prev_name != current_name:
        prev_name = current_name

        # Find all bracket pairs in the current version of the string
        stack = []
        pairs = []

        for i, char in enumerate(current_name):
            if char in "([":
                stack.append((char, i))
            elif char in ")]":
                if stack:
                    start_char, start_idx = stack.pop()
                    if (start_char == "(" and char == ")") or (
                        start_char == "[" and char == "]"
                    ):
                        pairs.append((start_idx, i))
                    # Handle mismatched brackets by clearing stack if top doesn't match
                    # This prevents incorrect removal based on unmatched opening brackets
                    elif (start_char == "(" and char == "]") or (
                        start_char == "[" and char == ")"
                    ):
                        # Log a warning if a mismatch is found that prevents pairing
                        error_logger.warning(
                            f"Mismatched brackets found near index {i} in '{current_name}'. Clearing stack to prevent incorrect pairing."
                        )
                        stack.clear()  # Clear stack on mismatch to avoid incorrect pairing
                else:
                    # Log a warning if a closing bracket is found with no open bracket
                    error_logger.warning(
                        f"Closing bracket '{char}' found at index {i} with no matching opening bracket in '{current_name}'."
                    )

        # Sort pairs by start index
        pairs.sort()

        # Find which pairs to remove based on keywords
        to_remove = set()
        for start, end in pairs:
            # Ensure indices are valid before slicing
            if 0 <= start < end < len(current_name):
                content = current_name[start + 1 : end]  # noqa: ignore=E203
                if any(keyword.lower() in content.lower() for keyword in keyword_set):
                    to_remove.add((start, end))
            else:
                # Log a warning if invalid indices are generated for a pair
                error_logger.warning(
                    f"Generated invalid indices ({start}, {end}) for bracket removal in "
                    f"'{current_name}' (original: '{name}'). Skipping removal for this pair."
                )

        # If nothing found to remove, we're done
        if not to_remove:
            break

        # Remove brackets (from right to left to maintain indices)
        # Convert set to list and sort in reverse order of end index
        sorted_to_remove = sorted(to_remove, key=lambda item: item[1], reverse=True)
        for start, end in sorted_to_remove:
            # Indices were checked during finding, but double-check here
            if 0 <= start < end < len(current_name):
                current_name = (
                    current_name[:start] + current_name[end + 1 :]
                )  # noqa: ignore=E203
            # Note: Invalid index cases were already logged during the find loop

    # Clean up multiple spaces
    result = re.sub(r"\s+", " ", current_name).strip()

    console_logger.debug("Cleaned result: '%s'", result)

    return result


def clean_names(
    artist: str,
    track_name: str,
    album_name: str,
    config: dict[str, Any],
    console_logger: logging.Logger,
    error_logger: logging.Logger,
) -> tuple[str, str]:
    """Clean the track name and album name based on the configuration settings.

    Requires config and loggers.

    :param artist: Artist name.
    :param track_name: Current track name.
    :param album_name: Current album name.
    :param config: Application configuration dictionary.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    :return: Tuple containing the cleaned track name and cleaned album name.
    """
    console_logger.info(
        "clean_names called with: artist='%s', track_name='%s', album_name='%s'",
        artist,
        track_name,
        album_name,
    )

    # Use config passed as argument
    exceptions = config.get("exceptions", {}).get("track_cleaning", [])
    # Check if the current artist/album pair is in the exceptions list
    is_exception = any(
        exc.get("artist", "").lower() == artist.lower()
        and exc.get("album", "").lower() == album_name.lower()
        for exc in exceptions
    )

    if is_exception:
        console_logger.info(
            "No cleaning applied due to exceptions for artist '%s', album '%s'.",
            artist,
            album_name,
        )
        return track_name.strip(), album_name.strip()  # Return original names, stripped

    # Get cleaning config
    cleaning_config = config.get("cleaning", {})
    remaster_keywords = cleaning_config.get(
        "remaster_keywords", ["remaster", "remastered"]
    )
    album_suffixes = set(cleaning_config.get("album_suffixes_to_remove", []))

    # Helper function for cleaning strings using remove_parentheses_with_keywords
    def clean_string(val: str, keywords: list[str]) -> str:
        # Use the utility function defined above, passing loggers
        new_val = remove_parentheses_with_keywords(
            val, keywords, console_logger, error_logger
        )
        # Clean up multiple spaces and strip whitespace
        new_val = re.sub(r"\s+", " ", new_val).strip()
        return (
            new_val if new_val else ""
        )  # Return empty string if result is empty after cleaning

    original_track = track_name
    original_album = album_name

    # Apply cleaning to track name and album name
    cleaned_track = clean_string(track_name, remaster_keywords)
    cleaned_album = clean_string(album_name, remaster_keywords)

    # Remove specified album suffixes
    for suffix in album_suffixes:
        if cleaned_album.endswith(suffix):
            # Perform removal only if the suffix is at the very end
            cleaned_album = cleaned_album[: -len(suffix)].strip()
            # Log the removal
            console_logger.info(
                "Removed suffix '%s' from album. New album name: '%s'",
                suffix,
                cleaned_album,
            )

    # Log the cleaning results
    console_logger.info(
        "Original track name: '%s' -> '%s'", original_track, cleaned_track
    )
    console_logger.info(
        "Original album name: '%s' -> '%s'", original_album, cleaned_album
    )

    return cleaned_track, cleaned_album


def is_music_app_running(error_logger: logging.Logger) -> bool:
    """Check if the Music.app is currently running using subprocess. Logs errors.

    :param error_logger: Logger for error output.
    :return: True if Music.app is running, False otherwise.
    """
    try:
        script = (
            'tell application "System Events" to (name of processes) contains "Music"'
        )
        # trunk-ignore(bandit/B603)
        # trunk-ignore(ruff/S603)
        result = subprocess.run(
            ["/usr/bin/osascript", "-e", script],
            capture_output=True,
            text=True,
            check=False,
        )
        # Log stderr from osascript if any, even if check=False
        if result.stderr:
            error_logger.warning(
                "AppleScript stderr during Music.app status check: %s",
                result.stderr.strip(),
            )

        return result.stdout.strip().lower() == "true"
    except (subprocess.SubprocessError, OSError) as e:
        error_logger.error("Unable to check Music.app status: %s", e, exc_info=True)
        return False  # Assume not running on error
    except Exception as e:  # Catch any other unexpected exceptions
        error_logger.error(
            "Unexpected error checking Music.app status: %s", e, exc_info=True
        )
        return False
