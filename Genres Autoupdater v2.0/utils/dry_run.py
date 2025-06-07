#!/usr/bin/env python3

"""Dry Run Module.

This module provides a dry run simulation for cleaning and genre updates.
It defines classes that can be used by the main application to simulate
AppleScript interactions and processing logic without modifying the actual
music library.
"""

from __future__ import annotations

# Standard library imports
import asyncio
import logging

from datetime import datetime
from typing import Any

from services.applescript_client import AppleScriptClient, AppleScriptClientProtocol

# Utility function imports
from utils.metadata import (
    clean_names,
    determine_dominant_genre_for_artist,
    group_tracks_by_artist,
    has_genre,
    merge_genres,
    parse_tracks,
)

DRY_RUN_SUCCESS_MESSAGE = "Success (dry run)"


class DryRunAppleScriptClient(AppleScriptClientProtocol):
    """AppleScript client that logs actions instead of modifying the library."""

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
    ) -> None:
        """Initialize the DryRunAppleScriptClient with dependencies."""
        self._real_client: AppleScriptClient = AppleScriptClient(config, console_logger, error_logger)
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.config = config
        self.actions: list[dict[str, Any]] = []

    async def initialize(self) -> None:
        """Initialize the DryRunAppleScriptClient."""
        await self._real_client.initialize()

    async def run_script(
        self,
        script_name: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run an AppleScript by name in dry run mode."""
        if script_name.startswith("fetch"):
            result = await self._real_client.run_script(script_name, arguments, timeout)
            if not isinstance(result, str | None):
                self.error_logger.warning(
                    "Unexpected return type from _real_client.run_script: %s",
                    type(result).__name__,
                )
                return None
            return result
        self.console_logger.info(
            "DRY-RUN: Would run %s with args: %s",
            script_name,
            arguments or [],
        )
        self.actions.append({"script": script_name, "args": arguments or []})
        return DRY_RUN_SUCCESS_MESSAGE

    async def run_script_code(
        self,
        script_code: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run raw AppleScript code in dry run mode."""
        async def _execute() -> str:
            self.console_logger.info("DRY-RUN: Would execute inline AppleScript")
            self.actions.append({"code": script_code, "args": arguments or []})
            return DRY_RUN_SUCCESS_MESSAGE
        try:
            if timeout is not None:
                return await asyncio.wait_for(_execute(), timeout=timeout)
            return await _execute()
        except TimeoutError:
            self.error_logger.error(
                "Timeout after %s seconds while executing AppleScript", timeout
            )
            return None
        except Exception as e:
            self.error_logger.error(
                "Error executing AppleScript: %s", str(e), exc_info=True
            )
            return None

    def get_actions(self) -> list[dict[str, Any]]:
        """Get the list of actions performed during the dry run."""
        return self.actions

class DryRunProcessor:
    """Processes the dry run simulation steps using injected dependencies."""

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        ap_client: AppleScriptClientProtocol,
        test_artists: list[str] | None = None,
    ) -> None:
        """Initialize the DryRunProcessor with dependencies."""
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.ap_client = ap_client
        self.test_artists = test_artists or []

    async def fetch_tracks(self, artist: str | None = None) -> list[dict[str, str]]:
        """Fetch tracks asynchronously from Music.app via AppleScript using the injected client."""
        if artist is None and self.test_artists:
            self.console_logger.info(
                "Dry-run mode: fetching tracks only for test artists: %s",
                self.test_artists,
            )
            collected: dict[str, dict[str, str]] = {}
            for art in self.test_artists:
                for track in await self.fetch_tracks(art):
                    if track.get("id"):
                        collected[track["id"]] = track
            return list(collected.values())
        self.console_logger.info(
            "Fetching tracks using AppleScript client for dry run %s",
            f"for artist: {artist}" if artist else "",
        )
        script_name = "fetch_tracks.applescript"
        script_args = [artist] if artist else []
        timeout = self.config.get("applescript_timeout_seconds", 900)
        raw_data = await self.ap_client.run_script(script_name, script_args, timeout=timeout)
        if raw_data:
            lines_count = raw_data.count("\n") + 1
            self.console_logger.info(
                "AppleScript returned data: %d bytes, approximately %d lines",
                len(raw_data.encode('utf-8')),
                lines_count,
            )
        else:
            self.error_logger.error(
                "Empty response received from AppleScript client for %s. Possible script error.",
                script_name,
            )
            return []
        parsed = parse_tracks(raw_data, self.error_logger)
        if not isinstance(parsed, list) or not all(isinstance(x, dict) for x in parsed):
            self.error_logger.error("Unexpected return type from parse_tracks")
            return []
        self.console_logger.info("Successfully parsed %s tracks from Music.app output", len(parsed))
        return parsed

    async def simulate_cleaning(self) -> list[dict[str, str]]:
        """Simulate cleaning of track/album names without applying changes."""
        simulated_changes: list[dict[str, str]] = []
        tracks = await self.fetch_tracks()
        if not tracks:
            self.error_logger.error("No tracks found for cleaning simulation. Cannot proceed.")
            return []
        self.console_logger.info("Starting cleaning simulation for %s tracks...", len(tracks))
        for track in tracks:
            track_status = track.get("trackStatus", "").lower()
            if track_status in ("prerelease", "no longer available"):
                self.console_logger.debug(f"Skipping track with status '{track_status}': {track.get('name', 'Unnamed track')}")
                continue
            original_name = str(track.get("name", ""))
            original_album = str(track.get("album", ""))
            artist_name = str(track.get("artist", "Unknown"))
            track_id = str(track.get("id", ""))
            date_added = str(track.get("dateAdded", ""))
            cleaned_name, cleaned_album = clean_names(
                artist=artist_name,
                track_name=original_name,
                album_name=original_album,
                config=self.config,
                console_logger=self.console_logger,
                error_logger=self.error_logger,
            )
            if cleaned_name != original_name or cleaned_album != original_album:
                change: dict[str, str] = {
                    "change_type": "cleaning",
                    "track_id": track_id,
                    "artist": artist_name,
                    "original_name": original_name,
                    "cleaned_name": cleaned_name,
                    "original_album": original_album,
                    "cleaned_album": cleaned_album,
                    "dateAdded": date_added,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                simulated_changes.append(change)
                self.console_logger.debug(f"Simulated cleaning change for '{artist_name}' - '{original_name}'")
        self.console_logger.info("Cleaning simulation found %s changes.", len(simulated_changes))
        return simulated_changes

    async def simulate_genre_update(self) -> list[dict[str, str]]:
        """Simulate genre updates without applying changes."""
        simulated_changes: list[dict[str, str]] = []
        tracks = await self.fetch_tracks()
        if not tracks:
            self.error_logger.error("No tracks found for genre update simulation. Cannot proceed.")
            return []
        self.console_logger.info("Starting genre update simulation for %s tracks...", len(tracks))
        artist_tracks = group_tracks_by_artist(tracks)
        self.console_logger.info("Grouped tracks into %s artists for genre analysis", len(artist_tracks))
        for artist, artist_tracks_list in artist_tracks.items():
            if not artist_tracks_list:
                continue
            dominant_genre = determine_dominant_genre_for_artist(
                artist_tracks_list,
                self.error_logger,
            )
            if not dominant_genre:
                self.console_logger.debug(f"Could not determine dominant genre for artist: {artist}")
                continue
            for track in artist_tracks_list:
                current_genre = str(track.get("genre", ""))
                track_status = track.get("trackStatus", "").lower()
                track_id = str(track.get("id", ""))
                track_name = str(track.get("name", ""))
                date_added = str(track.get("dateAdded", ""))
                non_modifiable_statuses = ("purchased", "matched", "uploaded", "ineligible", "no longer available", "not eligible for upload")
                if has_genre(current_genre, dominant_genre) or track_status in non_modifiable_statuses:
                    continue
                if track_status in ("subscription", "downloaded"):
                    new_genre = merge_genres(current_genre, dominant_genre)
                    change: dict[str, str] = {
                        "change_type": "genre_update",
                        "track_id": track_id,
                        "artist": artist,
                        "track_name": track_name,
                        "original_genre": current_genre,
                        "new_genre": new_genre,
                        "dateAdded": date_added,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    simulated_changes.append(change)
                else:
                    self.console_logger.debug(f"Skipping track with unknown status '{track_status}': {track_name}")
        self.console_logger.info("Genre update simulation found %s changes.", len(simulated_changes))
        return simulated_changes
