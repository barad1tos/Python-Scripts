#!/usr/bin/env python3

"""Dry Run Module.

This module provides a dry run simulation for cleaning and genre updates.
It fetches tracks directly using AppleScript (via the DependencyContainer's client),
simulates cleaning and genre updates, and saves the changes to a unified CSV report.
This allows users to preview all potential changes without modifying the actual music library.

Refactored to use Dependency Injection for accessing services like
AppleScriptClient and loggers.
"""

from __future__ import annotations

# Standard library imports
import asyncio
import logging
import os
import sys
import time

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

# Third-party imports
# trunk-ignore(mypy/import-untyped)
# trunk-ignore(mypy/note)
import yaml

# Local application imports
if TYPE_CHECKING:
    from services.applescript_client import AppleScriptClient
from utils.config import load_config
from utils.logger import get_full_log_path, get_loggers
from utils.metadata import (
    clean_names,
    determine_dominant_genre_for_artist,
    group_tracks_by_artist,
    has_genre,
    is_music_app_running,
    merge_genres,
    parse_tracks,
)
from utils.reports import save_unified_dry_run

# Adjusting path to the project root directory where my-config.yaml is located
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, "..")

# Ensure the project root is in sys.path for imports like services.* or utils.*
if project_root not in sys.path:
    sys.path.insert(0, project_root)

CONFIG_PATH = os.path.join(project_root, "my-config.yaml")

try:
    CONFIG = load_config(CONFIG_PATH)
except (FileNotFoundError, ValueError, yaml.YAMLError) as e:
    print(f"ERROR: Failed to load or validate configuration: {e}", file=sys.stderr)
    sys.exit(1)  # Exit if config loading fails

console_logger, error_logger, analytics_logger, listener = get_loggers(CONFIG)

DRY_RUN_SUCCESS_MESSAGE = "Success (dry run)"


@runtime_checkable
class AppleScriptClientProtocol(Protocol):
    """Protocol defining the interface for AppleScript clients.

    This allows both AppleScriptClient and DryRunAppleScriptClient to be used
    interchangeably as long as they implement these methods.
    """

    async def run_script(
        self,
        script_name: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run an AppleScript by name.

        Args:
            script_name: Name of the script to run
            arguments: Optional list of arguments to pass to the script
            timeout: Optional timeout in seconds

        Returns:
            Script output as string, or None if failed

        """
        ...

    async def run_script_code(
        self,
        script_code: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Run raw AppleScript code.

        Args:
            script_code: The AppleScript code to execute
            arguments: Optional list of arguments to pass to the script
            timeout: Optional timeout in seconds

        Returns:
            Script output as string, or None if failed

        """
        ...

    async def initialize(self) -> None:
        """Initialize the AppleScript client.

        This method should be called before any other methods.
        """
        ...


class DryRunAppleScriptClient(AppleScriptClientProtocol):
    """AppleScript client that logs actions instead of modifying the library."""

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
    ) -> None:
        """Initialize the DryRunAppleScriptClient with necessary services."""
        self._real_client: AppleScriptClient = AppleScriptClient(config, console_logger, error_logger)
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.config = config
        self.actions: list[dict[str, Any]] = []

    async def initialize(self) -> None:
        """Initialize the client."""
        await self._real_client.initialize()

    async def run_script(
        self,
        script_name: str,
        arguments: list[str] | None = None,
        timeout: float | None = None,
    ) -> str | None:
        """Execute an AppleScript script in dry run mode."""
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
        """Execute inline AppleScript in dry run mode with optional timeout.

        Args:
            script_code: The AppleScript code to execute
            arguments: Optional list of arguments to pass to the script
            timeout: Maximum time in seconds to wait for execution, or None for no timeout

        Returns:
            str: Success message if successful, None otherwise

        """
        async def _execute() -> str:
            """Execute the dry-run operation."""
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
    ):
        """Initialize the DryRunProcessor with necessary services."""
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.ap_client = ap_client
        self.test_artists = test_artists or []

    async def fetch_tracks(self, artist: str | None = None) -> list[dict[str, str]]:
        """Fetch tracks asynchronously from Music.app via AppleScript using the injected client.

        This replaces the old fetch_tracks_direct function.
        Uses the parse_tracks utility from metadata_helpers.py.
        """
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

        # script_path logic is now part of AppleScriptClient, just need script name
        script_name = "fetch_tracks.applescript"
        script_args = [artist] if artist else []

        # Use timeout from config, fallback to 900 (15 mins)
        timeout = self.config.get("applescript_timeout_seconds", 900)

        # Use the injected ap_client
        raw_data = await self.ap_client.run_script(script_name, script_args, timeout=timeout)

        if raw_data:
            lines_count = raw_data.count("\n") + 1
            self.console_logger.info(
                "AppleScript returned data: %d bytes, approximately %d lines",
                len(raw_data),
                lines_count,
            )
        else:
            # Error logging is handled by AppleScriptClient, but we can add context here
            self.error_logger.error(
                "Empty response received from AppleScript client for %s. Possible script error.",
                script_name,
            )
            return []

        # Use the parse_tracks utility function from metadata_helpers.py
        # Pass the error_logger from the processor instance
        parsed = parse_tracks(raw_data, self.error_logger)

        # Verify the return type is as expected
        if not isinstance(parsed, list) or not all(isinstance(x, dict) for x in parsed):
            self.error_logger.error("Unexpected return type from parse_tracks")
            return []

        self.console_logger.info("Successfully parsed %s tracks from Music.app output", len(parsed))
        return parsed

    async def simulate_cleaning(self) -> list[dict[str, str]]:
        """Simulate cleaning of track/album names without applying changes.

        Returns:
            list[dict[str, str]]: A list of dictionaries containing cleaning changes.
            Each dictionary has string keys and string values.

        """
        simulated_changes: list[dict[str, str]] = []

        # Use the fetch_tracks method of this class
        tracks = await self.fetch_tracks()

        if not tracks:
            self.error_logger.error("No tracks found for cleaning simulation. Cannot proceed.")
            return []

        self.console_logger.info("Starting cleaning simulation for %s tracks...", len(tracks))

        for track in tracks:
            # Skip tracks with certain statuses
            track_status = track.get("trackStatus", "").lower()
            if track_status in ("prerelease", "no longer available"):
                self.console_logger.debug(f"Skipping track with status '{track_status}': {track.get('name', 'Unnamed track')}")
                continue

            original_name = str(track.get("name", ""))
            original_album = str(track.get("album", ""))
            artist_name = str(track.get("artist", "Unknown"))
            track_id = str(track.get("id", ""))
            date_added = str(track.get("dateAdded", ""))

            # Use the clean_names utility function from metadata_helpers.py
            cleaned_name, cleaned_album = clean_names(
                artist=artist_name,
                track_name=original_name,
                album_name=original_album,
                config=self.config,
                console_logger=self.console_logger,
                error_logger=self.error_logger,
            )

            # If either name or album was cleaned, add to changes
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
        """Simulate genre updates without applying changes.

        Returns:
            list[dict[str, str]]: A list of simulated 'genre_update' changes.
            Uses group_tracks_by_artist and determine_dominant_genre_for_artist.

        """
        simulated_changes: list[dict[str, str]] = []

        # Use the fetch_tracks method of this class
        tracks = await self.fetch_tracks()

        if not tracks:
            self.error_logger.error("No tracks found for genre update simulation. Cannot proceed.")
            return []

        self.console_logger.info("Starting genre update simulation for %s tracks...", len(tracks))

        # Group tracks by artist for genre analysis
        artist_tracks = group_tracks_by_artist(tracks)
        self.console_logger.info("Grouped tracks into %s artists for genre analysis", len(artist_tracks))

        # Process each artist's tracks
        for artist, artist_tracks_list in artist_tracks.items():
            if not artist_tracks_list:
                continue

            # Get dominant genre for this artist's tracks
            dominant_genre = determine_dominant_genre_for_artist(
                artist_tracks_list,
                self.error_logger,
            )

            if not dominant_genre:
                self.console_logger.debug(f"Could not determine dominant genre for artist: {artist}")
                continue

            # Check each track for this artist
            for track in artist_tracks_list:
                current_genre = str(track.get("genre", ""))
                track_status = track.get("trackStatus", "").lower()
                track_id = str(track.get("id", ""))
                track_name = str(track.get("name", ""))
                date_added = str(track.get("dateAdded", ""))

                # Skip if genre is already correct or track status doesn't allow modification
                non_modifiable_statuses = ("purchased", "matched", "uploaded", "ineligible", "no longer available", "not eligible for upload")

                if has_genre(current_genre, dominant_genre) or track_status in non_modifiable_statuses:
                    self.console_logger.debug(
                        f"Skipping genre update for track {track_name} by {artist}: "
                        f"Already has genre '{current_genre}' or status '{track_status}' does not allow modification"
                    )
                    continue

                # Check track status to determine if it's eligible for genre updates
                if track_status in ("matched", "uploaded"):
                    self.console_logger.debug(
                        f"Skipping genre simulation for track ID {track_id}: Status '{track_status}' does not allow modification"
                    )
                elif track_status in ("subscription", "downloaded"):
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
                    self.console_logger.debug(f"Simulated genre change for '{artist}' - '{track_name}': {current_genre} -> {dominant_genre}")
                else:
                    self.console_logger.debug(f"Skipping track with unknown status '{track_status}': {track_name}")

        self.console_logger.info("Genre update simulation found %s changes.", len(simulated_changes))
        return simulated_changes

    # Optional: Add simulate_year_updates method here following plan step 3.2 (later)
    # async def simulate_year_updates(self): ...


# --- Main Execution Block ---
def main() -> None:
    """Run the dry run simulation.

    Initializes dependencies and orchestrates the simulation process.
    """
    from services.dependencies_service import DependencyContainer  # Moved import here
    start_all = time.time()
    # Argument parsing is not handled within the dry_run script itself in the original structure,
    # it's handled by music_genre_updater.py which then *calls* dry_run.main().
    # We will assume dry_run.main is called without arguments, as in the original.
    # If dry_run needs arguments (like --artist), they should be passed to dry_run.main().
    # For now, proceeding assuming no arguments are passed to dry_run.main() directly.
    # If dry_run needs artist filtering, it would need to receive it, maybe via DryRunProcessor __init__
    # or fetch_tracks method. Sticking to original behavior (simulates for all tracks) for now.
    # args = parse_arguments() # Removed - handled by the calling script

    # Loggers and listener are initialized at the top level
    # console_logger, error_logger, analytics_logger, listener = get_loggers(CONFIG) # Already initialized

    # Ensure Music.app is running using the utility function
    # Pass error_logger from the dry run script's scope
    if not is_music_app_running(error_logger):
        console_logger.error("Music app is not running! Please start Music.app before running the dry run script.")
        sys.exit(1)  # Exit if Music app is not running

    # Log configured test artists if any
    test_artists = CONFIG.get("development", {}).get("test_artists", [])
    if test_artists:
        console_logger.info("Dry-run will process only test artists: %s", test_artists)

    # --- Initialize Dependency Container ---
    # The DependencyContainer needs the CONFIG and loggers
    deps = None
    dry_run_processor = None
    try:
        # Create the DependencyContainer instance, passing config and loggers
        # DependencyContainer expects loggers to be available when initialized
        deps = DependencyContainer(CONFIG_PATH, console_logger, error_logger, analytics_logger, listener)  # Pass all logger instances
        # Passing CONFIG_PATH makes deps load it again. This is acceptable for now.
        # A future refactor might pass the loaded CONFIG dict directly to DependencyContainer.

        # Get the required services from the container
        ap_client = deps.ap_client
        # Optional: get external_api_service if simulating year updates later
        # external_api_service = deps.external_api_service

        # Create an instance of the DryRunProcessor, passing the dependencies
        dry_run_processor = DryRunProcessor(
            config=CONFIG,  # Pass the globally loaded CONFIG
            console_logger=console_logger,
            error_logger=error_logger,
            ap_client=ap_client,
            test_artists=test_artists,
            # Optional: pass external_api_service here
        )

        # Run the simulation methods on the processor instance
        # Use asyncio.run to execute the main async logic
        async def run_simulations() -> None:
            cleaning_changes = await dry_run_processor.simulate_cleaning()
            genre_changes = await dry_run_processor.simulate_genre_update()
            # Optional: Call year simulation here
            # year_changes = await dry_run_processor.simulate_year_updates() # needs implementation

            # We get the path for the combined report from the configuration
            dry_run_report_file = get_full_log_path(CONFIG, "dry_run_report_file", "csv/dry_run_combined.csv", error_logger)

            # We save the combined report using the utility function
            # Pass loggers from the dry run script's scope
            save_unified_dry_run(
                cleaning_changes,
                genre_changes,
                dry_run_report_file,
                console_logger,
                error_logger,
            )

            console_logger.info(
                "Dry run simulation completed with %s cleaning changes and %s genre changes.",
                len(cleaning_changes),
                len(genre_changes),
            )
            # Log total changes saved
            console_logger.info(
                "Total simulated changes saved to report: %s",
                len(cleaning_changes) + len(genre_changes),
            )

        asyncio.run(run_simulations())

    except KeyboardInterrupt:
        if console_logger:
            console_logger.info("Dry run interrupted by user.")
        else:
            print("INFO: Dry run interrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Use error logger from the main scope
        if error_logger:
            error_logger.critical("Critical error during dry run execution: %s", e, exc_info=True)
        else:
            print(
                f"CRITICAL ERROR: Critical error during dry run execution: {e}",
                file=sys.stderr,
            )
            import traceback

            traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        # Ensure DependentContainer shutdown logic is called if deps was created
        if deps:
            try:
                # Assuming DependencyContainer has a shutdown method for cleanup (e.g. closing API session)
                # Check if the method exists before calling it
                if hasattr(deps, "shutdown") and callable(deps.shutdown):
                    console_logger.info("Shutting down DependencyContainer...")
                    deps.shutdown()  # Call the shutdown method
                else:
                    console_logger.warning("DependencyContainer instance has no shutdown method.")

            except Exception as shutdown_e:
                # Use error logger from the main scope
                if error_logger:
                    error_logger.error(
                        f"Error during DependencyContainer shutdown: {shutdown_e}",
                        exc_info=True,
                    )
                else:
                    print(
                        f"ERROR: Error during DependencyContainer shutdown: {shutdown_e}",
                        file=sys.stderr,
                    )

        # Stop the QueueListener when the script finishes or encounters a critical error
        # Check if the listener was successfully initialized before trying to stop it
        if listener is not None and hasattr(listener, "stop"):
            try:
                if console_logger:
                    console_logger.info("Stopping QueueListener (dry run)...")
                else:
                    print("INFO: Stopping QueueListener (dry run)...", file=sys.stderr)
                listener.stop()
            except Exception as e:
                if console_logger:
                    console_logger.warning(f"Error stopping QueueListener: {e}")
                else:
                    print(f"WARNING: Error stopping QueueListener: {e}", file=sys.stderr)

        # Explicitly close handlers for robustness, especially in case of errors before listener stops cleanly
        # This part is likely redundant if listener.stop() and handler.close() are properly implemented,
        # but kept for safety based on original code.
        if console_logger:
            console_logger.info("Explicitly closing logger handlers (dry run)...")
        else:
            print("INFO: Explicitly closing logger handlers (dry run)...", file=sys.stderr)

        # Use module-level loggers or root logger to get handlers
        all_handlers = []
        # Close root handlers first
        for handler in logging.root.handlers[:]:
            all_handlers.append(handler)
            logging.root.removeHandler(handler)
        # Close handlers attached to specific loggers (console, error, analytics, year_updates)
        # Use the names of the loggers initialized by get_loggers
        for name in [
            "console_logger",
            "main_logger",
            "analytics_logger",
            "year_updates",
        ]:
            logger = logging.getLogger(name)
            for handler in logger.handlers[:]:
                all_handlers.append(handler)
                logger.removeHandler(handler)

        # Close all collected handlers
        for handler in all_handlers:
            try:
                handler.close()
            except Exception as e:
                print(f"Error closing handler {handler}: {e}", file=sys.stderr)

    end_all = time.time()
    print(f"\nTotal script execution time (dry run): {end_all - start_all:.2f} seconds")
    # sys.exit(0) # Exit status is handled by explicit sys.exit calls above


if __name__ == "__main__":
    main()
