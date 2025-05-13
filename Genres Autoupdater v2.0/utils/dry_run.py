#!/usr/bin/env python3

"""
Dry Run Module

This module provides a dry run simulation for cleaning and genre updates.
It fetches tracks directly using AppleScript (via the DependencyContainer's client),
simulates cleaning and genre updates, and saves the changes to a unified CSV report.
This allows users to preview all potential changes without modifying the actual music library.

Refactored to use Dependency Injection for accessing services like
AppleScriptClient and loggers.
"""

import asyncio
import logging
import os
import sys
import time

from datetime import datetime

# Removed subprocess import, as it's now only used within is_music_app_running utility
from typing import Any, Dict, List

import yaml

from services.applescript_client import AppleScriptClient  # For type hinting

# Import DependencyContainer and services for type hinting and instantiation
from services.dependencies_service import DependencyContainer

# Import necessary utilities that are NOT part of the dependency cycle
from utils.config import load_config  # Still needed to load config initially
from utils.logger import get_full_log_path, get_loggers  # Still needed to set up loggers

# Import utility functions from the metadata_helpers module
# These functions are now independent helpers
from utils.metadata import clean_names  # Used by simulate_cleaning
from utils.metadata import determine_dominant_genre_for_artist  # Used by simulate_genre_update
from utils.metadata import group_tracks_by_artist  # Used by simulate_genre_update
from utils.metadata import is_music_app_running  # Used by main for initial check
from utils.metadata import parse_tracks  # Used by fetch_tracks
from utils.reports import save_unified_dry_run  # Still needed to save the report

# Adjusting path to the project root directory where my-config.yaml is located
current_dir = os.path.dirname(os.path.abspath(__file__))
# Go up one directory from utils to the project root
project_root = os.path.join(current_dir, "..")

# Ensure the project root is in sys.path for imports like services.* or utils.*
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# Load the configuration early, as loggers and DependencyContainer need it
CONFIG_PATH = os.path.join(project_root, "my-config.yaml")

try:
    # Load CONFIG using the utility function
    CONFIG = load_config(CONFIG_PATH)
except (FileNotFoundError, ValueError, yaml.YAMLError) as e:
    # Use basic print if loggers are not yet set up
    print(f"ERROR: Failed to load or validate configuration: {e}", file=sys.stderr)
    sys.exit(1)  # Exit if config loading fails

# Initialize loggers using the loaded CONFIG
# These loggers will be passed to the DependencyContainer and DryRunProcessor
console_logger, error_logger, analytics_logger, listener = get_loggers(CONFIG)


# --- DryRunProcessor Class ---
# This class encapsulates the simulation logic and holds its dependencies
class DryRunProcessor:
    """
    Processes the dry run simulation steps using injected dependencies.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        ap_client: AppleScriptClient,  # Receives AppleScriptClient instance
        # Optional: add external_api_service here if year simulation is added later
        # external_api_service: ExternalApiService
    ):
        """
        Initializes the DryRunProcessor with necessary services.
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.ap_client = ap_client

    async def fetch_tracks(self, artist: str = None) -> List[Dict[str, str]]:
        """
        Fetches tracks asynchronously from Music.app via AppleScript using the injected client.
        This replaces the old fetch_tracks_direct function.
        Uses the parse_tracks utility from metadata_helpers.py.
        """
        self.console_logger.info("Fetching tracks using AppleScript client for dry run %s", f"for artist: {artist}" if artist else "")

        # script_path logic is now part of AppleScriptClient, just need script name
        script_name = "fetch_tracks.applescript"
        script_args = [artist] if artist else []

        # Use timeout from config, fallback to 900 (15 mins)
        timeout = self.config.get("applescript_timeout_seconds", 900)

        # Use the injected ap_client
        raw_data = await self.ap_client.run_script(script_name, script_args, timeout=timeout)

        if raw_data:
            lines_count = raw_data.count('\n') + 1
            self.console_logger.info("AppleScript returned data: %d bytes, approximately %d lines", len(raw_data), lines_count)
        else:
            # Error logging is handled by AppleScriptClient, but we can add context here
            self.error_logger.error("Empty response received from AppleScript client for %s. Possible script error.", script_name)
            return []

        # Use the parse_tracks utility function from metadata_helpers.py
        # Pass the error_logger from the processor instance
        tracks = parse_tracks(raw_data, self.error_logger)

        self.console_logger.info("Successfully parsed %s tracks from Music.app output", len(tracks))
        return tracks

    async def simulate_cleaning(self) -> List[Dict[str, str]]:
        """
        Simulate cleaning of track/album names without applying changes.
        Returns a list of simulated cleaning changes.
        Uses the clean_names utility from metadata_helpers.py.
        """
        simulated_changes = []

        # Use the fetch_tracks method of this class
        tracks = await self.fetch_tracks()

        if not tracks:
            self.error_logger.error("Failed to fetch tracks for dry run cleaning simulation!")
            return []

        self.console_logger.info("Simulating cleaning for %s tracks", len(tracks))

        for track in tracks:
            # Skip tracks with statuses that should not be cleaned
            if track.get("trackStatus", "").lower() in ("prerelease", "no longer available"):
                self.console_logger.debug(
                    f"Skipping cleaning for track ID {track.get('id', 'N/A')} due to status '{track.get('trackStatus', 'N/A')}'"
                )
                continue

            original_name = track.get("name", "")
            original_album = track.get("album", "")
            artist_name = track.get("artist", "Unknown")

            # Use the clean_names utility function from metadata_helpers.py
            # Pass the CONFIG, console_logger, and error_logger from the processor instance
            cleaned_name, cleaned_album = clean_names(artist_name, original_name, original_album, self.config, self.console_logger, self.error_logger)

            if cleaned_name != original_name or cleaned_album != original_album:
                simulated_changes.append(
                    {
                        "change_type": "cleaning",
                        "track_id": track.get("id", ""),
                        "artist": artist_name,
                        "original_name": original_name,
                        "cleaned_name": cleaned_name,
                        "original_album": original_album,
                        "cleaned_album": cleaned_album,
                        "dateAdded": track.get("dateAdded", ""),  # Keep original dateAdded for context
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Add timestamp for the change
                    }
                )
                self.console_logger.debug(f"Simulated cleaning change for '{artist_name}' - '{original_name}'")  # Log simulated change

        self.console_logger.info("Cleaning simulation found %s changes.", len(simulated_changes))
        return simulated_changes

    async def simulate_genre_update(self) -> List[Dict[str, str]]:
        """
        Simulate genre updates without applying changes.
        Returns a list of simulated 'genre_update' changes.
        Uses group_tracks_by_artist and determine_dominant_genre_for_artist from metadata_helpers.py.
        """
        simulated_changes = []

        # Use the fetch_tracks method of this class
        tracks = await self.fetch_tracks()

        if not tracks:
            self.error_logger.error("Failed to fetch tracks for genre update simulation!")
            return []

        # Skip tracks with unusable status for genre updates
        tracks_to_process = [t for t in tracks if t.get("trackStatus", "").lower() not in ("prerelease", "no longer available")]
        self.console_logger.info(
            "Simulating genre updates for %s tracks (skipped %s unusable tracks)", len(tracks_to_process), len(tracks) - len(tracks_to_process)
        )

        # Use group_tracks_by_artist utility function from metadata_helpers.py
        # This utility function does not require loggers
        grouped_by_artist = group_tracks_by_artist(tracks_to_process)

        # We determine the dominant genre for each artist
        for artist, artist_tracks in grouped_by_artist.items():
            if not artist_tracks:
                continue  # Should not happen with group_tracks_by_artist, but safety check
            try:
                # Use determine_dominant_genre_for_artist utility function from metadata_helpers.py
                # Pass the error_logger from the processor instance
                dominant_genre = determine_dominant_genre_for_artist(artist_tracks, self.error_logger)
            except Exception as e:
                self.error_logger.error(f"Error determining dominant genre for artist '{artist}' during simulation: {e}", exc_info=True)
                dominant_genre = "Unknown"  # Default on error

            if dominant_genre == "Unknown":
                self.console_logger.debug(f"Skipping genre update simulation for artist '{artist}': Dominant genre could not be determined.")
                continue  # Skip if dominant genre is unknown

            for track in artist_tracks:
                current_genre = track.get("genre", "").strip()
                track_status = track.get("trackStatus", "").lower()

                # Check if update is needed and status allows modification (simulation still respects this logic)
                if current_genre != dominant_genre and track_status in ("subscription", "downloaded"):
                    simulated_changes.append(
                        {
                            "change_type": "genre_update",
                            "track_id": track.get("id", ""),
                            "artist": artist,
                            "album": track.get("album", ""),
                            "track_name": track.get("name", ""),
                            "original_genre": current_genre,
                            "simulated_genre": dominant_genre,
                            "dateAdded": track.get("dateAdded", ""),  # Keep original dateAdded for context
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Add timestamp for the change
                        }
                    )
                    self.console_logger.debug(
                        f"Simulated genre change for '{artist}' - '{track.get('name', '')}': {current_genre} -> {dominant_genre}"
                    )  # Log simulated change

                elif current_genre == dominant_genre:
                    self.console_logger.debug(
                        f"Skipping genre simulation for track ID {track.get('id', 'N/A')}: Genre already matches dominant ({dominant_genre})"
                    )
                elif track_status not in ("subscription", "downloaded"):
                    self.console_logger.debug(
                        f"Skipping genre simulation for track ID {track.get('id', 'N/A')}: Status '{track_status}' does not allow modification"
                    )

        self.console_logger.info("Genre update simulation found %s changes.", len(simulated_changes))
        return simulated_changes

    # Optional: Add simulate_year_updates method here following plan step 3.2 (later)
    # async def simulate_year_updates(self): ...


# --- Main Execution Block ---
def main() -> None:
    """
    Main synchronous function to run the dry run simulation.
    Initializes dependencies and orchestrates the simulation process.
    """
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

    # --- Initialize Dependency Container ---
    # The DependencyContainer needs the CONFIG and loggers
    deps = None
    dry_run_processor = None
    try:
        # Create the DependencyContainer instance, passing config and loggers
        # DependencyContainer expects loggers to be available when initialized
        deps = DependencyContainer(CONFIG_PATH)  # DependencyContainer loads config internally, but we already have it loaded globally for loggers.
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
            # Optional: pass external_api_service here
        )

        # Run the simulation methods on the processor instance
        # Use asyncio.run to execute the main async logic
        async def run_simulations():
            cleaning_changes = await dry_run_processor.simulate_cleaning()
            genre_changes = await dry_run_processor.simulate_genre_update()
            # Optional: Call year simulation here
            # year_changes = await dry_run_processor.simulate_year_updates() # needs implementation

            # Combine all changes
            all_simulated_changes = cleaning_changes + genre_changes  # + year_changes if implemented

            # We get the path for the combined report from the configuration
            # Use config loaded at the top and loggers
            dry_run_report_file = get_full_log_path(CONFIG, "dry_run_report_file", "csv/dry_run_combined.csv", error_logger)

            # We save the combined report using the utility function
            # Pass loggers from the dry run script's scope
            save_unified_dry_run(all_simulated_changes, dry_run_report_file, console_logger, error_logger)

            console_logger.info(
                "Dry run simulation completed with %s cleaning changes and %s genre changes.", len(cleaning_changes), len(genre_changes)
            )
            # Log total changes saved
            console_logger.info("Total simulated changes saved to report: %s", len(all_simulated_changes))

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
            print(f"CRITICAL ERROR: Critical error during dry run execution: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        # Ensure DependentContainer shutdown logic is called if deps was created
        if deps:
            try:
                # Assuming DependencyContainer has a shutdown method for cleanup (e.g. closing API session)
                # Check if the method exists before calling it
                if hasattr(deps, 'shutdown') and callable(deps.shutdown):
                    console_logger.info("Shutting down DependencyContainer...")
                    deps.shutdown()  # Call the shutdown method
                else:
                    console_logger.warning("DependencyContainer instance has no shutdown method.")

            except Exception as shutdown_e:
                # Use error logger from the main scope
                if error_logger:
                    error_logger.error(f"Error during DependencyContainer shutdown: {shutdown_e}", exc_info=True)
                else:
                    print(f"ERROR: Error during DependencyContainer shutdown: {shutdown_e}", file=sys.stderr)

        # Stop the QueueListener when the script finishes or encounters a critical error
        # Check if the listener was successfully initialized before trying to stop it
        if listener:
            if console_logger:
                console_logger.info("Stopping QueueListener (dry run)...")
            else:
                print("INFO: Stopping QueueListener (dry run)...", file=sys.stderr)
            listener.stop()

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
        for name in ['console_logger', 'main_logger', 'analytics_logger', 'year_updates']:
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
