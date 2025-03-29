#!/usr/bin/env python3
"""
Pending Verification Module

This module maintains a list of albums that need re-verification in the future.
When an album's year cannot be definitely determined from external sources,
it is added to this list with a timestamp. On future runs, albums whose
verification period has elapsed will be checked again.

Usage:
    service = PendingVerificationService(config, console_logger, error_logger)

    # Mark album for future verification
    service.mark_for_verification("Pink Floyd", "The Dark Side of the Moon")

    # Check if album needs verification now
    if service.is_verification_needed("Pink Floyd", "The Dark Side of the Moon"):
        # Perform verification
"""

import csv
import os

from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple


class PendingVerificationService:
    """
    Service to track albums needing future verification of their release year.

    Attributes:
        pending_file_path (str): Path to the CSV file storing pending verifications
        console_logger: Logger for console output
        error_logger: Logger for error output
        verification_interval_days (int): Days to wait before re-checking an album
        pending_albums (Dict): Cache of pending albums and their re-check timestamps
    """

    def __init__(self, config: Dict, console_logger, error_logger):
        """
        Initialize the PendingVerificationService.

        Args:
            config: Application configuration dictionary
            console_logger: Logger for console output
            error_logger: Logger for error output

        Example:
            service = PendingVerificationService(config, console_logger, error_logger)
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger

        # Get verification interval from config or use default (30 days)
        self.verification_interval_days = config.get("year_retrieval", {}).get("pending_verification_interval_days", 30)

        # Set up the pending file path
        logs_base_dir = config.get("logs_base_dir", ".")
        self.pending_file_path = os.path.join(logs_base_dir, "csv", "pending_year_verification.csv")

        # Initialize in-memory cache of pending albums
        self.pending_albums = {}

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.pending_file_path), exist_ok=True)

        # Load existing pending albums
        self._load_pending_albums()

    def _load_pending_albums(self) -> None:
        """
        Load the list of pending albums from the CSV file.

        Example:
            service._load_pending_albums()  # Loads data from CSV into memory
        """
        if not os.path.exists(self.pending_file_path):
            self.console_logger.info(f"Pending verification file not found, will create at: {self.pending_file_path}")
            return

        try:
            with open(self.pending_file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    artist = row.get("artist", "").strip()
                    album = row.get("album", "").strip()
                    timestamp_str = row.get("timestamp", "").strip()

                    if artist and album and timestamp_str:
                        try:
                            # Parse timestamp to check if verification period has elapsed
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                            key = f"{artist}|||{album}"
                            self.pending_albums[key] = timestamp
                        except ValueError:
                            self.error_logger.warning(f"Invalid timestamp format in pending file: {timestamp_str}")

            self.console_logger.info(f"Loaded {len(self.pending_albums)} pending albums for verification")
        except Exception as e:
            self.error_logger.error(f"Error loading pending verification file: {e}")

    def _save_pending_albums(self) -> None:
        """
        Save the current list of pending albums to the CSV file.

        Example:
            service._save_pending_albums()  # Writes pending albums to CSV
        """
        try:
            with open(self.pending_file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["artist", "album", "timestamp"])
                writer.writeheader()

                for key, timestamp in self.pending_albums.items():
                    artist, album = key.split("|||", 1)
                    writer.writerow({"artist": artist, "album": album, "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")})

            self.console_logger.info(f"Saved {len(self.pending_albums)} pending albums for verification")
        except Exception as e:
            self.error_logger.error(f"Error saving pending verification file: {e}")

    def mark_for_verification(self, artist: str, album: str) -> None:
        """
        Mark an album for future verification.

        Args:
            artist: Artist name
            album: Album name

        Example:
            service.mark_for_verification("Pink Floyd", "The Dark Side of the Moon")
        """
        key = f"{artist}|||{album}"
        self.pending_albums[key] = datetime.now()
        self.console_logger.info(f"Marked '{artist} - {album}' for verification in {self.verification_interval_days} days")
        self._save_pending_albums()

    def is_verification_needed(self, artist: str, album: str) -> bool:
        """
        Check if an album needs verification now.

        Args:
            artist: Artist name
            album: Album name

        Returns:
            True if verification period has elapsed, False otherwise

        Example:
            if service.is_verification_needed("Pink Floyd", "The Dark Side of the Moon"):
                # Perform verification
        """
        key = f"{artist}|||{album}"
        if key not in self.pending_albums:
            return False

        timestamp = self.pending_albums[key]
        verification_time = timestamp + timedelta(days=self.verification_interval_days)

        if datetime.now() >= verification_time:
            # Verification period has elapsed
            self.console_logger.info(f"Verification period elapsed for '{artist} - {album}'")
            return True

        return False

    def remove_from_pending(self, artist: str, album: str) -> None:
        """
        Remove an album from the pending verification list.

        Args:
            artist: Artist name
            album: Album name

        Example:
            service.remove_from_pending("Pink Floyd", "The Dark Side of the Moon")
        """
        key = f"{artist}|||{album}"
        if key in self.pending_albums:
            del self.pending_albums[key]
            self.console_logger.info(f"Removed '{artist} - {album}' from pending verification")
            self._save_pending_albums()

    def get_all_pending_albums(self) -> List[Tuple[str, str, datetime]]:
        """
        Get a list of all pending albums with their verification timestamps.

        Returns:
            List of tuples containing (artist, album, timestamp)

        Example:
            pending_list = service.get_all_pending_albums()
            for artist, album, timestamp in pending_list:
                print(f"{artist} - {album}: {timestamp}")
        """
        result = []
        for key, timestamp in self.pending_albums.items():
            artist, album = key.split("|||", 1)
            result.append((artist, album, timestamp))
        return result

    def get_verified_album_keys(self) -> Set[str]:
        """
        Get the set of album keys (artist|||album) that need verification now.

        Returns:
            Set of album keys needing verification

        Example:
            for key in service.get_verified_album_keys():
                artist, album = key.split("|||", 1)
                # Perform verification
        """
        now = datetime.now()
        verified_keys = set()

        for key, timestamp in list(self.pending_albums.items()):
            verification_time = timestamp + timedelta(days=self.verification_interval_days)
            if now >= verification_time:
                artist, album = key.split("|||", 1)
                self.console_logger.info(f"Album '{artist} - {album}' needs verification")
                verified_keys.add(key)

        return verified_keys
