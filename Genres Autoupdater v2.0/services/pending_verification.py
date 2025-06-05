#!/usr/bin/env python3
"""Pending Verification Module.

This module maintains a list of albums that need re-verification in the future.
When an album's year cannot be definitely determined from external sources,
it is added to this list with a timestamp. On future runs, albums whose
verification period has elapsed will be checked again.

File operations (_load_pending_albums, _save_pending_albums) are asynchronous
using asyncio's run_in_executor to avoid blocking the event loop.

Refactored: Initial asynchronous loading handled in a separate async initialize method,
called by DependencyContainer after service instantiation.

Usage:
    service = PendingVerificationService(config, console_logger, error_logger)
    await service.initialize() # IMPORTANT: Call this after creating the instance

    # Mark album for future verification (now an async method)
    await service.mark_for_verification("Pink Floyd", "The Dark Side of the Moon")

    # Check if album needs verification now (now an async method)
    if await service.is_verification_needed("Pink Floyd", "The Dark Side of the Moon"):
        # Perform verification
        pass

    # Get all pending albums (now an async method)
    pending_list = await service.get_all_pending_albums()

    # Get verified album keys (now an async method)
    verified_keys = await service.get_verified_album_keys()

"""

import asyncio
import csv
import hashlib
import logging
import os
import sys

from datetime import datetime, timedelta
from typing import Any, Protocol

from utils.logger import get_full_log_path

class Logger(Protocol):
    """Protocol defining the interface for loggers used in the application.

    This Protocol specifies the required methods that any logger implementation
    must provide to be compatible with the PendingVerificationService.
    Implementations should handle different log levels appropriately.
    """

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an informational message."""
        ...

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a warning message."""
        ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log an error message."""
        ...

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """Log a debug message."""
        ...

class PendingVerificationService:
    """Service to track albums needing future verification of their release year.

    Uses hash-based keys for album data. File operations are asynchronous.
    Initializes asynchronously.

    Attributes:
        pending_file_path (str): Path to the CSV file storing pending verifications
        console_logger: Logger for console output
    def __init__(self, config: dict[str, Any], console_logger: Logger, error_logger: Logger):
        verification_interval_days (int): Days to wait before re-checking an album
        pending_albums: Cache of pending albums using hash keys.
        _lock: asyncio.Lock for synchronizing access to pending_albums cache

    """

    def __init__(self, config: dict[str, Any], console_logger: logging.Logger, error_logger: logging.Logger):
        """Initialize the PendingVerificationService.

        Does NOT perform file loading here. Use the async initialize method.
        Does NOT perform file loading here. Use the async initialize method.

        Args:
            config: Application configuration dictionary
            console_logger: Logger for console output
            error_logger: Logger for error logging.

        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger

        # Get verification interval from config or use default (30 days)
        # Ensure the config path exists before accessing
        year_retrieval_config = config.get("year_retrieval", {})
        processing_config = year_retrieval_config.get(
            "processing", {}
        )  # Get processing subsection
        self.verification_interval_days = processing_config.get(
            "pending_verification_interval_days", 30
        )  # Get from processing

        # Set up the pending file path using the utility function
        self.pending_file_path = get_full_log_path(
            config,
            "pending_verification_file",
            "csv/pending_year_verification.csv",
            error_logger,
        )
        # Initialize in-memory cache of pending albums - it will be populated in async initialize
        # key: hash of "artist|album", value: (timestamp, artist, album)
        self.pending_albums: dict[str, tuple[datetime, str, str]] = {}

        # Initialize an asyncio Lock for thread-safe access to the in-memory cache
        self._lock = asyncio.Lock()
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Asynchronously initializes the PendingVerificationService by loading data from disk.

        This method must be called after instantiation.
        """
        self.console_logger.info(
            "Initializing PendingVerificationService asynchronously..."
        )
        await self._load_pending_albums()
        self.console_logger.info(
            "PendingVerificationService asynchronous initialization complete."
        )

    def _generate_album_key(self, artist: str, album: str) -> str:
        """Generate a unique hash key for an album based on artist and album names.

        Uses SHA256 to avoid issues with separator characters in names.
        (Duplicated from CacheService for independence, could be moved to a shared utility).

        :param artist: The artist name.
        :param album: The album name.
        :return: A SHA256 hash string.
        """
        # Combine artist and album names (normalized) and hash them
        normalized_names = f"{artist.strip().lower()}|{album.strip().lower()}"
        return hashlib.sha256(normalized_names.encode("utf-8")).hexdigest()

    async def _load_pending_albums(self) -> None:
        """Load the list of pending albums from the CSV file into memory asynchronously.

        Uses loop.run_in_executor for blocking file operations.
        Reads artist, album, and timestamp from CSV and stores using hash keys.
        """
        # Use the asyncio loop to run blocking file I/O in a thread pool
        loop = asyncio.get_event_loop()

        # Define the blocking file reading operation
        def blocking_load() -> dict[str, tuple[datetime, str, str]]:
            pending_data: dict[str, tuple[datetime, str, str]] = {}
            try:
                # Ensure directory exists before trying to open the file
                os.makedirs(os.path.dirname(self.pending_file_path), exist_ok=True)

                if not os.path.exists(self.pending_file_path):
                    self.console_logger.info(
                        f"Pending verification file not found, will create at: {self.pending_file_path}"
                    )
                    return pending_data  # Return empty if file doesn't exist

                # Use print/basic logging in executor thread as main logger might not be safe
                print(
                    f"DEBUG: Reading pending verification file: {self.pending_file_path}",
                    file=sys.stderr,
                )

                with open(self.pending_file_path, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    # Ensure expected headers are present, fallback if not
                    fieldnames = reader.fieldnames if reader.fieldnames else []
                    if (
                        "artist" not in fieldnames
                        or "album" not in fieldnames
                        or "timestamp" not in fieldnames
                    ):
                        # Use print as error_logger might not be safe in this context
                        print(
                            f"WARNING: Pending verification CSV header missing expected fields in "
                            f"{self.pending_file_path}. Found: {fieldnames}. Skipping load.",
                            file=sys.stderr,
                        )
                        return pending_data  # Return empty on missing headers

                    albums_data = list(reader)  # Read all lines at once

                # Process data after closing the file
                for row in albums_data:
                    artist = row.get("artist", "").strip()
                    album = row.get("album", "").strip()
                    timestamp_str = row.get("timestamp", "").strip()

                    if artist and album and timestamp_str:
                        try:
                            # Use a more flexible date parsing to handle potential inconsistencies
                            # Try primary format, then fallback if needed
                            try:
                                timestamp = datetime.strptime(
                                    timestamp_str, "%Y-%m-%d %H:%M:%S"
                                )
                            except ValueError:
                                # Fallback to date only if necessary
                                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d")
                                self.error_logger.warning(
                                    f"Pending file timestamp had date-only format for '{artist} - {album}': {timestamp_str}. Parsed as date only."
                                )

                            key_hash = self._generate_album_key(artist, album)
                            pending_data[key_hash] = (timestamp, artist, album)
                        except ValueError:
                            # Use print as error_logger might not be safe in this context
                            print(
                                f"WARNING: Invalid timestamp format in pending file for '{artist} - {album}': {timestamp_str}",
                                file=sys.stderr,
                            )
                        except Exception as row_e:
                            # Use print for other potential errors during row processing
                            print(
                                f"WARNING: Error processing row in pending file for '{artist} - {album}': {row_e}",
                                file=sys.stderr,
                            )
                    else:
                        # Use print for malformed row warnings
                        print(
                            f"WARNING: Skipping malformed row in pending file: {row}",
                            file=sys.stderr,
                        )

                return pending_data

            except (FileNotFoundError, OSError) as e:
                # Use print for errors during file reading
                print(
                    f"ERROR reading pending verification file {self.pending_file_path}: {e}",
                    file=sys.stderr,
                )
                return {}  # Return empty data on error
            except (
                Exception
            ) as e:  # Catch other potential errors during blocking operation
                print(
                    f"UNEXPECTED ERROR during blocking load of pending albums from {self.pending_file_path}: {e}",
                    file=sys.stderr,
                )
                return {}  # Return empty data on unexpected error

        # Run the blocking load operation in the default executor
        # Acquire lock before updating the in-memory cache AFTER the blocking read is done
        async with self._lock:
            self.pending_albums = await loop.run_in_executor(None, blocking_load)

        # Log success/failure at console logger level after the executor task is complete
        if self.pending_albums:
            self.console_logger.info(
                f"Loaded {len(self.pending_albums)} pending albums for verification from {self.pending_file_path}"
            )
        else:
            self.console_logger.info(
                f"No pending albums loaded from {self.pending_file_path} (file not found or empty/corrupt)."
            )

    async def _save_pending_albums(self) -> None:
        """Save the current list of pending albums to the CSV file asynchronously.

        Uses loop.run_in_executor for blocking file operations.
        Writes artist, album, and timestamp columns.
        """
        # Use the asyncio loop to run blocking file I/O in a thread pool
        loop = asyncio.get_event_loop()

        # Define the blocking file writing operation
        def blocking_save() -> None:
            # First, write to a temporary file
            temp_file = f"{self.pending_file_path}.tmp"

            try:
                # Create directories if they do not exist
                os.makedirs(os.path.dirname(self.pending_file_path), exist_ok=True)

                with open(temp_file, "w", newline="", encoding="utf-8") as f:
                    # Define fieldnames for the CSV file
                    fieldnames = ["artist", "album", "timestamp"]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    # Acquire lock is done outside the blocking function now
                    # Iterate through values in the in-memory cache (which are (timestamp, artist, album) tuples)
                    # Create a list copy to iterate safely in a separate thread
                    pending_items_to_save = list(self.pending_albums.values())

                    for timestamp, artist, album in pending_items_to_save:
                        # Write artist, album, and timestamp
                        writer.writerow(
                            {
                                "artist": artist,
                                "album": album,
                                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            }
                        )

                # Rename the temporary file (atomic operation)
                os.replace(temp_file, self.pending_file_path)

                # self.console_logger.info(f"Saved {len(pending_items_to_save)} pending albums for verification") # Log inside blocking is not ideal

            except Exception as e:
                # Log error and clean up temp file
                print(
                    f"ERROR during blocking save of pending verification file to {self.pending_file_path}: {e}",
                    file=sys.stderr,
                )
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except OSError as cleanup_e:
                        print(
                            f"WARNING: Could not remove temporary pending file {temp_file}: {cleanup_e}",
                            file=sys.stderr,
                        )
                # Re-raise the exception so run_in_executor propagates it
                raise

        # Acquire lock before accessing the in-memory cache for saving
        async with self._lock:
            # Run the blocking save operation in the default executor
            try:
                await loop.run_in_executor(None, blocking_save)
                self.console_logger.info(
                    f"Saved {len(self.pending_albums)} pending albums for verification to {self.pending_file_path}"
                )  # Log after successful save
            except Exception as e:
                # The exception from blocking_save is caught here
                self.error_logger.error(f"Error saving pending verification file: {e}")

    # Mark album for future verification (now an async method because it calls async _save_pending_albums)
    async def mark_for_verification(self, artist: str, album: str) -> None:
        """Mark an album for future verification.

        Uses a hash key for storage. Saves asynchronously.

        Args:
            artist: Artist name
            album: Album name

        Example:
            >>> await service.mark_for_verification("Pink Floyd", "The Dark Side of the Moon")

        """
        # Acquire lock before modifying the in-memory cache
        async with self._lock:
            # Generate the hash key for the album
            key_hash = self._generate_album_key(artist, album)
            # Store the current timestamp, original artist, and original album in the value
            # Use datetime.now() directly, no need for strftime yet
            self.pending_albums[key_hash] = (
                datetime.now(),
                artist.strip(),
                album.strip(),
            )

        self.console_logger.info(
            f"Marked '{artist} - {album}' for verification in {self.verification_interval_days} days"
        )
        # Save asynchronously after modifying the cache
        await self._save_pending_albums()

    # is_verification_needed is now an async method because it acquires the lock
    async def is_verification_needed(self, artist: str, album: str) -> bool:
        """Check if an album needs verification now.

        Uses the hash key for lookup. Reads from in-memory cache (async with lock).

        Args:
            artist: Artist name
            album: Album name

        Returns:
            True if verification period has elapsed, False otherwise

        Example:
            >>> if await service.is_verification_needed("Pink Floyd", "The Dark Side of the Moon"):
            >>>     # Perform verification

        """
        # Acquire lock before reading from the in-memory cache
        async with self._lock:
            # Generate the hash key for the album
            key_hash = self._generate_album_key(artist, album)

            # Check if the hash key exists in the in-memory cache
            if key_hash not in self.pending_albums:
                return False

            # The value is a tuple (timestamp, artist, album)
            timestamp, stored_artist, stored_album = self.pending_albums[key_hash]
            verification_time = timestamp + timedelta(
                days=self.verification_interval_days
            )

            if datetime.now() >= verification_time:
                # Verification period has elapsed
                # Retrieve original artist/album from the stored tuple for logging
                self.console_logger.info(
                    f"Verification period elapsed for '{stored_artist} - {stored_album}'"
                )
                return True

            # Log when verification is NOT needed, for debugging
            # diff = verification_time - datetime.now()
            # days_remaining = diff.total_seconds() / 86400
            # self.console_logger.debug(
            #     f"Verification not needed yet for '{stored_artist} - {stored_album}'. "
            #     f"Next check in {days_remaining:.1f} days."
            # )

            return False

    # remove_from_pending is now an async method because it calls async _save_pending_albums
    async def remove_from_pending(self, artist: str, album: str) -> None:
        """Remove an album from the pending verification list.

        Uses the hash key for removal. Saves asynchronously.

        Args:
            artist: Artist name
            album: Album name

        Example:
            >>> await service.remove_from_pending("Pink Floyd", "The Dark Side of the Moon")

        """
        # Acquire lock before modifying the in-memory cache
        async with self._lock:
            # Generate the hash key for the album
            key_hash = self._generate_album_key(artist, album)

            # Remove from in-memory cache if the hash key exists
            if key_hash in self.pending_albums:
                # Retrieve original artist/album from the stored tuple for logging
                _, stored_artist, stored_album = self.pending_albums[key_hash]
                del self.pending_albums[key_hash]
                self.console_logger.info(
                    f"Removed '{stored_artist} - {stored_album}' from pending verification"
                )
            # No need to save if the item wasn't found
            else:
                self.console_logger.debug(
                    f"Attempted to remove '{artist} - {album}' from pending verification, but it was not found."
                )
                return  # Exit without saving if no removal occurred

        # Save asynchronously after modifying the cache
        await self._save_pending_albums()

    # get_all_pending_albums is now an async method because it needs to acquire the lock
    # to safely access the in-memory cache.
    async def get_all_pending_albums(
        self,
    ) -> list[
        tuple[datetime, str, str]
    ]:  # Correct return type includes timestamp first
        """Get a list of all pending albums with their verification timestamps.

        Retrieves timestamp, artist, and album from the stored tuples.
        Accesses the in-memory cache asynchronously with a lock.

        Returns:
            List of tuples containing (timestamp, artist, album)

        Example:
            >>> pending_list = await service.get_all_pending_albums()
            >>> for timestamp, artist, album in pending_list: # Correct order
            >>>     print(f"{artist} - {album}: {timestamp}")

        """
        result = []
        # Acquire lock before accessing the in-memory cache
        async with self._lock:
            # Iterate through values in the in-memory cache (which are (timestamp, artist, album) tuples)
            for timestamp, artist, album in self.pending_albums.values():
                # Return in the order (timestamp, artist, album) as stored
                result.append((timestamp, artist, album))
        return result

    # get_verified_album_keys is now an async method because it needs to acquire the lock
    # to safely access the in-memory cache.
    async def get_verified_album_keys(self) -> set[str]:
        """Get the set of album hash keys that need verification now.

        Checks the timestamp in the stored tuple.
        Accesses the in-memory cache asynchronously with a lock.

        Returns:
            Set of album hash keys needing verification

        Example:
            >>> for key_hash in await service.get_verified_album_keys():
            >>>     # You would need to retrieve artist/album names separately if needed
            >>>     # For example, by looking up the hash in the cache or another source
            >>>     pass # Perform verification based on hash key

        """
        now = datetime.now()
        verified_keys = set()

        # Acquire lock before accessing the in-memory cache
        async with self._lock:
            # Iterate through items (key_hash, value_tuple) in the in-memory cache
            # Iterate over a list copy to allow potential future modifications during iteration if needed,
            # though in this specific method it's just reading.
            for key_hash, value_tuple in list(self.pending_albums.items()):
                # The value is a tuple (timestamp, artist, album)
                timestamp, stored_artist, stored_album = value_tuple
                verification_time = timestamp + timedelta(
                    days=self.verification_interval_days
                )
                if now >= verification_time:
                    self.console_logger.info(
                        f"Album '{stored_artist} - {stored_album}' needs verification"
                    )
                    verified_keys.add(key_hash)
                # else: # Log when not needed, for debugging
                # diff = verification_time - now
                # days_remaining = diff.total_seconds() / 86400
                # self.console_logger.debug(
                #     f"Album '{stored_artist} - {stored_album}' does not need verification yet. "
                #     f"Next check in {days_remaining:.1f} days."
                # )

        return verified_keys
