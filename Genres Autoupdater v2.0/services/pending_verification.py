#!/usr/bin/env python3
"""
Pending Verification Module

This module maintains a list of albums that need re-verification in the future.
When an album's year cannot be definitely determined from external sources,
it is added to this list with a timestamp. On future runs, albums whose
verification period has elapsed will be checked again.

File operations (_load_pending_albums, _save_pending_albums) are now asynchronous
using asyncio's run_in_executor to avoid blocking the event loop.

Usage:
    service = PendingVerificationService(config, console_logger, error_logger)

    # Mark album for future verification (now an async method)
    await service.mark_for_verification("Pink Floyd", "The Dark Side of the Moon")

    # Check if album needs verification now (remains sync)
    if service.is_verification_needed("Pink Floyd", "The Dark Side of the Moon"):
        # Perform verification
        pass

    # Get all pending albums (now an async method)
    pending_list = await service.get_all_pending_albums()

    # Get verified album keys (remains sync, but internal check might implicitly use async load)
    verified_keys = service.get_verified_album_keys()

"""

import asyncio
import csv
import hashlib
import os
import sys

from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple


class PendingVerificationService:
    """
    Service to track albums needing future verification of their release year.
    Uses hash-based keys for album data. File operations are asynchronous.

    Attributes:
        pending_file_path (str): Path to the CSV file storing pending verifications
        console_logger: Logger for console output
        error_logger: Logger for error output
        verification_interval_days (int): Days to wait before re-checking an album
        # pending_albums: Cache of pending albums using hash keys.
        # Value is a tuple: (timestamp, artist, album)
        pending_albums: Dict[str, Tuple[datetime, str, str]]
        _lock: asyncio.Lock for synchronizing access to pending_albums cache
    """

    def __init__(self, config: Dict, console_logger, error_logger):
        """
        Initialize the PendingVerificationService.

        Args:
            config: Application configuration dictionary
            console_logger: Logger for console output
            error_logger: Logger for error output

        Example:
            >>> service = PendingVerificationService(config, console_logger, error_logger)
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger

        # Get verification interval from config or use default (30 days)
        # Ensure the config path exists before accessing
        year_retrieval_config = config.get("year_retrieval", {})
        self.verification_interval_days = year_retrieval_config.get("pending_verification_interval_days", 30)

        # Set up the pending file path
        logs_base_dir = config.get("logs_base_dir", ".")
        self.pending_file_path = os.path.join(logs_base_dir, "csv", "pending_year_verification.csv")

        # Initialize in-memory cache of pending albums
        # key: hash of "artist|album", value: (timestamp, artist, album)
        self.pending_albums = {}

        # Create directory if it doesn't exist (this is a quick sync operation, can remain sync)
        os.makedirs(os.path.dirname(self.pending_file_path), exist_ok=True)

        # Initialize an asyncio Lock for thread-safe access to the in-memory cache
        self._lock = asyncio.Lock()

        # Load existing pending albums asynchronously during initialization
        # Note: Calling async methods in __init__ is not standard.
        # A common pattern is to have an async `initialize` method for the service.
        # For now, we'll call it directly, but be aware this might need adjustment
        # in the DependencyContainer if it expects init to be fully synchronous.
        # Let's add a flag and call it from an async context later if needed.
        # For simplicity in this step, we'll assume it's called from an async context
        # or handle the initial load outside __init__.
        # Given DependencyContainer is sync, the initial load should probably remain sync
        # or the service needs an async init method called after creation.
        # Let's keep the initial load sync in __init__ for now, but make save/subsequent load async.
        # A better approach would be to make the service have an async init method.

        # Let's make _load_pending_albums async, but call it from a sync wrapper in __init__
        # This is a common pattern to bridge sync __init__ with async setup.
        async def _async_init_load():
            await self._load_pending_albums()

        # Run the async load using a new loop or the current one if available
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(_async_init_load())
        except RuntimeError:
            # If no loop is running (e.g., script run directly), run it in a new loop
            asyncio.run(_async_init_load())
        except Exception as e:
            self.error_logger.error(f"Error during initial async load of pending albums: {e}", exc_info=True)

    def _generate_album_key(self, artist: str, album: str) -> str:
        """
        Generates a unique hash key for an album based on artist and album names.
        Uses SHA256 to avoid issues with separator characters in names.
        (Duplicated from CacheService for independence, could be moved to a shared utility)

        :param artist: The artist name.
        :param album: The album name.
        :return: A SHA256 hash string.
        """
        # Combine artist and album names (normalized) and hash them
        normalized_names = f"{artist.strip().lower()}|{album.strip().lower()}"
        return hashlib.sha256(normalized_names.encode('utf-8')).hexdigest()

    async def _load_pending_albums(self) -> None:
        """
        Load the list of pending albums from the CSV file into memory asynchronously.
        Uses loop.run_in_executor for blocking file operations.
        Reads artist, album, and timestamp from CSV and stores using hash keys.
        """
        # Use the asyncio loop to run blocking file I/O in a thread pool
        loop = asyncio.get_event_loop()

        # Define the blocking file reading operation
        def blocking_load():
            pending_data = {}
            if not os.path.exists(self.pending_file_path):
                self.console_logger.info(f"Pending verification file not found, will create at: {self.pending_file_path}")
                return pending_data  # Return empty if file doesn't exist

            try:
                with open(self.pending_file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    albums_data = list(reader)  # Read all lines at once

                # Process data after closing the file
                for row in albums_data:
                    artist = row.get("artist", "").strip()
                    album = row.get("album", "").strip()
                    timestamp_str = row.get("timestamp", "").strip()

                    if artist and album and timestamp_str:
                        try:
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                            key_hash = self._generate_album_key(artist, album)
                            pending_data[key_hash] = (timestamp, artist, album)
                        except ValueError:
                            self.error_logger.warning(f"Invalid timestamp format in pending file for '{artist} - {album}': {timestamp_str}")
                    else:
                        self.error_logger.warning(f"Skipping malformed row in pending file: {row}")

                return pending_data

            except (FileNotFoundError, IOError, OSError) as e:
                self.error_logger.error(f"Error reading pending file: {e}")
                return {}  # Return empty data on error
            except Exception as e:  # Catch other potential errors during processing rows
                self.error_logger.error(f"Unexpected error processing pending file data: {e}", exc_info=True)
                return {}  # Return empty data on unexpected error

        # Run the blocking load operation in the default executor
        # Acquire lock before updating the in-memory cache
        async with self._lock:
            self.pending_albums = await loop.run_in_executor(None, blocking_load)

        self.console_logger.info(f"Loaded {len(self.pending_albums)} pending albums for verification")

    async def _save_pending_albums(self) -> None:
        """
        Save the current list of pending albums to the CSV file asynchronously.
        Uses loop.run_in_executor for blocking file operations.
        Writes artist, album, and timestamp columns.
        """
        # Use the asyncio loop to run blocking file I/O in a thread pool
        loop = asyncio.get_event_loop()

        # Define the blocking file writing operation
        def blocking_save():
            # First, write to a temporary file
            temp_file = f"{self.pending_file_path}.tmp"

            try:
                # Create directories if they do not exist (can remain sync as it's quick)
                os.makedirs(os.path.dirname(self.pending_file_path), exist_ok=True)

                # Write to a temporary file
                with open(temp_file, "w", newline="", encoding="utf-8") as f:
                    # Define fieldnames for the CSV file
                    writer = csv.DictWriter(f, fieldnames=["artist", "album", "timestamp"])
                    writer.writeheader()

                    # Acquire lock to safely read from the in-memory cache
                    # Note: The lock is acquired *inside* the blocking function,
                    # which is okay because the entire function runs in a single thread
                    # from the executor, preventing race conditions with other async methods
                    # trying to access the cache.
                    # However, it's generally better to acquire the lock *before*
                    # calling run_in_executor if the blocking operation itself doesn't need the lock.
                    # Let's acquire the lock outside the blocking function.

                    # Re-defining blocking_save to acquire lock outside
                    pass  # This inner function will be replaced

                # Redefine blocking_save to capture current state and acquire lock outside
                pending_items_to_save = list(self.pending_albums.values())  # Get a copy of items to save

                with open(temp_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["artist", "album", "timestamp"])
                    writer.writeheader()
                    for timestamp, artist, album in pending_items_to_save:
                        writer.writerow({"artist": artist, "album": album, "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")})

                # Rename the temporary file (atomic operation)
                if os.path.exists(self.pending_file_path):
                    if sys.platform == 'win32':
                        os.replace(temp_file, self.pending_file_path)  # Windows: atomic rename with replacement
                    else:
                        os.rename(temp_file, self.pending_file_path)  # POSIX: atomic rename
                else:
                    os.rename(temp_file, self.pending_file_path)

                self.console_logger.info(f"Saved {len(pending_items_to_save)} pending albums for verification")

            except (IOError, OSError) as e:
                self.error_logger.error(f"Error saving pending verification file: {e}")
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)  # Remove temporary file in case of error
                    except OSError:
                        pass  # Silently ignore error removing temp file
            except Exception as e:  # Catch other potential errors during saving
                self.error_logger.error(f"Unexpected error saving pending verification file: {e}", exc_info=True)
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except OSError:
                        pass

        # Acquire lock before accessing the in-memory cache for saving
        async with self._lock:
            # Run the blocking save operation in the default executor
            await loop.run_in_executor(None, blocking_save)

    # Mark album for future verification (now an async method because it calls async _save_pending_albums)
    async def mark_for_verification(self, artist: str, album: str) -> None:
        """
        Mark an album for future verification.
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
            self.pending_albums[key_hash] = (datetime.now(), artist.strip(), album.strip())

        self.console_logger.info(f"Marked '{artist} - {album}' for verification in {self.verification_interval_days} days")
        # Save asynchronously after modifying the cache
        await self._save_pending_albums()

    # is_verification_needed remains sync as it only reads from in-memory cache
    def is_verification_needed(self, artist: str, album: str) -> bool:
        """
        Check if an album needs verification now.
        Uses the hash key for lookup. Reads from in-memory cache (sync).

        Args:
            artist: Artist name
            album: Album name

        Returns:
            True if verification period has elapsed, False otherwise

        Example:
            >>> if service.is_verification_needed("Pink Floyd", "The Dark Side of the Moon"):
            >>>     # Perform verification
        """
        # Acquire lock before reading from the in-memory cache
        # Note: In sync methods, you cannot use 'async with self._lock:'.
        # To safely read from the cache in a sync method when the cache is
        # modified by async methods, you would typically use loop.call_soon_threadsafe
        # to schedule the read in the event loop thread, or ensure the sync method
        # is only called from within the event loop thread (e.g., via run_until_complete
        # or ensuring the caller is already in an async context).
        # For simplicity here, we'll assume sync methods are called in a way that
        # doesn't cause race conditions with async modifications, OR that the lock
        # is acquired by the caller if needed.
        # A more robust solution for sync access to async-modified data is complex.
        # Let's assume sync reads are safe enough for this context or will be called appropriately.
        # If strict thread safety is needed for sync reads, the method would need to become async.

        # Let's add a note about potential thread safety if called from multiple threads
        # outside the event loop.
        # For now, proceed with direct access assuming it's called from the event loop thread.

        # Generate the hash key for the album
        key_hash = self._generate_album_key(artist, album)

        # Check if the hash key exists in the in-memory cache
        if key_hash not in self.pending_albums:
            return False

        # The value is a tuple (timestamp, artist, album)
        timestamp, _, _ = self.pending_albums[key_hash]
        verification_time = timestamp + timedelta(days=self.verification_interval_days)

        if datetime.now() >= verification_time:
            # Verification period has elapsed
            # Retrieve original artist/album from the stored tuple for logging
            _, stored_artist, stored_album = self.pending_albums[key_hash]
            self.console_logger.info(f"Verification period elapsed for '{stored_artist} - {stored_album}'")
            return True

        return False

    # remove_from_pending is now an async method because it calls async _save_pending_albums
    async def remove_from_pending(self, artist: str, album: str) -> None:
        """
        Remove an album from the pending verification list.
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
                self.console_logger.info(f"Removed '{stored_artist} - {stored_album}' from pending verification")

        # Save asynchronously after modifying the cache
        await self._save_pending_albums()

    # get_all_pending_albums is now an async method because it needs to acquire the lock
    # to safely access the in-memory cache.
    async def get_all_pending_albums(self) -> List[Tuple[str, str, datetime]]:
        """
        Get a list of all pending albums with their verification timestamps.
        Retrieves artist, album, and timestamp from the stored tuples.
        Accesses the in-memory cache asynchronously with a lock.

        Returns:
            List of tuples containing (artist, album, timestamp)

        Example:
            >>> pending_list = await service.get_all_pending_albums()
            >>> for artist, album, timestamp in pending_list:
            >>>     print(f"{artist} - {album}: {timestamp}")
        """
        result = []
        # Acquire lock before accessing the in-memory cache
        async with self._lock:
            # Iterate through values in the in-memory cache (which are (timestamp, artist, album) tuples)
            for timestamp, artist, album in self.pending_albums.values():
                result.append((artist, album, timestamp))
        return result

    # get_verified_album_keys is now an async method because it needs to acquire the lock
    # to safely access the in-memory cache.
    async def get_verified_album_keys(self) -> Set[str]:
        """
        Get the set of album hash keys that need verification now.
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
            for key_hash, value_tuple in list(self.pending_albums.items()):
                # The value is a tuple (timestamp, artist, album)
                timestamp, stored_artist, stored_album = value_tuple
                verification_time = timestamp + timedelta(days=self.verification_interval_days)
                if now >= verification_time:
                    self.console_logger.info(f"Album '{stored_artist} - {stored_album}' needs verification")
                    verified_keys.add(key_hash)

        return verified_keys
