#!/usr/bin/env python3

"""Cache Service Module.

This module provides two caching mechanisms:
    1. An in-memory (dict) cache for generic data with TTL support (via get_async, set_async, etc.)
    2. A persistent CSV-based cache for album release years that persists data across application runs.

Features:
    - In-memory cache: supports TTL, async operations, key hashing, and automatic value computation
    - Special handling for key="ALL" to return all valid cached track objects
    - CSV cache: stores album year information persistently with artist/album lookup
    - Both caches support invalidation (individual entries or complete cache)
    - Uses SHA256 hash for album keys to avoid separator issues
    - Automatic cache synchronization to disk based on time interval
    - Atomic file writes for data safety
    - Tracking of last incremental run timestamp

Refactored: Initial asynchronous loading and saving handled in separate async methods,
called by DependencyContainer after service instantiation.
"""

import asyncio
import csv
import hashlib
import json
import os
import sys
import time

from collections.abc import Callable
from datetime import datetime
from typing import Any

# Assuming get_full_log_path is a utility function that handles path joining and dir creation
from utils.logger import get_full_log_path

class CacheService:
    """Cache Service Class.

    This class provides an in-memory cache with TTL support and a persistent CSV-based cache
    for album release years. Uses hash-based keys for album data.
    Initializes asynchronously.
    """

    CACHE_ENTRY_LENGTH = 2

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: Any,
        error_logger: Any,
    ):
        """Initialize the CacheService with configuration and loggers.

        Does NOT perform file loading here. Use the async initialize method.

        :param config: Configuration dictionary loaded from my-config.yaml.
        :param console_logger: Logger object for console output.
        :param error_logger: Logger object for error logging.
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger

        # In-memory cache settings
        self.default_ttl = config.get("cache_ttl_seconds", 900)
        self.cache: dict[str, tuple[Any, float]] = (
            {}
        )  # key (hash) -> (value, expiry_time)

        # CSV cache settings
        # logs_base_dir is retrieved via get_full_log_path
        self.album_cache_csv = get_full_log_path(
            config, "album_cache_csv", "csv/cache_albums.csv", error_logger
        )  # Use utility function

        # Persistent cache file for general API responses
        self.cache_file = get_full_log_path(
            config, "api_cache_file", "cache.json", error_logger
        )

        # In-memory album years cache
        # key: hash of "artist|album", value: (year, artist, album)
        self.album_years_cache: dict[str, tuple[str, str, str]] = {}
        self.last_cache_sync = time.time()
        self.cache_dirty = False  # Flag indicating unsaved changes
        self.sync_interval = config.get(
            "album_cache_sync_interval", 300
        )  # Sync every 5 minutes by default

        # Initial album cache loading is now done in the async initialize method
        # self._load_album_years_cache() # REMOVED from __init__

    async def initialize(self) -> None:
        """Asynchronously initializes the CacheService by loading data from disk.

        This method must be called after instantiation.
        """
        self.console_logger.info("Initializing CacheService asynchronously...")
        await self.load_cache()
        await self._load_album_years_cache()
        self.console_logger.info(
            "CacheService asynchronous initialization complete."
        )

    def _generate_album_key(self, artist: str, album: str) -> str:
        """Generate a unique hash key for an album based on artist and album names.

        Uses SHA256 to avoid issues with separator characters in names.

        :param artist: The artist name.
        :param album: The album name.
        :return: A SHA256 hash string.
        """
        # Combine artist and album names (normalized) and hash them
        normalized_names = f"{artist.strip().lower()}|{album.strip().lower()}"
        return hashlib.sha256(normalized_names.encode("utf-8")).hexdigest()

    def _is_expired(self, expiry_time: float) -> bool:
        """Check if the cache entry has expired based on the expiry time.

        :param expiry_time: The expiry time in seconds since epoch.
        :return: True if the entry has expired, False otherwise.
        """
        return time.time() > expiry_time

    def _hash_key(self, key_data: Any) -> str:
        """Generate a SHA256 hash for a general cache key.

        Used for the generic in-memory cache.

        :param key_data: The key data to hash (can be a tuple or any hashable object).
        :return: The hashed key string.
        """
        # Ensure key_data is hashable or convert it to a consistent string representation
        try:
            key_str = str(key_data)
        except Exception as e:
            self.error_logger.error(
                f"Failed to convert key_data to string for hashing: {e}"
            )
            # Fallback to a default or raise an error, depending on desired behavior
            key_str = "unhashable_key"  # Or raise TypeError("Unhashable key data")

        return hashlib.sha256(key_str.encode("utf-8")).hexdigest()

    def set(self, key_data: Any, value: Any, ttl: int | None = None) -> None:
        """Set a value in the in-memory cache with an optional TTL."""
        key = self._hash_key(key_data)
        ttl_value = ttl if ttl is not None else self.default_ttl
        expiry_time = time.time() + ttl_value
        self.cache[key] = (value, expiry_time)

    async def set_async(
        self, key_data: Any, value: Any, ttl: int | None = None
    ) -> None:
        """Asynchronously set a value in the in-memory cache with an optional TTL.

        Currently just calls sync set, but kept async signature for consistency/future needs.

        :param key_data: The key data to hash (can be a tuple or any hashable object).
        :param value: The value to store in the cache.
        :param ttl: The time-to-live in seconds for the cache entry.
        """
        # In-memory dictionary operations are synchronous.
        # If cache involved blocking I/O (like a database), this would need executor.
        self.set(key_data, value, ttl)

    async def get_async(
        self,
        key_data: Any,
        compute_func: Callable[[], "asyncio.Future[Any]"] | None = None,
    ) -> Any:
        """Asynchronously fetch a value from the in-memory cache or compute it if needed.

        - If `key_data == "ALL"`, returns a list of *valid* cached track objects.
        - Otherwise uses a hashed key for the dictionary-based lookup.
        - If value not found or expired, and compute_func provided, it calls compute_func().
        """
        # Special case if key_data == "ALL": return a list of track dicts
        if key_data == "ALL":
            tracks = []
            now_ts = time.time()
            # Iterate through values, which are (value, expiry_time) tuples
            for val, expiry in list(
                self.cache.values()
            ):  # Iterate over a copy to allow deletion during iteration
                # Filter out only non-expired track objects (assuming tracks are dicts with 'id')
                if now_ts < expiry and isinstance(val, dict) and "id" in val:
                    tracks.append(val)
                # Optional: remove expired entries here if iterating all is frequent
                # elif now_ts >= expiry and self._hash_key(val.get('id', '')) in self.cache: # Need a stable key to remove
                #      del self.cache[self._hash_key(val.get('id', ''))] # This assumes track id is hashable key data
            return tracks

        # Otherwise standard logic for a specific key
        key = self._hash_key(key_data)
        if key in self.cache:
            value, expiry_time = self.cache[key]
            if not self._is_expired(expiry_time):
                self.console_logger.debug(f"Cache hit for {key_data}")
                return value
            else:
                self.console_logger.debug(f"Cache expired for {key_data}")
                del self.cache[key]  # remove expired entry

        # If not found in cache or expired, compute if compute_func is given
        if compute_func is not None:
            self.console_logger.debug(f"Computing value for {key_data}")
            value = await compute_func()
            if value is not None:
                self.set(key_data, value)
            return value
        return None

    def invalidate(self, key_data: Any) -> None:
        """Invalidate a single cache entry by key or clear all entries if key_data == "ALL".

        This is a synchronous operation on the in-memory cache.

        :param key_data: The key data to invalidate or "ALL" to clear all entries.
        """
        if key_data == "ALL":
            # Clear all generic cache entries
            self.cache.clear()
            self.console_logger.info("Invalidated ALL in-memory generic cache entries.")
            return
        key = self._hash_key(key_data)
        if key in self.cache:
            del self.cache[key]
            self.console_logger.info(
                f"In-memory generic cache invalidated for key: {key_data}"
            )

    def clear(self) -> None:
        """Clear the entire in-memory generic cache.

        This is a synchronous operation.
        """
        self.cache.clear()
        self.console_logger.info("All in-memory generic cache entries cleared.")

    async def load_cache(self) -> None:
        """Load the persistent generic cache from disk asynchronously."""
        loop = asyncio.get_event_loop()

        def blocking_load() -> dict[str, tuple[Any, float]]:
            if not os.path.exists(self.cache_file):
                return {}
            try:
                with open(self.cache_file, encoding="utf-8") as f:
                    data = json.load(f)
                result: dict[str, tuple[Any, float]] = {}
                for k, v in data.items():
                    if (
                        isinstance(v, list)
                        and len(v) == self.CACHE_ENTRY_LENGTH
                        and isinstance(v[1], int | float)
                        and not self._is_expired(float(v[1]))
                    ):
                        result[k] = (v[0], float(v[1]))
                return result
            except Exception as e:
                self.error_logger.error(
                    f"Error loading cache from {self.cache_file}: {e}"
                )
                return {}

        self.cache = await loop.run_in_executor(None, blocking_load)
        self.console_logger.info(
            f"Loaded {len(self.cache)} cached entries from {self.cache_file}"
        )


    async def save_cache(self) -> None:
        """Persist the generic cache to disk asynchronously."""
        loop = asyncio.get_event_loop()

        def blocking_save() -> None:
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                data = {
                    k: [v, exp]
                    for k, (v, exp) in self.cache.items()
                    if not self._is_expired(exp)
                }
                with open(self.cache_file, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except Exception as e:  # pragma: no cover - best effort logging
                print(
                    f"ERROR saving cache to {self.cache_file}: {e}",
                    file=sys.stderr,
                )

        await loop.run_in_executor(None, blocking_save)

    async def _load_album_years_cache(self) -> None:
        """Load album years cache from CSV file into memory asynchronously.

        Uses loop.run_in_executor for blocking file operations.
        Reads artist, album, and year from CSV and stores using hash keys.
        """
        self.album_years_cache.clear()  # Clear existing cache before loading
        loop = asyncio.get_event_loop()

        def blocking_load() -> dict[str, tuple[str, str, str]]:
            album_data: dict[str, tuple[str, str, str]] = {}
            try:
                # Check if the directory exists before trying to open the file
                os.makedirs(os.path.dirname(self.album_cache_csv), exist_ok=True)
                if os.path.exists(self.album_cache_csv):
                    with open(self.album_cache_csv, encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        # Ensure expected headers are present, fallback if not
                        fieldnames = reader.fieldnames if reader.fieldnames else []
                        if (
                            "artist" not in fieldnames
                            or "album" not in fieldnames
                            or "year" not in fieldnames
                        ):
                            self.error_logger.warning(
                                f"Album cache CSV header missing expected fields in {self.album_cache_csv}. Found: {fieldnames}. Skipping load."
                            )
                            return album_data  # Return empty on missing headers

                        for row in reader:
                            # Read artist, album, and year from CSV columns
                            a = row.get("artist", "").strip()
                            b = row.get("album", "").strip()
                            y = row.get("year", "").strip()
                            if a and b and y:
                                # Generate hash key from artist and album
                                key_hash = self._generate_album_key(a, b)
                                # Store year, original artist, and original album in the in-memory cache value
                                album_data[key_hash] = (y, a, b)
                            else:
                                self.error_logger.warning(
                                    f"Skipping malformed row in album cache file: {row}"
                                )

                else:
                    self.console_logger.info(
                        f"Album-year cache file not found, will create at: {self.album_cache_csv}"
                    )

                return album_data

            except Exception as e:
                # Log error and return empty data on failure
                self.error_logger.error(
                    f"Error loading album years cache from {self.album_cache_csv}: {e}",
                    exc_info=True,
                )
                return {}

        # Run the blocking load operation in the default executor
        self.album_years_cache = await loop.run_in_executor(None, blocking_load)
        self.console_logger.info(
            f"Loaded {len(self.album_years_cache)} album years into memory cache from {self.album_cache_csv}"
        )

    async def _sync_cache_if_needed(self, force: bool = False) -> None:
        """Synchronize cache to disk if needed based on time interval or force flag.

        :param force: If True, force synchronization regardless of time interval
        """
        now = time.time()
        if force or (
            self.cache_dirty and now - self.last_cache_sync >= self.sync_interval
        ):
            self.console_logger.info(
                f"Syncing album years cache to disk (force={force}, dirty={self.cache_dirty}, "
                f"interval_elapsed={now - self.last_cache_sync >= self.sync_interval})..."
            )
            await self._save_cache_to_disk()
            self.last_cache_sync = now
            self.cache_dirty = False
        # else:
        # self.console_logger.debug("Cache sync not needed yet.") # Too verbose for debug

    async def _save_cache_to_disk(self) -> None:
        """Save the in-memory album years cache to disk asynchronously.

        Uses atomic write pattern with temporary file and loop.run_in_executor.
        Writes artist, album, and year columns.
        """
        temp_file = f"{self.album_cache_csv}.tmp"
        loop = asyncio.get_event_loop()

        def blocking_save() -> None:
            try:
                # Create directories if they do not exist
                os.makedirs(os.path.dirname(self.album_cache_csv), exist_ok=True)

                with open(temp_file, "w", newline="", encoding="utf-8") as f:
                    # Define fieldnames for the CSV file
                    fieldnames = ["artist", "album", "year"]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    # Iterate through values in the in-memory cache (which are (year, artist, album) tuples)
                    # Create a list copy to iterate safely in a separate thread
                    items_to_save = list(self.album_years_cache.values())

                    for year, artist, album in items_to_save:
                        # Write artist, album, and year as separate columns
                        writer.writerow(
                            {"artist": artist, "album": album, "year": year}
                        )

                # Atomic rename (will replace existing file)
                os.replace(temp_file, self.album_cache_csv)

                # self.console_logger.info(f"Synchronized {len(items_to_save)} album years to disk.") # Log inside blocking is not ideal

            except Exception as e:
                # Log error and clean up temp file
                # Use print as loggers might have issues in this separate thread context
                print(
                    f"ERROR during blocking save of album years cache to {self.album_cache_csv}: {e}",
                    file=sys.stderr,
                )
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except OSError as cleanup_e:
                        print(
                            f"WARNING: Could not remove temporary cache file {temp_file}: {cleanup_e}",
                            file=sys.stderr,
                        )
                # Re-raise the exception so run_in_executor propagates it
                raise

        # Run the blocking save operation in the default executor
        try:
            await loop.run_in_executor(None, blocking_save)
            self.console_logger.info(
                f"Synchronized {len(self.album_years_cache)} album years to disk."
            )  # Log after successful save
        except Exception as e:
            # The exception from blocking_save is caught here
            self.error_logger.error(
                f"Error synchronizing album years cache to disk: {e}"
            )

    async def initialize_album_cache_csv(self) -> None:
        """If the album cache CSV file does not exist, create it with the header row asynchronously.

        Ensures the CSV has 'artist', 'album', 'year' columns.
        Uses loop.run_in_executor for blocking file operations.
        """
        loop = asyncio.get_event_loop()

        def blocking_init() -> None:
            # Check if path exists before creating
            if not os.path.exists(self.album_cache_csv):
                self.console_logger.info(
                    f"Creating album-year CSV cache: {self.album_cache_csv}"
                )
                try:
                    # Ensure directory exists before creating file
                    os.makedirs(os.path.dirname(self.album_cache_csv), exist_ok=True)
                    # Define fieldnames for the CSV file
                    fieldnames = ["artist", "album", "year"]
                    with open(
                        self.album_cache_csv, "w", newline="", encoding="utf-8"
                    ) as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                except Exception as e:
                    print(
                        f"ERROR during blocking init of album cache CSV {self.album_cache_csv}: {e}",
                        file=sys.stderr,
                    )
                    raise  # Re-raise to propagate

        # Run the blocking initialization operation
        try:
            await loop.run_in_executor(None, blocking_init)
        except Exception as e:
            self.error_logger.error(f"Error initializing album cache CSV: {e}")

    async def get_last_run_timestamp(self) -> datetime:
        """Get the timestamp of the last incremental run asynchronously.

        Uses loop.run_in_executor for blocking file operations.

        Returns:
            datetime: The timestamp of the last incremental run, or datetime.min if not found or error occurs.

        """
        loop = asyncio.get_event_loop()

        def blocking_read_timestamp() -> datetime:
            # Ensure the logging config section and key exist before accessing
            logging_config = self.config.get("logging", {})
            last_file_key = logging_config.get("last_incremental_run_file")

            if not last_file_key:
                # Use print as loggers might not be fully available in this thread context
                print(
                    "WARNING: Config key 'logging.last_incremental_run_file' is missing.",
                    file=sys.stderr,
                )
                return datetime.min  # Return min datetime if config key is missing

            # Use get_full_log_path to get the file path (it ensures directory exists)
            # Pass error_logger, but it might not work reliably in executor thread
            last_file = get_full_log_path(
                self.config,
                "last_incremental_run_file",
                "last_incremental_run.log",
                self.error_logger,
            )

            if not os.path.exists(last_file):
                return datetime.min  # Return min datetime if file does not exist

            try:
                with open(last_file, encoding="utf-8") as f:
                    last_run_str = f.read().strip()
                return datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, OSError) as e:
                # Use print as loggers might not be fully available in this thread context
                print(
                    f"ERROR during blocking read of last run timestamp from {last_file}: {e}",
                    file=sys.stderr,
                )
                return datetime.min

        # Run the blocking read operation in the default executor
        try:
            timestamp = await loop.run_in_executor(None, blocking_read_timestamp)
            # Log success at console/error logger level after getting result
            if timestamp != datetime.min:
                self.console_logger.debug(
                    f"Successfully read last run timestamp: {timestamp}"
                )
            # else: # Logged in blocking_read_timestamp if config key missing or file not found initially
            return timestamp
        except Exception as e:
            # Catch exceptions from the executor
            self.error_logger.error(
                f"Error getting last run timestamp from executor: {e}"
            )
            return datetime.min  # Return min datetime on error

    async def get_album_year_from_cache(self, artist: str, album: str) -> str | None:
        """Get album year from the in-memory cache for a given artist and album.

        Uses the hash key for lookup. Returns the year if found, None otherwise.
        Automatically syncs cache if needed before lookup.

        :param artist: The artist name.
        :param album: The album name.
        :return: The year of the album as a string, or None if not found.
        """
        # Generate the hash key for the album
        key_hash = self._generate_album_key(artist, album)

        # Sync cache if needed (this will save to disk if dirty/interval met)
        # Note: Initial loading is done in async initialize, not here.
        await self._sync_cache_if_needed()

        # Check if the hash key exists in the in-memory cache
        if key_hash in self.album_years_cache:
            # The value is a tuple (year, artist, album)
            year, _, _ = self.album_years_cache[key_hash]
            self.console_logger.debug(
                f"Album year cache hit for '{artist} - {album}': {year}"
            )
            return year
        self.console_logger.debug(f"Album year cache miss for '{artist} - {album}'")
        return None

    async def store_album_year_in_cache(
        self, artist: str, album: str, year: str
    ) -> None:
        """Store or update an album year in the in-memory cache and mark it for disk sync.

        Uses the hash key for storage. The actual disk write happens based on sync interval or forced sync.

        :param artist: The artist name.
        :param album: The album name.
        :param year: The album release year.
        :return: None
        """
        # Generate the hash key for the album
        key_hash = self._generate_album_key(artist, album)

        # Store the year, original artist, and original album in the in-memory cache value
        self.album_years_cache[key_hash] = (year.strip(), artist.strip(), album.strip())
        self.cache_dirty = True

        # Sync with disk if needed
        await self._sync_cache_if_needed()

        self.console_logger.debug(
            f"Album year stored in cache for '{artist} - {album}': {year}"
        )

    def invalidate_album_cache(self, artist: str, album: str) -> None:
        """Invalidate the album-year cache for a given artist and album.

        Removes the entry from the in-memory cache using the hash key.
        Marks the cache as dirty for eventual disk sync.
        This is a synchronous operation on the in-memory cache.

        :param artist: The artist name.
        :param album: The album name.
        :return: None
        """
        # Generate the hash key for the album
        key_hash = self._generate_album_key(artist, album)

        # Remove from in-memory cache if the hash key exists
        if key_hash in self.album_years_cache:
            del self.album_years_cache[key_hash]
            self.cache_dirty = True
            self.console_logger.info(
                f"Invalidated album-year cache for: {artist} - {album}"
            )
        # No need to save immediately here, _sync_cache_if_needed will handle it based on interval/force

    def invalidate_all_albums(self) -> None:
        """Invalidate the entire album-year cache.

        Clears the in-memory cache and marks it for synchronization.
        This is a synchronous operation on the in-memory cache.
        """
        if self.album_years_cache:
            self.album_years_cache.clear()
            self.cache_dirty = True
            self.console_logger.info("Entire album-year cache cleared.")
        # No need to save immediately here, _sync_cache_if_needed will handle it based on interval/force

    async def sync_cache(self) -> None:
        """Force synchronization of the in-memory album cache to disk.

        Useful before application shutdown to ensure data persistence.
        """
        await self._sync_cache_if_needed(force=True)
        await self.save_cache()
