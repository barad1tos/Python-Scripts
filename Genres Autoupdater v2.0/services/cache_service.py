#!/usr/bin/env python3

"""
Cache Service Module

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

Usage:
    - For general data caching: get_async/set_async with optional TTL
    - For album-year caching: get_album_year_from_cache/store_album_year_in_cache
    - Force disk synchronization with sync_cache() before shutdown
"""

import asyncio
import csv
import hashlib
import os
import sys
import time

from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

from utils.logger import get_full_log_path


class CacheService:
    """
    Cache Service Class
    This class provides an in-memory cache with TTL support and a persistent CSV-based cache
    for album release years. Uses hash-based keys for album data.
    """

    def __init__(self, config: dict, console_logger, error_logger):
        """
        Initialize the CacheService with configuration and loggers.

        :param config: Configuration dictionary loaded from my-config.yaml.
        :param console_logger: Logger object for console output.
        :param error_logger: Logger object for error logging.
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger

        # In-memory cache settings
        self.default_ttl = config.get("cache_ttl_seconds", 900)
        self.cache: Dict[str, Tuple[Any, float]] = {}  # key (hash) -> (value, expiry_time)

        # CSV cache settings
        logs_base_dir = self.config.get("logs_base_dir", ".")
        # Save the file in the "csv" folder next to track_list.csv
        self.album_cache_csv = os.path.join(logs_base_dir, "csv", "cache_albums.csv")
        os.makedirs(os.path.dirname(self.album_cache_csv), exist_ok=True)

        # In-memory album years cache
        # key: hash of "artist|album", value: (year, artist, album)
        self.album_years_cache: Dict[str, Tuple[str, str, str]] = {}
        self.last_cache_sync = time.time()
        self.cache_dirty = False  # Flag indicating unsaved changes
        self.sync_interval = config.get("album_cache_sync_interval", 300)  # Sync every 5 minutes by default

        # Load initial album cache
        self._load_album_years_cache()

    def _generate_album_key(self, artist: str, album: str) -> str:
        """
        Generates a unique hash key for an album based on artist and album names.
        Uses SHA256 to avoid issues with separator characters in names.

        :param artist: The artist name.
        :param album: The album name.
        :return: A SHA256 hash string.
        """
        # Combine artist and album names (normalized) and hash them
        normalized_names = f"{artist.strip().lower()}|{album.strip().lower()}"
        return hashlib.sha256(normalized_names.encode('utf-8')).hexdigest()

    def _is_expired(self, expiry_time: float) -> bool:
        """
        Check if the cache entry has expired based on the expiry time.

        :param expiry_time: The expiry time in seconds since epoch.
        :return: True if the entry has expired, False otherwise.
        """
        return time.time() > expiry_time

    def _hash_key(self, key_data: Any) -> str:
        """
        Generates a SHA256 hash for a general cache key.
        Used for the generic in-memory cache.

        :param key_data: The key data to hash (can be a tuple or any hashable object).
        :return: The hashed key string.
        """
        # Ensure key_data is hashable or convert it to a consistent string representation
        try:
            key_str = str(key_data)
        except Exception as e:
            self.error_logger.error(f"Failed to convert key_data to string for hashing: {e}")
            # Fallback to a default or raise an error, depending on desired behavior
            key_str = "unhashable_key"  # Or raise TypeError("Unhashable key data")

        return hashlib.sha256(key_str.encode('utf-8')).hexdigest()

    def set(self, key_data: Any, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set a value in the in-memory cache with an optional TTL.
        """
        key = self._hash_key(key_data)
        ttl_value = ttl if ttl is not None else self.default_ttl
        expiry_time = time.time() + ttl_value
        self.cache[key] = (value, expiry_time)

    async def set_async(self, key_data: Any, value: Any, ttl: Optional[int] = None) -> None:
        """
        Asynchronously set a value in the in-memory cache with an optional TTL.

        :param key_data: The key data to hash (can be a tuple or any hashable object).
        :param value: The value to store in the cache.
        :param ttl: The time-to-live in seconds for the cache entry.
        """
        self.set(key_data, value, ttl)

    async def get_async(self, key_data: Any, compute_func: Optional[Callable[[], "asyncio.Future[Any]"]] = None) -> Any:
        """
        Asynchronously fetch a value from the in-memory cache or compute it if needed.
        - If `key_data == "ALL"`, returns a list of *valid* cached track objects.
        - Otherwise uses a hashed key for the dictionary-based lookup.
        - If value not found or expired, and compute_func provided, it calls compute_func().
        """

        # Special case if key_data == "ALL": return a list of track dicts
        if key_data == "ALL":
            tracks = []
            now_ts = time.time()
            # Iterate through values, which are (value, expiry_time) tuples
            for val, expiry in self.cache.values():
                # Filter out only non-expired track objects (assuming tracks are dicts with 'id')
                if now_ts < expiry and isinstance(val, dict) and "id" in val:
                    tracks.append(val)
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
        """
        Invalidate a single cache entry by key or clear all entries if key_data == "ALL".

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
            self.console_logger.info(f"In-memory generic cache invalidated for key: {key_data}")

    def clear(self) -> None:
        """
        Clear the entire in-memory generic cache.
        """
        self.cache.clear()
        self.console_logger.info("All in-memory generic cache entries cleared.")

    def _load_album_years_cache(self) -> None:
        """
        Load album years cache from CSV file into memory.
        Called during initialization.
        Reads artist, album, and year from CSV and stores using hash keys.
        """
        self.album_years_cache.clear()  # Clear existing cache before loading
        try:
            if os.path.exists(self.album_cache_csv):
                with open(self.album_cache_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Read artist, album, and year from CSV columns
                        a = row.get("artist", "").strip()
                        b = row.get("album", "").strip()
                        y = row.get("year", "").strip()
                        if a and b and y:
                            # Generate hash key from artist and album
                            key_hash = self._generate_album_key(a, b)
                            # Store year, original artist, and original album in the in-memory cache value
                            self.album_years_cache[key_hash] = (y, a, b)
                self.console_logger.info(f"Loaded {len(self.album_years_cache)} album years into memory cache")
            else:
                self.console_logger.info(f"Album-year cache file not found, will create at: {self.album_cache_csv}")

        except Exception as e:
            self.error_logger.error(f"Error loading album years cache: {e}", exc_info=True)

    async def _sync_cache_if_needed(self, force=False) -> None:
        """
        Synchronize cache to disk if needed based on time interval or force flag.

        :param force: If True, force synchronization regardless of time interval
        """
        now = time.time()
        if force or (self.cache_dirty and now - self.last_cache_sync >= self.sync_interval):
            await self._save_cache_to_disk()
            self.last_cache_sync = now
            self.cache_dirty = False

    async def _save_cache_to_disk(self) -> None:
        """
        Save the in-memory album years cache to disk.
        Uses atomic write pattern with temporary file.
        Writes artist, album, and year columns.
        """
        temp_file = f"{self.album_cache_csv}.tmp"

        try:
            os.makedirs(os.path.dirname(self.album_cache_csv), exist_ok=True)

            with open(temp_file, "w", newline="", encoding="utf-8") as f:
                # Define fieldnames for the CSV file
                writer = csv.DictWriter(f, fieldnames=["artist", "album", "year"])
                writer.writeheader()

                # Iterate through values in the in-memory cache (which are (year, artist, album) tuples)
                for year, artist, album in self.album_years_cache.values():
                    # Write artist, album, and year as separate columns
                    writer.writerow({"artist": artist, "album": album, "year": year})

            # Atomic rename
            # If on Windows, you may need to delete the target file before renaming it
            if os.path.exists(self.album_cache_csv):
                if sys.platform == 'win32':
                    os.replace(temp_file, self.album_cache_csv)  # Windows: atomic rename with replacement
                else:
                    os.rename(temp_file, self.album_cache_csv)  # POSIX: atomic rename
            else:
                os.rename(temp_file, self.album_cache_csv)

            self.console_logger.info(f"Synchronized {len(self.album_years_cache)} album years to disk")

        except Exception as e:
            self.error_logger.error(f"Error synchronizing cache to disk: {e}", exc_info=True)
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except OSError as e:
                    # Log the error if removing the temp file fails, but don't raise it
                    self.error_logger.warning(f"Could not remove temporary cache file {temp_file}: {e}")

    async def initialize_album_cache_csv(self) -> None:
        """
        If the album cache CSV file does not exist, create it with the header row.
        Ensures the CSV has 'artist', 'album', 'year' columns.

        :return: None
        """
        if not os.path.exists(self.album_cache_csv):
            self.console_logger.info(f"Creating album-year CSV cache: {self.album_cache_csv}")
            # Define fieldnames for the CSV file
            with open(self.album_cache_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["artist", "album", "year"])
                writer.writeheader()

    async def get_last_run_timestamp(self) -> datetime:
        """
        Get the timestamp of the last incremental run.

        Returns:
            datetime: The timestamp of the last incremental run, or datetime.min if not found

        Example:
            >>> last_run = await cache_service.get_last_run_timestamp()
            >>> if (datetime.now() - last_run).total_seconds() > 3600:
            >>>     print("More than an hour since last run")
        """
        # Ensure the logging config section and key exist before accessing
        logging_config = self.config.get("logging", {})
        last_file_key = logging_config.get("last_incremental_run_file")

        if not last_file_key:
            self.error_logger.warning("Config key 'logging.last_incremental_run_file' is missing.")
            return datetime.min  # Return min datetime if config key is missing

        # Use get_full_log_path to get the file path
        last_file = get_full_log_path(self.config, "last_incremental_run_file", "last_incremental_run.log", self.error_logger)

        if not os.path.exists(last_file):
            return datetime.min

        try:
            with open(last_file, "r", encoding="utf-8") as f:
                last_run_str = f.read().strip()
            return datetime.strptime(last_run_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, IOError, OSError) as e:
            self.error_logger.error(f"Error reading last run timestamp from {last_file}: {e}")
            return datetime.min

    async def get_album_year_from_cache(self, artist: str, album: str) -> Optional[str]:
        """
        Get album year from the in-memory cache for a given artist and album.
        Uses the hash key for lookup. Returns the year if found, None otherwise.

        :param artist: The artist name.
        :param album: The album name.
        :return: The year of the album as a string, or None if not found.
        """
        # Generate the hash key for the album
        key_hash = self._generate_album_key(artist, album)

        # Sync cache if needed (this will load from disk if not already loaded)
        await self._sync_cache_if_needed()  # Note: _sync_cache_if_needed saves, doesn't load. Loading is in __init__

        # Check if the hash key exists in the in-memory cache
        if key_hash in self.album_years_cache:
            # The value is a tuple (year, artist, album)
            year, _, _ = self.album_years_cache[key_hash]
            self.console_logger.debug(f"Album year cache hit for '{artist} - {album}': {year}")
            return year
        self.console_logger.debug(f"Album year cache miss for '{artist} - {album}'")
        return None

    async def store_album_year_in_cache(self, artist: str, album: str, year: str) -> None:
        """
        Store or update an album year in the in-memory cache and mark it for disk sync.
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

        self.console_logger.debug(f"Album year stored in cache for '{artist} - {album}': {year}")

    def invalidate_album_cache(self, artist: str, album: str) -> None:
        """
        Invalidate the album-year cache for a given artist and album.
        Removes the entry from the in-memory cache using the hash key.

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
            self.console_logger.info(f"Invalidated album-year cache for: {artist} - {album}")
            # No need to save immediately here, _sync_cache_if_needed will handle it based on interval/force

    def invalidate_all_albums(self) -> None:
        """
        Invalidate the entire album-year cache.
        Clears the in-memory cache and marks it for synchronization.
        """
        if self.album_years_cache:
            self.album_years_cache.clear()
            self.cache_dirty = True
            self.console_logger.info("Entire album-year cache cleared.")
            # No need to save immediately here, _sync_cache_if_needed will handle it based on interval/force

    async def sync_cache(self) -> None:
        """
        Force synchronization of the in-memory album cache to disk.
        Useful before application shutdown to ensure data persistence.
        """
        await self._sync_cache_if_needed(force=True)
