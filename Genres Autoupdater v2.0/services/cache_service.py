#!/usr/bin/env python3

"""
Cache Service Module

This module provides two caching mechanisms:
    1. An in-memory (dict) cache for generic track data with TTL support (via get_async, set_async, etc.)
    2. A persistent CSV-based cache for album years that persists data across application runs.

Features:
    - In-memory cache: supports TTL, async operations, and automatic value computation
    - CSV cache: stores album year information persistently with artist/album lookup
    - Both caches support invalidation (individual entries or complete cache)

Usage:
    - For track-level caching: get_async/set_async with optional TTL
    - For album-year caching: get_album_year_from_cache/store_album_year_in_cache
"""
import asyncio
import csv
import hashlib
import os
import time

from typing import Any, Callable, Dict, Optional, Tuple

class CacheService:
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
        self.cache: Dict[str, Tuple[Any, float]] = {}  # key -> (value, expiry_time)

        # CSV cache settings
        logs_base_dir = self.config.get("logs_base_dir", ".")
        # Save the file in the "csv" folder next to track_list.csv
        self.album_cache_csv = os.path.join(logs_base_dir, "csv", "cache_albums.csv")
        os.makedirs(os.path.dirname(self.album_cache_csv), exist_ok=True)

    def _is_expired(self, expiry_time: float) -> bool:
        """
        Check if the cache entry has expired based on the expiry time.
        
        :param expiry_time: The expiry time in seconds since epoch.
        :return: True if the entry has expired, False otherwise.
        """
        return time.time() > expiry_time

    def _hash_key(self, key_data: Any) -> str:
        """
        Save the value to the in-memory cache with specified TTL (or default).

        :param key_data: The key data to hash (can be a tuple or any hashable object).
        :return: The hashed key string.
        """
        if isinstance(key_data, tuple):
            key_str = '|'.join(str(item) for item in key_data)
        else:
            key_str = str(key_data)
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

    async def get_async(
        self,
        key_data: Any,
        compute_func: Optional[Callable[[], "asyncio.Future[Any]"]] = None
    ) -> Any:
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
            for (_, (val, expiry)) in self.cache.items():
                # Filter out only non-expired track objects
                if now_ts < expiry and isinstance(val, dict) and "id" in val:
                    tracks.append(val)
            return tracks

        # Otherwise standard logic
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
            # Clear all
            self.cache.clear()
            self.console_logger.info("Invalidated ALL in-memory cache entries.")
            return
        key = self._hash_key(key_data)
        if key in self.cache:
            del self.cache[key]
            self.console_logger.info(f"In-memory cache invalidated for key: {key_data}")

    def clear(self) -> None:
        """
        Clear the entire in-memory cache.
        """
        self.cache.clear()
        self.console_logger.info("All in-memory cache entries cleared.")

    async def initialize_album_cache_csv(self) -> None:
        """
        If the album cache CSV file does not exist, create it with the header row.

        :return: None
        """
        if not os.path.exists(self.album_cache_csv):
            self.console_logger.info(f"Creating album-year CSV cache: {self.album_cache_csv}")
            with open(self.album_cache_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["artist", "album", "year"])
                writer.writeheader()

    def _read_album_csv(self) -> Dict[str, str]:
        """
        Read the album cache CSV file and return the contents as a dictionary.

        :return: A dictionary mapping artist|||album to year.
        """
        cache_map: Dict[str, str] = {}
        if not os.path.exists(self.album_cache_csv):
            return cache_map
        try:
            with open(self.album_cache_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    a = row.get("artist", "").strip().lower()
                    b = row.get("album", "").strip().lower()
                    y = row.get("year", "").strip()
                    if a and b and y:
                        key = f"{a}|||{b}"
                        cache_map[key] = y
        except Exception as e:
            self.error_logger.error(f"Failed to read album cache CSV: {e}", exc_info=True)
        return cache_map

    def _write_album_csv(self, cache_map: Dict[str, str]) -> None:
        """
        Write the album cache dictionary to the CSV file.

        :param cache_map: A dictionary mapping artist|||album to year.
        :return: None
        """
        try:
            with open(self.album_cache_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["artist", "album", "year"])
                writer.writeheader()
                for key, year in cache_map.items():
                    # key = a|||b
                    # year = str
                    a, b = key.split("|||", 1)
                    writer.writerow({
                        "artist": a,
                        "album": b,
                        "year": year
                    })
        except Exception as e:
            self.error_logger.error(f"Failed to write album cache CSV: {e}", exc_info=True)

    async def get_album_year_from_cache(self, artist: str, album: str) -> Optional[str]:
        """
        Get album year from the CSV cache for a given artist and album.
        If the entry is not found, return None.

        :param artist: The artist name.
        :param album: The album name.
        :return: The year of the album as a string, or None if not found.
        """
        await self.initialize_album_cache_csv()

        a_lower = artist.strip().lower()
        b_lower = album.strip().lower()
        cache_map = self._read_album_csv()
        key = f"{a_lower}|||{b_lower}"

        if key in cache_map:
            return cache_map[key]
        return None

    async def store_album_year_in_cache(self, artist: str, album: str, year: str) -> None:
        """
        Save the album year in the CSV cache for a given artist and album.

        :param artist: The artist name.
        :param album: The album name.
        :param year: The year of the album as a string.
        :return: None
        """
        await self.initialize_album_cache_csv()

        a_lower = artist.strip().lower()
        b_lower = album.strip().lower()
        cache_map = self._read_album_csv()
        key = f"{a_lower}|||{b_lower}"
        cache_map[key] = year.strip()  # Save as a string

        await self._write_album_csv(cache_map)
        self.console_logger.debug(f"Album year stored in CSV cache for '{artist} - {album}': {year}")

    def invalidate_album_cache(self, artist: str, album: str) -> None:
        """
        Invalidate the album-year cache for a given artist and album.

        :param artist: The artist name.
        :param album: The album name.
        :return: None
        """
        if not os.path.exists(self.album_cache_csv):
            return
        a_lower = artist.strip().lower()
        b_lower = album.strip().lower()
        key = f"{a_lower}|||{b_lower}"

        cache_map = self._read_album_csv()
        if key in cache_map:
            del cache_map[key]
            self._write_album_csv(cache_map)
            self.console_logger.info(f"Invalidated album-year cache for: {artist} - {album}")

    def invalidate_all_albums(self) -> None:
        """
        Invalidate the entire album-year cache.

        :return: None
        """
        if os.path.exists(self.album_cache_csv):
            try:
                os.remove(self.album_cache_csv)
                self.console_logger.info("Entire album-year CSV cache removed.")
            except Exception as e:
                self.error_logger.error(f"Failed to remove album-year CSV cache: {e}", exc_info=True)