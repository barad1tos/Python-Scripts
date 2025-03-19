#!/usr/bin/env python3

"""
Cache Service Module

This module provides a simple cache service that stores key-value pairs
with an optional time-to-live (TTL) for each entry. The cache can be used
to store the results of expensive computations and avoid redundant work.

Example:
    >>> cache = CacheService(config, console_logger, error_logger)
    >>> value = await cache.get_async("some_key", lambda: some_async_function())
    >>> print(value)
"""

import asyncio
import hashlib
import time

from typing import Any, Callable, Dict, Optional, Tuple

class CacheService:
    def __init__(self, config: dict, console_logger, error_logger):
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.default_ttl = config.get("cache_ttl_seconds", 900)
        self.cache: Dict[str, Tuple[Any, float]] = {}  # (value, expiry_time)

    def _is_expired(self, expiry_time: float) -> bool:
        """Checks if the cache has expired"""
        return time.time() > expiry_time

    def _hash_key(self, key_data: Any) -> str:
        """Creates a unique hash for the cache key"""
        if isinstance(key_data, tuple):
            key_str = '|'.join(str(item) for item in key_data)
        else:
            key_str = str(key_data)
        return hashlib.sha256(key_str.encode('utf-8')).hexdigest()

    def set(self, key_data: Any, value: Any, ttl: Optional[int] = None) -> None:
        """Saves the value to the cache with the specified TTL"""
        key = self._hash_key(key_data)
        ttl_value = ttl if ttl is not None else self.default_ttl
        expiry_time = time.time() + ttl_value
        self.cache[key] = (value, expiry_time)

    async def set_async(self, key_data: Any, value: Any, ttl: Optional[int] = None) -> None:
        """Asynchronous version set for compatibility"""
        self.set(key_data, value, ttl)

    async def get_async(self, key_data: Any, compute_func: Optional[Callable[[], "asyncio.Future[Any]"]] = None) -> Any:
        """Asynchronously fetches a value from the cache or calculates it if necessary"""
        # Special case for key "ALL"
        if key_data == "ALL":
            tracks = []
            for cache_key, (value, expiry_time) in self.cache.items():
                if isinstance(value, dict) and "id" in value and not self._is_expired(expiry_time):
                    tracks.append(value)
            return tracks
            
        # Standard logic for other keys
        key = self._hash_key(key_data)
        if key in self.cache:
            value, expiry_time = self.cache[key]
            if not self._is_expired(expiry_time):
                self.console_logger.debug(f"Cache hit for {key_data}")
                return value
            else:
                self.console_logger.debug(f"Cache expired for {key_data}")
                del self.cache[key]
                
        # If the value is not in the cache or is out of date, calculate it
        if compute_func is not None:
            self.console_logger.debug(f"Computing value for {key_data}")
            value = await compute_func()
            if value is not None:
                self.set(key_data, value)
            return value
        return None

    async def get_album_year_from_cache(self, artist: str, album: str) -> Optional[str]:
        """Gets the year of the album from the cache"""
        cache_key = f"year_{artist}_{album}".replace(" ", "_").lower()
        return await self.get_async(cache_key)
        
    async def store_album_year_in_cache(self, artist: str, album: str, year: str) -> None:
        """Saves album year to cache with long-term TTL"""
        cache_key = f"year_{artist}_{album}".replace(" ", "_").lower()
        ttl = self.config.get('year_retrieval', {}).get('cache_ttl_days', 365) * 24 * 60 * 60
        await self.set_async(cache_key, year, ttl)

    def invalidate(self, key_data: Any) -> None:
        """Deletes a value from the cache"""
        key = self._hash_key(key_data)
        if key in self.cache:
            del self.cache[key]

    def clear(self) -> None:
        """Clears all cache"""
        self.cache.clear()