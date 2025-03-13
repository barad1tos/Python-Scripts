#!/usr/bin/env python3

"""
Cache Service Module

This module provides a simple cache service that stores key-value pairs
with an optional time-to-live (TTL) for each entry. The cache can be used
to store the results of expensive computations and avoid redundant work.

Example:
    >>> cache = CacheService(ttl=60)
    >>> value = await cache.get_async("some_key", lambda: some_async_function())
    >>> print(value)
"""

import asyncio
import hashlib
import time

from typing import Any, Callable, Dict, Optional, Tuple

class CacheService:
    def __init__(self, ttl: Optional[int] = None):
        """
        Initialize the CacheService with an optional time-to-live (TTL) value.

        :param ttl: Time-to-live (in seconds) for each cache entry.
        """
        self.ttl = ttl
        self.cache: Dict[str, Tuple[Any, float]] = {}

    def _is_expired(self, timestamp: float) -> bool:
        """
        Check if the cache entry with the given timestamp is expired.

        :param timestamp: Timestamp of the cache entry.
        :return: True if the cache entry is expired, False otherwise.
        """
        if self.ttl is None:
            return False
        return (time.time() - timestamp) > self.ttl

    def _hash_key(self, key_data: Any) -> str:
        """
        Generate a unique hash key from the key_data.

        :param key_data: Data to generate the key from.
        :return: Hashed key string.
        """
        if isinstance(key_data, tuple):
            key_str = '|'.join(str(item) for item in key_data)
        else:
            key_str = str(key_data)
        return hashlib.sha256(key_str.encode('utf-8')).hexdigest()

    async def get_async(self, key_data: Any, compute_func: Optional[Callable[[], "asyncio.Future[Any]"]] = None) -> Any:
        """
        Asynchronously retrieve a value from the cache by key_data.
        If the key is not present or has expired and compute_func is provided,
        await compute_func() to compute a new value, store it, and return it.

        :param key_data: Data for generating the cache key.
        :param compute_func: An asynchronous function to compute the value if not found or expired.
        :return: The cached or computed value.
        """
        key = self._hash_key(key_data)
        if key in self.cache:
            value, timestamp = self.cache[key]
            if not self._is_expired(timestamp):
                return value
            else:
                del self.cache[key]
        if compute_func is not None:
            value = await compute_func()
            self.set(key_data, value)
            return value
        return None

    def set(self, key_data: Any, value: Any) -> None:
        """
        Set a value in the cache with the given key_data.

        :param key_data: Data for generating the cache key.
        :param value: Value to store in the cache.
        """
        key = self._hash_key(key_data)
        self.cache[key] = (value, time.time())

    def invalidate(self, key_data: Any) -> None:
        """
        Invalidate a cache entry by key_data.

        :param key_data: Data for generating the cache key.
        """
        key = self._hash_key(key_data)
        if key in self.cache:
            del self.cache[key]

    def clear(self) -> None:
        """
        Clear the entire cache.
        """
        self.cache.clear()