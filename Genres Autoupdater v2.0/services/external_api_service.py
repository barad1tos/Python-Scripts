#!/usr/bin/env python3
"""
Enhanced ExternalApiService for Music Metadata Retrieval

This module provides an optimized service for interacting with external APIs (MusicBrainz, Discogs, and Last.fm)
to retrieve album release years and other metadata with enhanced performance and improved rate limiting.

Key features:
1. Moving window rate limiting for optimal API throughput while respecting API constraints
2. Concurrent API requests with controlled parallelism
3. Sophisticated scoring algorithm for determining original album release years
4. Artist activity period determination for better release validation
5. Multi-source data aggregation with intelligent conflict resolution
6. Detailed performance metrics and structured logging

The module contains:
- EnhancedRateLimiter: Provides precise rate limiting using a moving window approach
- ExternalApiService: Main service class that coordinates API requests and data processing

Example:
    >>> import asyncio, logging
    >>> from services.external_api_service import ExternalApiService
    >>>
    >>> # Setup loggers and load config
    >>> console_logger = logging.getLogger("console")
    >>> error_logger = logging.getLogger("error")
    >>> service = ExternalApiService(config, console_logger, error_logger)
    >>>
    >>> async def get_year():
    ...     await service.initialize()
    ...     try:
    ...         year, is_definitive = await service.get_album_year("Agalloch", "The White EP", "2008")
    ...         print(f"Original release year: {year}, Definitive: {is_definitive}")
    ...     finally:
    ...         await service.close()
    >>>
    >>> asyncio.run(get_year())
"""

import asyncio
import logging
import random
import re
import time
import urllib.parse

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import requests


class EnhancedRateLimiter:
    """
    Advanced rate limiter using a moving window approach.

    This ensures maximum throughput while strictly adhering to API rate limits.

    Attributes:
        requests_per_window (int): Maximum requests allowed in the time window
        window_size (float): Time window in seconds
        request_timestamps (List[float]): Timestamps of recent requests
        semaphore (asyncio.Semaphore): Controls concurrent access
        logger (logging.Logger): Logger for rate limiting events
    """

    def __init__(
        self,
        requests_per_window: int,
        window_size: float,
        max_concurrent: int = 3,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the rate limiter.

        Args:
            requests_per_window: Maximum requests allowed in the time window
            window_size: Time window in seconds
            max_concurrent: Maximum concurrent requests
            logger: Logger for rate limiting events

        Example:
            >>> limiter = EnhancedRateLimiter(60, 60.0, 5)  # 60 requests/minute, max 5 concurrent
        """
        self.requests_per_window = requests_per_window
        self.window_size = window_size
        self.request_timestamps = []
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.logger = logger or logging.getLogger(__name__)
        self.total_requests = 0
        self.total_wait_time = 0

    async def acquire(self) -> float:
        """
        Acquire permission to make a request, waiting if necessary.

        Returns:
            float: The wait time in seconds (0 if no wait was needed)

        Example:
            >>> wait_time = await limiter.acquire()
            >>> if wait_time > 0:
            >>>     print(f"Waited {wait_time:.2f}s for rate limiting")
        """
        start_wait = time.time()
        wait_time = await self._wait_if_needed()
        self.total_requests += 1
        self.total_wait_time += wait_time

        # Acquire semaphore for concurrency control (this will wait if too many concurrent requests)
        await self.semaphore.acquire()

        return wait_time

    def release(self) -> None:
        """
        Release the semaphore after request is completed.

        Example:
            >>> limiter.release()
        """
        self.semaphore.release()

    async def _wait_if_needed(self) -> float:
        """
        Check if we need to wait to respect rate limits.

        Returns:
            float: Wait time in seconds (0 if no wait needed)
        """
        now = time.time()

        # Clean up old timestamps outside the window
        while self.request_timestamps and now - self.request_timestamps[0] > self.window_size:
            self.request_timestamps.pop(0)

        # If we've reached the limit, wait until oldest timestamp leaves the window
        if len(self.request_timestamps) >= self.requests_per_window:
            wait_time = self.request_timestamps[0] + self.window_size - now
            if wait_time > 0:
                self.logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                # Recursive call to check again after waiting
                return wait_time + await self._wait_if_needed()

        # Record this request's timestamp
        self.request_timestamps.append(time.time())
        return 0

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about rate limiter usage.

        Returns:
            Dict with stats including total requests, wait time, and average wait

        Example:
            >>> stats = limiter.get_stats()
            >>> print(f"Made {stats['total_requests']} requests with {stats['avg_wait_time']:.2f}s average wait")
        """
        return {
            "total_requests": self.total_requests,
            "total_wait_time": self.total_wait_time,
            "avg_wait_time": self.total_wait_time / max(1, self.total_requests),
            "current_window_usage": len(self.request_timestamps),
            "max_requests_per_window": self.requests_per_window,
        }


class ExternalApiService:
    """
    Enhanced service for interacting with MusicBrainz and Discogs APIs.

    Features:
    - Accurate rate limiting with moving window approach
    - Concurrency control for parallel requests
    - Proper authentication handling for all APIs
    - Structured logging for debugging
    - Year determination with improved scoring algorithm
    """

    def __init__(self, config: Dict[str, Any], console_logger: logging.Logger, error_logger: logging.Logger):
        """
        Initialize the API service with configuration and loggers.

        Args:
            config: Application configuration
            console_logger: Logger for console messages
            error_logger: Logger for error messages

        Example:
            >>> service = ExternalApiService(config, console_logger, error_logger)
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.session: Optional[aiohttp.ClientSession] = None

        # Extract API configuration
        year_config = config.get("year_retrieval", {})

        # Last.fm API configuration
        self.lastfm_api_key = config.get("year_retrieval", {}).get("lastfm_api_key")
        self.lastfm_api_secret = config.get("year_retrieval", {}).get("lastfm_api_secret")
        self.use_lastfm = self.lastfm_api_key is not None and config.get("year_retrieval", {}).get("use_lastfm", True)

        # Initialize enhanced rate limiters
        self.rate_limiters = {
            'discogs': EnhancedRateLimiter(
                requests_per_window=year_config.get("discogs_requests_per_minute", 25),
                window_size=60.0,  # 1 minute window
                max_concurrent=year_config.get("concurrent_api_calls", 3),
                logger=console_logger,
            ),
            'musicbrainz': EnhancedRateLimiter(
                requests_per_window=year_config.get("musicbrainz_requests_per_second", 1),
                window_size=1.0,  # 1 second window
                max_concurrent=config.get("year_retrieval", {}).get("concurrent_api_calls", 3),
                logger=console_logger,
            ),
            'lastfm': EnhancedRateLimiter(
                requests_per_window=year_config.get("lastfm_requests_per_minute", 5),
                window_size=1.0,  # 1 second window
                max_concurrent=year_config.get("concurrent_api_calls", 3),
                logger=console_logger,
            ),
        }

        self.request_counts = {'discogs': 0, 'musicbrainz': 0, 'lastfm': 0}

        # API authentication and client settings
        self.preferred_api = year_config.get('preferred_api', 'musicbrainz')
        self.discogs_token = year_config.get('discogs_token')
        self.musicbrainz_app_name = year_config.get('musicbrainz_app_name', "MusicGenreUpdater/2.0")
        self.musicbrainz_contact_email = year_config.get('contact_email', 'roman.borodavkin@gmail.com')

        # Parameters for year validation
        self.min_valid_year = year_config.get('min_valid_year', 1900)
        self.trust_threshold = year_config.get('trust_threshold', 50)
        self.max_year_difference = year_config.get('max_year_difference', 5)
        self.current_year = datetime.now().year + 1  # +1 allows future releases

        # Cache for artist activity periods
        self.activity_cache: Dict[str, Tuple[Tuple[Optional[int], Optional[int]], float]] = {}
        self.artist_period_context: Optional[Dict[str, Optional[int]]] = None
        self.cache_ttl_days = config.get("year_retrieval", {}).get("artist_cache_ttl_days", 30)  # Default 30 days

        # Stats tracking
        self.request_counts = {'discogs': 0, 'musicbrainz': 0}

    async def initialize(self, force: bool = False) -> None:
        """
        Initialize the aiohttp session with optimized settings.

        Args:
            force: Force reinitialization even if a session exists

        Example:
            >>> await service.initialize()
        """
        if force or not self.session or self.session.closed:
            if force and self.session and not self.session.closed:
                await self.session.close()

            # Create session with optimized timeout and TCP connector settings
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)
            connector = aiohttp.TCPConnector(
                limit=20,  # Max connections (higher than default)
                ssl=False,  # Skip SSL verification for performance
                use_dns_cache=True,
                ttl_dns_cache=300,  # Cache DNS results for 5 minutes
            )

            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self.console_logger.info(f"External API session initialized with optimized settings" + (" (forced)" if force else ""))

    async def close(self) -> None:
        """
        Close the aiohttp session and log statistics.

        Example:
            >>> await service.close()
        """
        if self.session and not self.session.closed:
            # Log rate limiting statistics
            discogs_stats = self.rate_limiters['discogs'].get_stats()
            mb_stats = self.rate_limiters['musicbrainz'].get_stats()

            self.console_logger.info(
                f"API Stats - Discogs: {discogs_stats['total_requests']} requests, " f"avg wait: {discogs_stats['avg_wait_time']:.2f}s"
            )
            self.console_logger.info(
                f"API Stats - MusicBrainz: {mb_stats['total_requests']} requests, " f"avg wait: {mb_stats['avg_wait_time']:.2f}s"
            )

            await self.session.close()
            self.console_logger.info("External API session closed")

    async def _make_api_request(
        self, api_name: str, url: str, headers: Dict[str, str] = None, max_retries: int = 3, base_delay: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        Make an API request with rate limiting, error handling, and retry logic.

        Args:
            api_name: API name for rate limiting ('musicbrainz' or 'discogs')
            url: Full URL to request
            headers: Request headers
            max_retries: Maximum number of retry attempts (default: 3)
            base_delay: Base delay for exponential backoff (default: 1s)

        Returns:
            Response data as dictionary or None if request failed
        """
        if not self.session:
            await self.initialize()

        if not headers:
            headers = {}

        # Add appropriate authentication and user agent
        if api_name == 'discogs':
            if self.discogs_token:
                headers['Authorization'] = f'Discogs token={self.discogs_token}'
            headers['User-Agent'] = f'{self.musicbrainz_app_name} ({self.musicbrainz_contact_email})'
        elif api_name == 'musicbrainz':
            headers['User-Agent'] = f'{self.musicbrainz_app_name} ({self.musicbrainz_contact_email})'

        limiter = self.rate_limiters.get(api_name)
        if not limiter:
            self.error_logger.error(f"No rate limiter found for API: {api_name}")
            return None

        # Add retry logic with exponential backoff
        import random

        for retry in range(max_retries + 1):  # +1 for initial attempt
            try:
                # Get permission from rate limiter (this will wait if needed)
                wait_time = await limiter.acquire()
                if wait_time > 1.0:
                    self.console_logger.debug(f"Waited {wait_time:.2f}s for {api_name} rate limiting")

                # Track request count
                self.request_counts[api_name] = self.request_counts.get(api_name, 0) + 1

                # Make the actual request
                start_time = time.time()
                async with self.session.get(url, headers=headers, timeout=10) as response:
                    elapsed = time.time() - start_time

                    # Log request details
                    self.console_logger.debug(f"{api_name.title()} request: {url} - {response.status} ({elapsed:.2f}s)")

                    # Check for rate limiting headers
                    if api_name == 'discogs' and 'X-RateLimit-Remaining' in response.headers:
                        remaining = response.headers.get('X-RateLimit-Remaining')
                        limit = response.headers.get('X-RateLimit-Limit')
                        self.console_logger.debug(f"Discogs rate limit: {remaining}/{limit} remaining")

                    # Handle server errors with retries (503, 502, etc.)
                    if response.status >= 500:
                        # If we have retries left, wait and try again
                        if retry < max_retries:
                            error_text = await response.text()
                            self.console_logger.warning(
                                f"{api_name.title()} server error ({response.status}), retrying {retry+1}/{max_retries}: {error_text[:100]}"
                            )

                            # Calculate delay with exponential backoff and jitter
                            delay = base_delay * (2**retry) * (0.5 + random.random())
                            await asyncio.sleep(delay)
                            continue

                    # Handle other errors
                    if response.status != 200:
                        error_text = await response.text()
                        self.error_logger.warning(f"{api_name.title()} API error: {response.status} - {error_text[:200]}")
                        return None

                    try:
                        return await response.json()
                    except Exception as e:
                        self.error_logger.error(f"Error parsing {api_name} JSON response: {e}")
                        return None

            except asyncio.TimeoutError:
                # If we have retries left, retry
                if retry < max_retries:
                    self.console_logger.warning(f"{api_name.title()} request timed out, retrying {retry+1}/{max_retries}: {url}")
                    # Exponential backoff with jitter
                    delay = base_delay * (2**retry) * (0.5 + random.random())
                    await asyncio.sleep(delay)
                    continue
                else:
                    self.error_logger.warning(f"{api_name.title()} request timed out after {max_retries} retries: {url}")
                    return None
            except Exception as e:
                self.error_logger.error(f"Error making {api_name} request: {e}")
                return None
            finally:
                # Always release the rate limiter semaphore
                limiter.release()

        # If we've exhausted all retries
        return None

    def should_update_album_year(
        self,
        tracks: List[Dict[str, str]],
        artist: str = "",
        album: str = "",
        current_library_year: str = "",
        pending_verification_service=None,
    ) -> bool:
        """
        Determines whether to update the year for an album based on the status of its tracks.

        Args:
            tracks: List of album tracks
            artist: Artist name (for logging)
            album: Album name (for logging)
            current_library_year: Current year in the library
            pending_verification_service: Service for marking albums for verification

        Returns:
            True if you can update the year, False if you need to keep the current year

        Example:
            >>> can_update = service.should_update_album_year(album_tracks, "Artist", "Album Name", "2020")
            >>> if can_update:
            >>>     # proceed with year update
            >>> else:
            >>>     # keep current year
        """
        prerelease_count = sum(1 for track in tracks if track.get("trackStatus", "").lower() == "prerelease")
        subscription_count = sum(1 for track in tracks if track.get("trackStatus", "").lower() == "subscription")
        total_tracks = len(tracks)

        # If there is at least one track in prerelease status
        if prerelease_count > 0:
            # If there are no tracks in the subscription status, or if the pre-release tracks are more than 50%
            if subscription_count == 0 or prerelease_count > total_tracks * 0.5:
                self.console_logger.info(
                    f"Album '{artist} - {album}' has {prerelease_count}/{total_tracks} tracks in prerelease status. "
                    f"Keeping current year: {current_library_year}"
                )

                # Mark for future verification if service is available
                if pending_verification_service:
                    pending_verification_service.mark_for_verification(artist, album)

                return False

        return True

    async def get_album_year(
        self, artist: str, album: str, current_library_year: Optional[str] = None, pending_verification_service=None
    ) -> Tuple[Optional[str], bool]:
        """
        Determine the original release year for an album using optimized API calls.

        This enhanced method prioritizes MusicBrainz release group dates and implements
        a more robust scoring system with three different data sources: MusicBrainz, Discogs,
        and Last.fm.

        Args:
            artist: Artist name
            album: Album name
            current_library_year: Year currently in the library (if any)
            pending_verification_service: Service to track albums needing future verification

        Returns:
            Tuple of (determined_year, is_definitive)
            - determined_year: The determined original release year as a string, or None if not found
            - is_definitive: False if the result is uncertain and should be verified later

        Example:
            >>> year, is_definitive = await service.get_album_year("Vildhjarta", "Thousands of Evils", "2013")
            >>> print(f"Original release year: {year}, Definitive: {is_definitive}")
        """
        # Normalize inputs to improve matching
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)

        self.console_logger.info(f"Searching for original release year: '{artist_norm} - {album_norm}' (current: {current_library_year or 'none'})")

        # Special handling for very recent/future releases
        current_year = datetime.now().year

        if current_library_year:
            try:
                library_year_int = int(current_library_year)
                # If the library year is current or future year, trust it
                if library_year_int >= current_year:
                    self.console_logger.info(
                        f"Current library year {current_library_year} indicates a very recent or upcoming release. Trusting library data."
                    )
                    return current_library_year, True
            except ValueError:
                pass

        # Check if we should update based on track status
        album_tracks = []  # You'll need to provide the actual tracks for this album here
        if album_tracks and not self.should_update_album_year(
            album_tracks, artist_norm, album_norm, current_library_year, pending_verification_service
        ):
            return current_library_year, False

        # Get artist's activity period for better year evaluation
        start_year, end_year = await self.get_artist_activity_period(artist_norm)

        # Set artist period context for this request
        self.artist_period_context = {'start_year': start_year, 'end_year': end_year}

        self.console_logger.info(f"Artist activity period: {start_year or 'unknown'} - {end_year or 'present'}")

        try:
            # Make API calls concurrently - this is a key optimization
            api_tasks = [
                self._get_scored_releases_from_musicbrainz(artist_norm, album_norm),
                self._get_scored_releases_from_discogs(artist_norm, album_norm),
            ]

            # Add Last.fm request if configured
            if self.use_lastfm:
                api_tasks.append(self._get_year_from_lastfm(artist_norm, album_norm))

            # Wait for all API calls to complete
            results = await asyncio.gather(*api_tasks, return_exceptions=True)

            # Process results
            musicbrainz_data = results[0] if isinstance(results[0], list) else []
            discogs_data = results[1] if len(results) > 1 and isinstance(results[1], list) else []

            # Process Last.fm results if available
            lastfm_data = []
            if self.use_lastfm and len(results) > 2:
                if isinstance(results[2], dict) and results[2].get('year'):
                    lastfm_year = results[2].get('year')
                    lastfm_source = results[2].get('source', 'Unknown')
                    lastfm_confidence = results[2].get('confidence', 60)

                    self.console_logger.info(f"Last.fm found year {lastfm_year} with confidence {lastfm_confidence} from source: {lastfm_source}")

                    # Add Last.fm result in standard format
                    lastfm_data = [
                        {
                            'source': 'lastfm',
                            'title': album_norm,
                            'year': lastfm_year,
                            'confidence': lastfm_confidence,
                            'source_detail': lastfm_source,
                            'score': 0,
                        }
                    ]

                    # Calculate score for Last.fm result
                    for release in lastfm_data:
                        release['score'] = self._score_lastfm_result(release, artist_norm, album_norm)

            # Combine releases from all sources
            all_releases = []
            if musicbrainz_data:
                all_releases.extend(musicbrainz_data)
            if discogs_data:
                all_releases.extend(discogs_data)
            if lastfm_data:
                all_releases.extend(lastfm_data)

            # Nothing found or very minimal information
            if not all_releases:
                self.console_logger.info(f"No release data found for '{artist} - {album}'")

                # If we have a pending verification service, mark this album for future checking
                if pending_verification_service and current_library_year:
                    pending_verification_service.mark_for_verification(artist, album)
                    self.console_logger.info(f"Marked '{artist} - {album}' for future verification. Using current year: {current_library_year}")

                # Return the current library year but mark as non-definitive
                return current_library_year, False

            # Check if we have minimal data (less than 2 sources with reliable data)
            if len(all_releases) < 2:
                self.console_logger.info(f"Limited release data found for '{artist} - {album}' (only {len(all_releases)} sources)")

                # Use limited data but mark for future verification
                if pending_verification_service and current_library_year:
                    pending_verification_service.mark_for_verification(artist, album)
                    self.console_logger.info(f"Marked '{artist} - {album}' for future verification due to limited data")

            # Cluster releases by years with improved logic
            year_clusters = {}
            year_scores = {}

            for release in all_releases:
                year = release.get('year')
                if not year:
                    continue

                # Cluster releases by year
                if year not in year_clusters:
                    year_clusters[year] = []
                year_clusters[year].append(release)

            # New approach: use only the best score per year
            for year, releases in year_clusters.items():
                # Sort releases by score
                sorted_releases = sorted(releases, key=lambda x: x.get('score', 0), reverse=True)

                # Take only the highest-scoring release for this year
                best_release = sorted_releases[0]
                best_score = best_release.get('score', 0)

                # For exact title+artist matches, use the full score
                if best_score >= 100:  # Likely an exact match
                    year_scores[year] = best_score
                else:
                    # For partial matches, we're more cautious
                    year_scores[year] = best_score

                self.console_logger.info(f"Year {year} best match: {best_release.get('title')} with score {best_score}")

                # If this matches the current library year, give it a significant bonus
                # This helps maintain stability unless there's strong evidence for another year
                if current_library_year and year == current_library_year:
                    library_bonus = self.config.get('year_retrieval', {}).get('library_year_bonus', 25)
                    year_scores[year] += library_bonus
                    self.console_logger.info(f"Added library year bonus of {library_bonus} to year {year}")

            # Sort years by score
            sorted_years = sorted(year_scores.items(), key=lambda x: x[1], reverse=True)

            # Log results
            self.console_logger.info(f"Year scores: " + ", ".join([f"{y}:{s}" for y, s in sorted_years]))

            # Special handling for very recent releases
            if current_library_year:
                try:
                    library_year_int = int(current_library_year)
                    if library_year_int >= current_year:
                        # This is a recent/upcoming release - prefer the library year
                        self.console_logger.info(
                            f"Current library year {current_library_year} is very recent/upcoming. "
                            f"Keeping it unless there's very strong evidence otherwise."
                        )

                        # Only override if we have a high-confidence earlier year
                        if sorted_years and sorted_years[0][1] > 80 and int(sorted_years[0][0]) < library_year_int:
                            self.console_logger.info(
                                f"Found high-confidence earlier year {sorted_years[0][0]} with score {sorted_years[0][1]}. "
                                f"Overriding current year {current_library_year}."
                            )
                            return sorted_years[0][0], True

                        return current_library_year, True
                except ValueError:
                    pass

            # Take the year with the highest score
            if sorted_years:
                best_year = sorted_years[0][0]
                best_score = sorted_years[0][1]

                # Additional check: if there are multiple years with close scores,
                # prefer the earliest year (more likely to be the original release)
                close_years = [y for y, s in sorted_years if s >= best_score * 0.85]

                if len(close_years) > 1:
                    close_years_int = []
                    for y in close_years:
                        try:
                            close_years_int.append(int(y))
                        except ValueError:
                            continue

                    if close_years_int:
                        # Among years with close scores, take the earliest (but valid)
                        valid_years = [y for y in close_years_int if self._is_valid_year(str(y), 75)]
                        if valid_years:
                            earliest_valid_year = str(min(valid_years))
                            self.console_logger.info(f"Multiple high-scoring years found. Using earliest valid: {earliest_valid_year}")

                            # Determine if this is a definitive result based on score
                            is_definitive = best_score >= self.trust_threshold
                            if not is_definitive and pending_verification_service:
                                pending_verification_service.mark_for_verification(artist, album)

                            return earliest_valid_year, is_definitive

                self.console_logger.info(f"Selected best year by score: {best_year} (score: {best_score})")

                # Final check: if the selected year is much newer than the current library year,
                # and the current library year scores reasonably well, stick with the library year
                if current_library_year and best_year != current_library_year:
                    try:
                        best_year_int = int(best_year)
                        current_year_int = int(current_library_year)

                        # If current year is earlier and scores at least 70% of best year
                        if current_year_int < best_year_int and year_scores.get(current_library_year, 0) >= best_score * 0.7:

                            # If significant difference (>3 years) and current year is valid
                            if best_year_int - current_year_int > 3 and self._is_valid_year(current_library_year, 60):

                                self.console_logger.info(
                                    f"Keeping current library year {current_library_year} instead of newer {best_year} "
                                    + f"(scores: {year_scores.get(current_library_year, 0)} vs {best_score})"
                                )

                                # Mark for verification if the scores are close
                                is_definitive = best_score - year_scores.get(current_library_year, 0) >= 25
                                if not is_definitive and pending_verification_service:
                                    pending_verification_service.mark_for_verification(artist, album)

                                return current_library_year, is_definitive
                    except ValueError:
                        pass

                # Determine if the result is definitive based on the score
                is_definitive = best_score >= self.trust_threshold
                if not is_definitive and pending_verification_service:
                    pending_verification_service.mark_for_verification(artist, album)

                return best_year, is_definitive

            # If we can't determine a year, mark for verification and return current year
            if pending_verification_service and current_library_year:
                pending_verification_service.mark_for_verification(artist, album)

            return current_library_year, False
        finally:
            # Always clear the artist period context after use
            self.artist_period_context = None

    async def get_artist_activity_period(self, artist: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Retrieve the period of activity for an artist from MusicBrainz.
        The result is cached to avoid redundant API calls.

        Args:
            artist (str): The artist's name.

        Returns:
            Tuple[Optional[int], Optional[int]]: (start_year, end_year).
            If end_year is None, the artist is still active.
        """
        # Clean cache if it's too large
        if len(self.activity_cache) > 100:
            self._clean_activity_cache()

        # Check cache first to avoid redundant API calls
        cache_key = f"activity_{artist.lower()}"
        # Return cached data if available
        if cache_key in self.activity_cache:
            cache_ttl_seconds = self.cache_ttl_days * 86400  # Convert days to seconds
            current_time = time.time()

            if cache_key in self.activity_cache:
                (start_year, end_year), timestamp = self.activity_cache[cache_key]

                current_time = time.time()
                # Checking if cache is still valid
                if current_time - timestamp < cache_ttl_seconds:
                    return start_year, end_year
                else:
                    self.console_logger.info(f"Cache expired for artist activity: {artist}")
            return self.activity_cache[cache_key]

        self.console_logger.info(f"Determining activity period for artist: {artist}")
        current_year = datetime.now().year

        # Check if the artist appears to be new based on name
        appears_new = any(marker in artist.lower() for marker in ["2020", "2021", "2022", "2023", "2024", "tiktok", "viral"])
        appears_old = any(marker in artist.lower() for marker in ["classic", "legend", "90s", "80s"])

        try:
            # Search for the artist using their name
            artist_encoded = urllib.parse.quote_plus(artist)
            search_url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{artist_encoded}&fmt=json"

            data = await self._make_api_request('musicbrainz', search_url)
            if not data or "artists" not in data or not data["artists"]:
                self.console_logger.warning(f"Cannot determine activity period for {artist}: Artist not found")

                # For new artists that aren't in MusicBrainz yet, assume they're new (last 2-3 years)
                if appears_new:
                    start_year = current_year - 2
                    self.console_logger.info(f"Artist '{artist}' appears to be new and not in MusicBrainz - assuming from {start_year}")
                    self.activity_cache[cache_key] = (start_year, None)
                    return start_year, None
                else:
                    start_year = current_year - 3
                    self.console_logger.info(f"Artist '{artist}' not found - assuming recent from {start_year}")
                    self.activity_cache[cache_key] = (start_year, None)
                    return start_year, None

            # Score artists for better matching
            scored_artists = []
            for artist_data in data["artists"]:
                score = 0
                # Exact name match is highest priority
                if artist_data.get("name", "").lower() == artist.lower():
                    score += 100
                # Partial match
                elif artist.lower() in artist_data.get("name", "").lower() or artist_data.get("name", "").lower() in artist.lower():
                    score += 50

                # Boost for recent artists (active since 2020)
                begin_date = artist_data.get("life-span", {}).get("begin", "")
                if begin_date.startswith("202"):  # 2020+
                    score += 40

                # Check for aliases (alternative names)
                if "aliases" in artist_data:
                    for alias in artist_data["aliases"]:
                        if alias.get("name", "").lower() == artist.lower():
                            score += 90  # Nearly as good as exact match
                            break

                scored_artists.append((score, artist_data))

            # Sort by score descending
            scored_artists.sort(reverse=True, key=lambda x: x[0])

            # Take best match
            if not scored_artists:
                self.console_logger.warning(f"No artist found for query: {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None

            best_score, artist_data = scored_artists[0]
            artist_id = artist_data.get("id")

            # Look for aliases if not done yet
            if "aliases" not in artist_data and artist_id:
                alias_url = f"https://musicbrainz.org/ws/2/artist/{artist_id}?inc=aliases&fmt=json"
                detailed_data = await self._make_api_request('musicbrainz', alias_url)
                if detailed_data and "aliases" in detailed_data:
                    artist_data["aliases"] = detailed_data["aliases"]

            # Check if the API provides lifespan directly
            begin_date = artist_data.get("life-span", {}).get("begin")
            end_date = artist_data.get("life-span", {}).get("end")
            ended = artist_data.get("life-span", {}).get("ended")

            # If we have complete lifespan data, use it but verify
            if begin_date:
                try:
                    start_year = int(begin_date.split("-")[0])
                    end_year = int(end_date.split("-")[0]) if end_date else None

                    # VALIDATION: Check if lifespan makes sense

                    # Override contradictory lifespan for new-looking artists
                    if appears_new and start_year < 2018:
                        self.console_logger.info(f"Artist '{artist}' appears to be new but has old start year {start_year}. Adjusting to 2020.")
                        start_year = 2020
                        end_year = None
                    # Similarly for old-looking artists with suspiciously recent start dates
                    elif appears_old and start_year > 2015:
                        self.console_logger.info(f"Artist '{artist}' appears to be established but has very recent start year {start_year}.")
                        # Don't override but note the discrepancy

                    # Now get the releases to verify the lifespan
                    releases_url = f"https://musicbrainz.org/ws/2/release-group?artist={artist_id}&fmt=json"
                    releases_data = await self._make_api_request('musicbrainz', releases_url)

                    if releases_data and "release-groups" in releases_data:
                        release_years = []
                        for rg in releases_data["release-groups"]:
                            if "first-release-date" in rg and rg["first-release-date"]:
                                try:
                                    year = int(rg["first-release-date"].split("-")[0])
                                    release_years.append(year)
                                except (ValueError, IndexError):
                                    pass

                        if release_years:
                            oldest_release = min(release_years)
                            newest_release = max(release_years)

                            # Check for major discrepancies between lifespan and releases
                            if start_year > oldest_release + 1:
                                self.console_logger.info(
                                    f"Start year {start_year} is later than oldest release {oldest_release} for '{artist}'. Adjusting."
                                )
                                start_year = oldest_release

                            if end_year and newest_release > end_year + 2:
                                self.console_logger.info(
                                    f"End year {end_year} is earlier than newest release {newest_release} for '{artist}'. Adjusting."
                                )
                                # Either the end date is wrong or there are posthumous releases
                                # If the gap is large, probably the end date is wrong
                                if newest_release > end_year + 5:
                                    end_year = None  # Assume still active

                    self.console_logger.info(f"Found artist lifespan: {start_year} - {end_year or 'present'}")
                    self.activity_cache[cache_key] = ((start_year, end_year), time.time())
                    return start_year, end_year
                except (ValueError, IndexError):
                    # If there's an error parsing the dates, fallback to releases
                    pass

            # Get release groups for this artist as a fallback
            releases_url = f"https://musicbrainz.org/ws/2/release-group?artist={artist_id}&fmt=json"

            data = await self._make_api_request('musicbrainz', releases_url)
            if not data:
                # For new-looking artists without data, assume recent start
                if appears_new:
                    start_year = current_year - 2
                    self.console_logger.info(f"No release data for new-looking artist '{artist}'. Assuming start year {start_year}")
                    self.activity_cache[cache_key] = (start_year, None)
                    return start_year, None

                self.console_logger.warning(f"Failed to get releases for {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None

            release_groups = data.get("release-groups", [])

            if not release_groups:
                # Assume recent start for new-looking artists without releases
                if appears_new:
                    start_year = current_year - 2
                    self.console_logger.info(f"No releases found for new-looking artist '{artist}'. Assuming start year {start_year}")
                    self.activity_cache[cache_key] = (start_year, None)
                    return start_year, None

                self.console_logger.warning(f"No releases found for {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None

            # Extract years from release dates
            years = []
            for rg in release_groups:
                if "first-release-date" in rg:
                    date = rg["first-release-date"]
                    if date:
                        try:
                            year = int(date.split("-")[0])
                            years.append(year)
                        except (ValueError, IndexError):
                            pass

            if not years:
                # For new-looking artists without parsable years, assume recent start
                if appears_new:
                    start_year = current_year - 2
                    self.console_logger.info(f"No parsable release years for new-looking artist '{artist}'. Assuming start year {start_year}")
                    self.activity_cache[cache_key] = (start_year, None)
                    return start_year, None

                self.console_logger.warning(f"No release years found for {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None

            # Find the earliest and latest years
            start_year = min(years)
            end_year = max(years)

            # IMPORTANT: Check if the start_year is accurate for new artists
            if end_year >= current_year - 3:  # Recent artist with releases in last 3 years
                # If earliest release seems too old (over 10 years gap) for recent artist, check more carefully
                if current_year - start_year > 10:
                    self.console_logger.info(f"Artist '{artist}' has recent releases but also old start year {start_year}.")

                    # Check for clusters of years to see if there's a gap suggesting a different artist
                    year_clusters = {}
                    for y in years:
                        decade = y // 10 * 10
                        year_clusters[decade] = year_clusters.get(decade, 0) + 1

                    # Check if most releases are recent (80%+ in last decade)
                    current_decade = current_year // 10 * 10
                    recent_releases = sum(year_clusters.get(decade, 0) for decade in [current_decade, current_decade - 10])

                    if recent_releases >= len(years) * 0.8:
                        # Calculate start year based on recent releases only
                        recent_years = [y for y in years if y >= current_decade - 10]
                        adjusted_start = min(recent_years)
                        self.console_logger.info(f"Most releases are recent - adjusting start year from {start_year} to {adjusted_start}")
                        start_year = adjusted_start

            # If artist is still active (recent releases)
            if end_year >= current_year - 3:
                end_year = None  # Mark as still active

            self.console_logger.info(f"Determined activity period for {artist}: {start_year} - {end_year or 'present'}")

            # Store in cache to avoid repeated API calls
            self.activity_cache[cache_key] = (start_year, end_year)
            return start_year, end_year

        except Exception as e:
            self.error_logger.error(f"Error determining activity period for '{artist}': {e}")
            # If error occurred but the artist appears new, assume recent start
            if appears_new:
                start_year = current_year - 2
                self.console_logger.info(f"Error determining period, but '{artist}' appears to be new. Assuming start year {start_year}")
                self.activity_cache[cache_key] = (start_year, None)
                return start_year, None
            else:
                self.activity_cache[cache_key] = (None, None)
                return None, None

    def _get_artist_region(self, artist: str) -> Optional[str]:
        """
        Attempt to determine the likely region of origin for an artist.

        This is a simple implementation that could be expanded with a database
        of artist regions or API calls to get more accurate data.

        Args:
            artist: Artist name

        Returns:
            Region code (country code) or None if unknown

        Example:
            >>> region = service._get_artist_region("Metallica")
            >>> print(region)  # Returns "us"
        """

        # This could be expanded with a proper database or API call
        # For now, just a simple map of known artists
        async def _get_artist_region(self, artist: str) -> Optional[str]:
            """
            Determine the region (country) of an artist using MusicBrainz API.

            Args:
                artist: Artist name

            Returns:
                ISO country code (lowercase) or None if unknown

            Example:
                >>> region = await service._get_artist_region("Metallica")
                >>> print(region)  # Returns "us"
            """
            # Check cache first
            cache_key = f"region_{artist.lower()}"
            if hasattr(self, 'region_cache') and cache_key in self.region_cache:
                return self.region_cache.get(cache_key)

            # Initialize region cache if it doesn't exist
            if not hasattr(self, 'region_cache'):
                self.region_cache = {}

            try:
                # Query MusicBrainz API for artist data
                artist_encoded = urllib.parse.quote_plus(artist)
                search_url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{artist_encoded}&fmt=json"

                data = await self._make_api_request('musicbrainz', search_url)
                if data and "artists" in data and data["artists"]:
                    # Get the most relevant artist (first result)
                    best_artist = data["artists"][0]

                    # Get country from MusicBrainz
                    country = best_artist.get("country", "").lower()
                    if country:
                        self.console_logger.debug(f"Found region {country} for artist '{artist}' from MusicBrainz")
                        self.region_cache[cache_key] = country
                        return country

                    # Try to get area information as fallback
                    if "area" in best_artist and "iso-3166-1-codes" in best_artist["area"]:
                        country_codes = best_artist["area"]["iso-3166-1-codes"]
                        if country_codes and len(country_codes) > 0:
                            country = country_codes[0].lower()
                            self.region_cache[cache_key] = country
                            return country

            except Exception as e:
                self.error_logger.warning(f"Error retrieving artist region for '{artist}': {e}")

            # Fall back to heuristics if API didn't return a region
            artist_lower = artist.lower()

            # Common name patterns that might indicate regions
            if re.search(r'\b(mc|mac)[a-z]', artist_lower) and "the" not in artist_lower:
                self.region_cache[cache_key] = "gb"  # Scottish/Irish naming pattern
                return "gb"

            if any(suffix in artist_lower for suffix in ["-en", "-on", "-son", "-sen", "sson", "berg"]):
                self.region_cache[cache_key] = "se"  # Nordic naming pattern
                return "se"

            if any(word in artist_lower.split() for word in ["die", "der", "das", "und"]):
                self.region_cache[cache_key] = "de"  # German language
                return "de"

            if artist_lower.endswith("ov") or artist_lower.endswith("sky"):
                self.region_cache[cache_key] = "ru"  # Russian/Slavic naming pattern
                return "ru"

            if "babymetal" in artist_lower or "band-maid" in artist_lower:
                self.region_cache[cache_key] = "jp"  # Known Japanese bands
                return "jp"

            # Default to None (unknown)
            self.region_cache[cache_key] = None
            return None

    def _score_original_release(self, release: Dict[str, Any], artist: str, album: str) -> int:
        """
        Enhanced scoring function with improved weighting for original release indicators.

        Args:
            release: Release data dictionary
            artist: Artist name being searched
            album: Album name being searched

        Returns:
            Score between 0 and 100
        """
        # Get scoring weights from config or use defaults
        scoring_config = self.config.get('year_retrieval', {}).get('scoring', {})

        score = scoring_config.get('default_base_score', 20)
        score_components = []
        current_year = datetime.now().year

        # Check for artist name match
        release_artist = release.get('artist', '').lower()
        artist_norm = artist.lower()

        # Check artist match
        artist_match = False
        if release_artist == artist_norm:
            artist_match = True
            artist_match_bonus = scoring_config.get('artist_match_bonus', 20)
            score += artist_match_bonus
            score_components.append(f"Artist exact match: +{artist_match_bonus}")

        # Album title matching with completely revised approach
        release_title = release.get('title', '').lower()
        album_norm = album.lower()

        # Clean titles for comparison
        def clean_for_comparison(text):
            return re.sub(r'[^\w\s]', '', text.lower()).strip()

        release_title_clean = clean_for_comparison(release_title)
        album_norm_clean = clean_for_comparison(album_norm)

        # EXACT MATCH CHECK - Most important!
        if release_title_clean == album_norm_clean:
            # Massive bonus for exact match
            exact_match_bonus = scoring_config.get('exact_match_bonus', 20) * 7  # Extremely strong weight
            score += exact_match_bonus
            score_components.append(f"EXACT title match: +{exact_match_bonus}")

            # Additional super bonus for perfect artist+album match
            if artist_match:
                perfect_match_bonus = 70  # Very high bonus for perfect match
                score += perfect_match_bonus
                score_components.append(f"PERFECT artist+album match: +{perfect_match_bonus}")
        else:
            # NOT AN EXACT MATCH - Now determine how to treat partial matches

            # CASE 1: Variation with extra info in parentheses/brackets
            # Examples: "Album" vs "Album (Remastered)" or "Album [Deluxe Edition]"
            base_title_pattern = re.escape(album_norm_clean) + r'(\s+[\(\[][^\)\]]+[\)\]])*$'
            if re.match(base_title_pattern, release_title_clean):
                variation_bonus = scoring_config.get('title_variation_bonus', 15)
                score += variation_bonus
                score_components.append(f"Title variation (basic+extras): +{variation_bonus}")

            # CASE 2: Same but reversed - searched album has extra text
            # Examples: "Album (Remastered)" vs "Album"
            base_release_pattern = re.escape(release_title_clean) + r'(\s+[\(\[][^\)\]]+[\)\]])*$'
            if re.match(base_release_pattern, album_norm_clean):
                variation_bonus = scoring_config.get('title_variation_bonus', 15)
                score += variation_bonus
                score_components.append(f"Title variation (release is base): +{variation_bonus}")

            # CASE 3: Short title is substring of long title but not at beginning
            # "Nomad" vs "The Nomad Series" - THIS IS BAD!
            elif len(album_norm_clean) < len(release_title_clean) and album_norm_clean in release_title_clean:
                # Major penalty for being embedded in a longer title
                substring_penalty = -60  # Very severe penalty
                score += substring_penalty
                score_components.append(f"Short title embedded in longer title: {substring_penalty}")

            # CASE 4: Long title is substring of short title but not at beginning
            # "The Nomad Series" vs "Nomad" - Also likely bad
            elif len(release_title_clean) < len(album_norm_clean) and release_title_clean in album_norm_clean:
                substring_penalty = -50
                score += substring_penalty
                score_components.append(f"Release title embedded in search title: {substring_penalty}")

            # CASE 5: No relationship - titles are completely different
            else:
                # Strong penalty for unrelated titles
                unrelated_title_penalty = -70
                score += unrelated_title_penalty
                score_components.append(f"Unrelated titles: {unrelated_title_penalty}")

        # Add bonus for release group first-release match
        if 'releasegroup_first_date' in release and release.get('year') and release['releasegroup_first_date']:
            try:
                rg_year = release['releasegroup_first_date'][:4]
                if rg_year == release['year']:
                    first_release_bonus = scoring_config.get('first_release_date_bonus', 30)
                    score += first_release_bonus
                    score_components.append(f"Release group first date match: +{first_release_bonus}")
            except (IndexError, ValueError):
                pass

        # Add country preference score
        if 'country_score' in release and release['country_score'] > 0:
            score += release['country_score']
            score_components.append(f"Preferred country ({release.get('country')}): +{release['country_score']}")

        # Release status with expanded types
        status = (release.get('status') or '').lower()

        if status == 'official':
            official_bonus = scoring_config.get('official_status_bonus', 15)
            score += official_bonus
            score_components.append(f"Official status: +{official_bonus}")
        elif status == 'bootleg':
            bootleg_penalty = scoring_config.get('bootleg_penalty', -40)
            score += bootleg_penalty
            score_components.append(f"Bootleg: {bootleg_penalty}")
        elif status == 'promo':
            promo_penalty = scoring_config.get('promo_penalty', -20)
            score += promo_penalty
            score_components.append(f"Promo: {promo_penalty}")
        elif status == 'unauthorized':
            unauthorized_penalty = scoring_config.get('unauthorized_penalty', -35)
            score += unauthorized_penalty
            score_components.append(f"Unauthorized: {unauthorized_penalty}")

        # Enhanced reissue detection
        # 1. Check explicit reissue flag
        if release.get('is_reissue', False):
            reissue_penalty = scoring_config.get('reissue_penalty', -40)
            score += reissue_penalty
            score_components.append(f"Explicit reissue flag: {reissue_penalty}")

        # Year validation relative to artist's career
        year_str = release.get('year', '')
        if year_str:
            try:
                year = int(year_str)

                # Check for future dates (impossible for original release)
                if year > current_year:
                    future_penalty = scoring_config.get('future_date_penalty', -50)
                    score += future_penalty
                    score_components.append(f"Future date: {future_penalty}")
                    return max(0, score)  # Immediately return with a low score

                # Check if album title contains year and if it matches release year
                year_in_album = re.search(r'\b(19\d\d|20\d\d)\b', album_norm)
                if year_in_album and int(year_in_album.group(1)) == year:
                    matching_year_bonus = 25
                    score += matching_year_bonus
                    score_components.append(f"Year in album title matches release year: +{matching_year_bonus}")

                # Use the artist period context if available
                if self.artist_period_context:
                    start_year = self.artist_period_context.get('start_year')
                    end_year = self.artist_period_context.get('end_year')

                    # Verify lifespan consistency with release date
                    if start_year and end_year:  # If we have complete lifespan data
                        # If release came out after official end + 2 years (allowing for posthumous releases)
                        if year > end_year + 2:
                            lifespan_inconsistency = -40
                            score += lifespan_inconsistency
                            score_components.append(f"Release after career end: {lifespan_inconsistency}")

                        # If release came out before official start - 1 year (allowing for early demos)
                        if year < start_year - 1:
                            lifespan_inconsistency = -30
                            score += lifespan_inconsistency
                            score_components.append(f"Release before career start: {lifespan_inconsistency}")

                    # Standard artist period checks (modified to be more robust)
                    if start_year:
                        # For new artists (started after 2020)
                        if start_year >= 2020:
                            if year >= 2022:
                                # For new artists, very recent years are more likely correct
                                new_artist_recent_year_bonus = 30
                                score += new_artist_recent_year_bonus
                                score_components.append(f"Recent year for new artist: +{new_artist_recent_year_bonus}")
                            elif year < 2018:
                                # For new artists, old years are likely incorrect
                                new_artist_old_year_penalty = -40
                                score += new_artist_old_year_penalty
                                score_components.append(f"Old year for new artist: {new_artist_old_year_penalty}")
                        else:
                            # Regular artist activity period logic
                            years_after_start = year - start_year
                            if years_after_start == 0:
                                earliest_year_bonus = scoring_config.get('earliest_year_bonus', 30)
                                score += earliest_year_bonus
                                score_components.append(f"Earliest possible year: +{earliest_year_bonus}")
                            elif years_after_start <= 2:
                                early_release_bonus = scoring_config.get('early_release_bonus', 20)
                                score += early_release_bonus
                                score_components.append(f"Early career release: +{early_release_bonus}")
                            elif years_after_start <= 5:
                                moderate_bonus = 10
                                score += moderate_bonus
                                score_components.append(f"Early-mid career: +{moderate_bonus}")
                            else:
                                # Later release penalty moderated by artist longevity
                                artist_longevity = (end_year or current_year) - start_year
                                if artist_longevity > 15:  # Long-term artist
                                    late_release_penalty = min(15, years_after_start)
                                else:  # Shorter-term artist
                                    late_release_penalty = min(30, years_after_start * 2)
                                score -= late_release_penalty
                                score_components.append(f"Late career: -{late_release_penalty}")
            except ValueError:
                pass

        # Log score components
        for component in score_components:
            self.console_logger.debug(f"Score component - {component}")

        # Log the final score calculation
        self.console_logger.info(f"Final score for {release.get('title')} ({year_str}): {score} from {len(score_components)} factors")

        return max(0, min(score, 100))  # Limit to 0-100 range

    async def _get_scored_releases_from_musicbrainz(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from MusicBrainz with enhanced focus on original release dates.

        Args:
            artist: Artist name
            album: Album name

        Returns:
            List of scored release dictionaries with improved release group date handling
        """
        scored_releases: List[Dict[str, Any]] = []
        try:
            artist_encoded = urllib.parse.quote_plus(artist)
            album_encoded = urllib.parse.quote_plus(album)

            # First try release group search
            search_url = f"https://musicbrainz.org/ws/2/release-group/?query=artist:{artist_encoded} AND releasegroup:{album_encoded}&fmt=json"

            data = await self._make_api_request('musicbrainz', search_url)
            if not data:
                self.console_logger.warning(f"MusicBrainz release group search failed for {artist} - {album}")
                return []

            release_groups = data.get("release-groups", [])

            # Process release groups - we want to capture their first-release-date
            release_group_data = {}
            for rg in release_groups:
                rg_id = rg.get("id")
                if rg_id and "first-release-date" in rg:
                    release_group_data[rg_id] = {
                        "first-release-date": rg.get("first-release-date"),
                        "primary-type": rg.get("primary-type", ""),
                        "title": rg.get("title", ""),
                    }

            # Now search for the actual releases
            if release_groups:
                # Found release groups, get releases for each group
                self.console_logger.info(f"Found {len(release_groups)} release groups for {artist} - {album}")

                for release_group in release_groups[:3]:  # Limit to top 3 groups
                    release_group_id = release_group.get("id")

                    # Get all releases for this group
                    releases_url = f"https://musicbrainz.org/ws/2/release?release-group={release_group_id}&fmt=json"
                    releases_data = await self._make_api_request('musicbrainz', releases_url)

                    if not releases_data:
                        self.console_logger.warning(f"Failed to get releases for group {release_group_id}")
                        continue

                    releases = releases_data.get("releases", [])
                    if not releases:
                        continue

                    # Log useful information about the release group
                    primary_type = release_group.get("primary-type", "Unknown")
                    first_release_date = release_group.get("first-release-date", "Unknown")
                    self.console_logger.info(f"Release group info - Type: {primary_type}, First release date: {first_release_date}")

                    # Process each release and add release group first-release-date
                    for release in releases:
                        # Extract year from date
                        year = None
                        release_date = release.get("date", "")
                        if release_date:
                            year_match = re.search(r'^(\d{4})', release_date)
                            if year_match:
                                year = year_match.group(1)

                        # Check for country preferences
                        country = release.get("country", "")
                        country_score = 0
                        preferred_countries = self.config.get("year_retrieval", {}).get("preferred_countries", ["US", "UK", "GB"])

                        # Get scoring configuration
                        scoring_config = self.config.get('year_retrieval', {}).get('scoring', {})

                        country_score = 0
                        if country.lower() == "us":
                            country_score = scoring_config.get('us_country_bonus', 10)
                        elif country.lower() in ["uk", "gb"]:
                            country_score = scoring_config.get('uk_country_bonus', 8)
                        elif country.lower() == "jp":
                            country_score = scoring_config.get('japan_country_bonus', 5)
                        else:
                            # For other countries with preferred_countries
                            if country in preferred_countries:
                                country_score = scoring_config.get('preferred_country_bonus', 3)

                        # Create A Standardized Release Data Structure for Scoring
                        # Obtaining a full list of keywords from config
                        reissue_keywords = self.config.get('year_retrieval', {}).get('reissue_keywords', [])
                        is_reissue = any(kw.lower() in release.get('title', '').lower() for kw in reissue_keywords)
                        release_data = {
                            'source': 'musicbrainz',
                            'title': release.get('title', ''),
                            'type': release.get('primary-type', '') or release_group.get('primary-type', ''),
                            'status': release.get('status', ''),
                            'year': year,
                            'country': country,
                            'packaging': release.get('packaging', ''),
                            'is_reissue': is_reissue,
                            'releasegroup_id': release_group_id,
                            'releasegroup_first_date': first_release_date,
                            'country_score': country_score,
                            'score': 0,
                        }

                        # Only score if we have a year
                        if release_data['year']:
                            # Add extra points if this is from the first release date of the release group
                            if first_release_date and first_release_date.startswith(release_data['year']):
                                release_data['original_release_bonus'] = 25
                            else:
                                release_data['original_release_bonus'] = 0

                            release_data['score'] = self._score_original_release(release_data, artist, album)
                            scored_releases.append(release_data)

                # Sort by score
                scored_releases.sort(key=lambda x: x['score'], reverse=True)

                # Log top results for debugging
                self.console_logger.info(f"Scored {len(scored_releases)} results from MusicBrainz")
                for i, r in enumerate(scored_releases[:3]):
                    self.console_logger.info(
                        f"MB #{i+1}: {r['title']} ({r['year']}) - Status: {r['status']}, Country: {r['country']}, Score: {r['score']}"
                    )

                return scored_releases
            else:
                # If no release groups, fall back to direct release search
                search_url = f"https://musicbrainz.org/ws/2/release/?query=artist:{artist_encoded} AND release:{album_encoded}&fmt=json"
                data = await self._make_api_request('musicbrainz', search_url)

                if not data:
                    self.console_logger.warning(f"MusicBrainz release search failed for {artist} - {album}")
                    return []

                releases = data.get("releases", [])

                if not releases:
                    self.console_logger.info(f"No results from MusicBrainz for '{artist} - {album}'")
                    return []

                # Process releases without release group data
                for release in releases:
                    # Extract year from date
                    year = None
                    release_date = release.get("date", "")
                    if release_date:
                        year_match = re.search(r'^(\d{4})', release_date)
                        if year_match:
                            year = year_match.group(1)

                    # Create release data structure
                    release_data = {
                        'source': 'musicbrainz',
                        'title': release.get('title', ''),
                        'type': release.get('primary-type', ''),
                        'status': release.get('status', ''),
                        'year': year,
                        'country': release.get('country', ''),
                        'packaging': release.get('packaging', ''),
                        'is_reissue': False,  # Can't determine from basic search
                        'score': 0,
                    }

                    # Only score if we have a year
                    if release_data['year']:
                        release_data['score'] = self._score_original_release(release_data, artist, album)
                        scored_releases.append(release_data)

                # Sort by score
                scored_releases.sort(key=lambda x: x['score'], reverse=True)

                # Log results
                self.console_logger.info(f"Scored {len(scored_releases)} results from MusicBrainz basic search")

                return scored_releases

        except Exception as e:
            self.error_logger.error(f"Error retrieving year from MusicBrainz for '{artist} - {album}': {e}")
            return []

    async def _get_scored_releases_from_discogs(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from Discogs.

        Args:
            artist: Artist name
            album: Album name

        Returns:
            List of scored release dictionaries
        """
        scored_releases: List[Dict[str, Any]] = []
        try:
            artist_encoded = urllib.parse.quote_plus(artist)
            album_encoded = urllib.parse.quote_plus(album)

            search_url = f"https://api.discogs.com/database/search?artist={artist_encoded}&release_title={album_encoded}&type=release"
            self.console_logger.info(f"Discogs query: {search_url}")

            data = await self._make_api_request('discogs', search_url)
            if not data:
                return []

            results = data.get('results', [])

            if not results:
                self.console_logger.info(f"No results from Discogs for '{artist} - {album}'")
                return []

            self.console_logger.info(f"Found {len(results)} potential matches from Discogs")

            # Process the results
            for item in results:
                title = item.get('title', '')
                is_reissue = any(kw.lower() in title.lower() for kw in self.config.get('year_retrieval', {}).get('reissue_keywords', []))

                formats = item.get('format', [])
                formats_str = ', '.join(formats) if isinstance(formats, list) else str(formats)

                # Extract detailed format information
                format_details = []
                if 'format_quantity' in item:
                    format_details.append(f"{item['format_quantity']}x{formats_str}")
                else:
                    format_details.append(formats_str)

                # Check for special labels or flags
                formats_tags = item.get('formats', [])
                special_flags = []

                for fmt in formats_tags:
                    if isinstance(fmt, dict):
                        if 'descriptions' in fmt:
                            special_flags.extend(fmt['descriptions'])

                # Log detailed information about the release
                self.console_logger.info(
                    f"Discogs release: {title} ({item.get('year', 'Unknown')}) - "
                    + f"Country: {item.get('country', 'Unknown')}, Format: {formats_str}"
                    + (f", Tags: {', '.join(special_flags)}" if special_flags else "")
                )

                # Determine if this is likely a reissue based on format details
                format_based_reissue = any(
                    kw.lower() in ' '.join(special_flags).lower() for kw in self.config.get('year_retrieval', {}).get('reissue_keywords', [])
                )

                # Sometimes reissues are marked in the comments
                comments = item.get('comment', '')
                comments_suggest_reissue = False
                if comments and isinstance(comments, str):
                    comments_suggest_reissue = any(
                        kw.lower() in comments.lower() for kw in self.config.get('year_retrieval', {}).get('reissue_keywords', [])
                    )

                # Create release data object
                release_data = {
                    'source': 'discogs',
                    'title': title,
                    'type': item.get('format', [''])[0] if item.get('format') else '',
                    'status': 'official',  # Discogs default
                    'year': str(item.get('year', '')),
                    'country': item.get('country', ''),
                    'format': formats_str,
                    'is_reissue': is_reissue or format_based_reissue or comments_suggest_reissue,
                    'format_details': ' '.join(format_details),
                    'special_flags': special_flags,
                    'score': 0,
                }

                if release_data['year']:
                    release_data['score'] = self._score_original_release(release_data, artist, album)
                    scored_releases.append(release_data)

            # Sort by score
            scored_releases.sort(key=lambda x: x['score'], reverse=True)

            # Log top results
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"Discogs #{i+1}: {r['title']} ({r['year']}) - "
                    + f"Country: {r.get('country', 'Unknown')}, "
                    + f"Format: {r.get('format', 'Unknown')}, "
                    + f"Is Reissue: {r.get('is_reissue')}, "
                    + f"Score: {r['score']}"
                )

            self.console_logger.info(f"Scored {len(scored_releases)} results from Discogs")
            return scored_releases

        except Exception as e:
            self.error_logger.error(f"Error retrieving year from Discogs for '{artist} - {album}': {e}")
            return []

    async def _get_year_from_lastfm(self, artist: str, album: str) -> Dict[str, Any]:
        """
        Retrieve album release year from Last.fm API.

        This method queries the Last.fm API for album information and extracts
        release year data from various sources within the response (wiki, tags, etc.)

        Args:
            artist: Artist name
            album: Album name

        Returns:
            Dictionary with year and metadata about the source/confidence

        Example:
            >>> result = await service._get_year_from_lastfm("Septicflesh", "Sumerian Daemons")
            >>> print(f"Year: {result.get('year')}, Source: {result.get('source')}")
        """
        if not self.lastfm_api_key:
            return {}

        try:
            self.console_logger.info(f"Searching Last.fm for album year: '{artist} - {album}'")

            # Get permission from rate limiter (this will wait if needed)
            wait_time = await self.rate_limiters['lastfm'].acquire()
            if wait_time > 0.5:
                self.console_logger.debug(f"Waited {wait_time:.2f}s for Last.fm rate limiting")

            # Track request count
            self.request_counts['lastfm'] = self.request_counts.get('lastfm', 0) + 1

            url = "http://ws.audioscrobbler.com/2.0/"
            params = {
                "method": "album.getInfo",
                "artist": artist,
                "album": album,
                "api_key": self.lastfm_api_key,
                "format": "json",
            }

            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.console_logger.warning(f"Last.fm API error: {response.status} - {error_text[:200]}")
                    return {}

                data = await response.json()

                if "error" in data:
                    self.console_logger.warning(f"Last.fm API error: {data.get('message', 'Unknown error')}")
                    return {}

                return self._extract_year_from_lastfm_data(data, artist, album)

        except Exception as e:
            self.error_logger.error(f"Error getting year from Last.fm for '{artist} - {album}': {e}")
            return {}
        finally:
            # Always release the rate limiter semaphore
            if 'lastfm' in self.rate_limiters:
                self.rate_limiters['lastfm'].release()

    def _extract_year_from_lastfm_data(self, data: Dict[str, Any], artist: str, album: str) -> Dict[str, Any]:
        """
        Extract year from Last.fm API response.

        Examines multiple potential sources of information in the API:
        1. Wiki text (most reliable)
        2. Tags (moderately reliable)
        3. Wiki published date (less reliable)

        Args:
            data: Last.fm API response data
            artist: Artist name (for logging)
            album: Album name (for logging)

        Returns:
            Dictionary with year and metadata

        Example:
            >>> lastfm_data = await session.get(url).json()
            >>> year_info = service._extract_year_from_lastfm_data(lastfm_data, "Septicflesh", "Sumerian Daemons")
            >>> if year_info.get('year') == "2003":
            >>>     print("Correct year found!")
        """
        result = {}
        confidence = 0
        source = "Unknown"

        # If there's no "album" key - no data
        if "album" not in data:
            self.console_logger.warning(f"No album data found in Last.fm response for '{artist} - {album}'")
            return {}

        album_data = data["album"]

        # Attempt 1: Check wiki content - with proper type checks
        if "wiki" in album_data and isinstance(album_data["wiki"], dict) and "content" in album_data["wiki"]:

            wiki_content = album_data["wiki"]["content"]
            year_from_wiki = self._extract_year_from_text(wiki_content)

            if year_from_wiki:
                result["year"] = year_from_wiki
                result["confidence"] = 70  # High confidence for wiki data
                result["source"] = "wiki"
                self.console_logger.info(f"Found year {year_from_wiki} in Last.fm wiki for '{artist} - {album}'")
                return result

        # Attempt 2: Check tags for release year - with proper type checks
        if "tags" in album_data and isinstance(album_data["tags"], dict) and "tag" in album_data["tags"]:

            # Tags can be either a single dict or a list of dicts
            tags_data = album_data["tags"]["tag"]

            # Convert to list if it's a single dict
            if isinstance(tags_data, dict):
                tags_data = [tags_data]

            if isinstance(tags_data, list):
                year_tags = []

                for tag in tags_data:
                    if isinstance(tag, dict) and "name" in tag:
                        tag_name = tag["name"].lower()
                        # Look for tags that look like years (1990-2030)
                        year_match = re.search(r"\b(19[89]\d|20[0-2]\d)\b", tag_name)
                        if year_match:
                            potential_year = year_match.group(1)
                            year_tags.append(potential_year)

                if year_tags:
                    # Use the most popular year in tags
                    year_counts = {}
                    for year in year_tags:
                        year_counts[year] = year_counts.get(year, 0) + 1

                    # Sort by count, then by year (earlier years take priority)
                    sorted_years = sorted(year_counts.items(), key=lambda x: (-x[1], x[0]))
                    best_year = sorted_years[0][0]

                    result["year"] = best_year
                    result["confidence"] = 60  # Medium confidence for tag data
                    result["source"] = "tags"
                    self.console_logger.info(f"Found year {best_year} in Last.fm tags for '{artist} - {album}'")
                    return result

        # Attempt 3: Check wiki published date - with proper type checks
        if "wiki" in album_data and isinstance(album_data["wiki"], dict) and "published" in album_data["wiki"]:

            published = album_data["wiki"]["published"]
            # Wiki published date can be a rough indicator of when the album already existed
            year_match = re.search(r"\b(19[89]\d|20[0-2]\d)\b", published)
            if year_match:
                wiki_year = year_match.group(1)
                result["year"] = wiki_year
                result["confidence"] = 40  # Low confidence for published date
                result["source"] = "wiki_published"
                self.console_logger.info(f"Found year {wiki_year} from Last.fm wiki published date for '{artist} - {album}'")
                return result

        # Attempt 4: Check for release date attribute directly
        if "releasedate" in album_data and album_data["releasedate"]:
            date_text = album_data["releasedate"]
            year_match = re.search(r"\b(19[89]\d|20[0-2]\d)\b", date_text)
            if year_match:
                release_year = year_match.group(1)
                result["year"] = release_year
                result["confidence"] = 75  # High confidence for direct release date
                result["source"] = "releasedate"
                self.console_logger.info(f"Found year {release_year} from Last.fm direct release date for '{artist} - {album}'")
                return result

        # Nothing found
        self.console_logger.warning(f"No year information found in Last.fm data for '{artist} - {album}'")
        return {}

    def _extract_year_from_text(self, text: str) -> Optional[str]:
        """
        Extract release year from text by looking for common date patterns.

        Args:
            text: Text to search for year

        Returns:
            Year as string or None if not found

        Example:
            >>> year = service._extract_year_from_text("The album was released in 2003 via...")
            >>> print(year)  # "2003"
        """
        # Pattern 1: "released in YYYY" or "released on DD Month YYYY"
        release_match = re.search(r"released\s+(?:in|on)?\s+(?:\d+\s+\w+\s+)?(\d{4})", text, re.IGNORECASE)
        if release_match:
            return release_match.group(1)

        # Pattern 2: "YYYY release" or "a YYYY album"
        release_match_2 = re.search(r"\b(19[89]\d|20[0-2]\d)\s+(?:release|album)\b", text, re.IGNORECASE)
        if release_match_2:
            return release_match_2.group(1)

        # Pattern 3: date in "DD Month YYYY" or "Month YYYY" format
        date_match = re.search(
            r"\b(?:\d{1,2}\s+)?(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
            text,
            re.IGNORECASE,
        )
        if date_match:
            return date_match.group(1)

        # Pattern 4: year in parentheses, which often indicates release date
        year_match = re.search(r"\((\d{4})\)", text)
        if year_match:
            return year_match.group(1)

        # No matches
        return None

    def _score_lastfm_result(self, release: Dict[str, Any], artist: str, album: str) -> int:
        """
        Calculate score for a Last.fm result based on multiple factors.

        Args:
            release: Release data from Last.fm
            artist: Artist name
            album: Album name

        Returns:
            Score from 0 to 100

        Example:
            >>> release_data = {"year": "2003", "confidence": 70, "source": "wiki"}
            >>> score = service._score_lastfm_result(release_data, "Septicflesh", "Sumerian Daemons")
            >>> print(f"Last.fm data score: {score}/100")
        """
        # Get scoring weights from config
        scoring_config = self.config.get('year_retrieval', {}).get('scoring', {})

        # Initial score based on confidence from extraction
        confidence = release.get('confidence', 50)
        score = confidence

        # Add bonuses or penalties based on data source
        source = release.get('source_detail', 'Unknown')

        if source == 'wiki':
            # Wiki is the most reliable source on Last.fm
            wiki_bonus = scoring_config.get('lastfm_wiki_bonus', 20)
            score += wiki_bonus
        elif source == 'tags':
            # Tags can be less reliable
            tags_bonus = scoring_config.get('lastfm_tags_bonus', 10)
            score += tags_bonus
        elif source == 'wiki_published':
            # Wiki published date is the least reliable source
            wiki_date_bonus = scoring_config.get('lastfm_wiki_date_bonus', 5)
            score += wiki_date_bonus

        # Check year relative to artist's career period
        year_str = release.get('year', '')
        if year_str and self.artist_period_context:
            try:
                year = int(year_str)
                start_year = self.artist_period_context.get('start_year')

                # Bonus for years falling within activity period
                if start_year and start_year <= year:
                    activity_bonus = scoring_config.get('active_period_bonus', 20)
                    score += activity_bonus

                    # Extra bonus for early releases
                    years_after_start = year - start_year
                    if years_after_start <= 3:
                        early_release_bonus = scoring_config.get('early_release_bonus', 10)
                        score += early_release_bonus

                # Penalty for years before career start
                if start_year and year < start_year:
                    pre_career_penalty = 15
                    score -= pre_career_penalty

                # Check plausibility of year
                current_year = datetime.now().year
                if year > current_year:
                    # Future year - impossible for old release
                    future_penalty = scoring_config.get('future_date_penalty', 50)
                    score -= future_penalty
                elif year >= current_year - 2:
                    # Very recent releases - less reliable
                    recent_penalty = scoring_config.get('recent_release_penalty', 10)
                    score -= recent_penalty
            except ValueError:
                pass

        # Limit score to 0-100 range
        return max(0, min(score, 100))

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a name by removing common suffixes and extra characters for better API matching.

        Args:
            name (str): The original name.

        Returns:
            str: Normalized name.

        Example:
            >>> service._normalize_name("Agalloch - The White EP")
            'Agalloch - The White'
        """
        suffixes = [' - EP', ' - Single', ' EP', ' (Deluxe)', ' (Remastered)']
        result = name
        for suffix in suffixes:
            if result.endswith(suffix):
                result = result[: -len(suffix)]
        result = re.sub(r'[^\w\s]', ' ', result)
        result = re.sub(r'\s+', ' ', result).strip()
        return result

    async def _get_artist_aliases(self, artist_id: str) -> List[str]:
        """
        Get list of aliases (alternative names) for an artist from MusicBrainz.

        Args:
            artist_id: MusicBrainz artist ID

        Returns:
            List of alias names in lowercase

        Example:
            >>> aliases = await service._get_artist_aliases("123e4567-e89b-12d3-a456-426614174000")
            >>> print(aliases)  # ["septic flesh", "s.f.", ...]
        """
        aliases = []
        try:
            url = f"https://musicbrainz.org/ws/2/artist/{artist_id}?inc=aliases&fmt=json"
            data = await self._make_api_request('musicbrainz', url)

            if data and "aliases" in data:
                for alias in data["aliases"]:
                    if "name" in alias:
                        aliases.append(alias["name"].lower())

            return aliases
        except Exception as e:
            self.error_logger.error(f"Error getting aliases for artist {artist_id}: {e}")
            return []

    def _is_valid_year(self, year_str: str, score: int) -> bool:
        """
        Validates whether a given year string represents a sensible release year.

        Args:
            year_str (str): Year as a string.
            score (int): Confidence score for the year.

        Returns:
            bool: True if valid, False otherwise.

        Example:
            >>> service._is_valid_year("1999", 80)
            True
        """
        try:
            year = int(year_str)
            if year < self.min_valid_year or year > self.current_year:
                return False
            if score < self.trust_threshold:
                current_year = datetime.now().year
                if year < 1950 or year > current_year:
                    return False
            return True
        except (ValueError, TypeError):
            return False


def _clean_activity_cache(self):
    """
    Clean expired entries from the activity cache.
    """
    cache_ttl_days = self.config.get('year_retrieval', {}).get('cache_ttl_days', 365)
    cache_ttl_seconds = cache_ttl_days * 86400
    current_time = time.time()
    expired_keys = []

    for key, ((_, _), timestamp) in self.activity_cache.items():
        if current_time - timestamp > cache_ttl_seconds:
            expired_keys.append(key)

    for key in expired_keys:
        del self.activity_cache[key]

    if expired_keys:
        self.console_logger.info(f"Cleaned {len(expired_keys)} expired entries from activity cache")
