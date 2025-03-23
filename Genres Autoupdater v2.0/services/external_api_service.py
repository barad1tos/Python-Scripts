#!/usr/bin/env python3
"""
Enhanced ExternalApiService for Music Metadata Retrieval

This module provides an optimized service for interacting with external APIs (MusicBrainz and Discogs)
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
    ...         year = await service.get_album_year("Agalloch", "The White EP", "2008")
    ...         print(f"Original release year: {year}")
    ...     finally:
    ...         await service.close()
    >>> 
    >>> asyncio.run(get_year())
"""

import asyncio
import logging
import re
import time
import urllib.parse

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp


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
    
    def __init__(self, 
                    requests_per_window: int, 
                    window_size: float,
                    max_concurrent: int = 3, 
                    logger: Optional[logging.Logger] = None):
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
            "max_requests_per_window": self.requests_per_window
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
        
        # Initialize enhanced rate limiters
        self.rate_limiters = {
            'discogs': EnhancedRateLimiter(
                requests_per_window=year_config.get("discogs_requests_per_minute", 25),
                window_size=60.0,  # 1 minute window
                max_concurrent=year_config.get("concurrent_api_calls", 3),
                logger=console_logger
            ),
            'musicbrainz': EnhancedRateLimiter(
                requests_per_window=year_config.get("musicbrainz_requests_per_second", 1),
                window_size=1.0,  # 1 second window
                max_concurrent=year_config.get("concurrent_api_calls", 3),
                logger=console_logger
            )
        }
        
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
        self.activity_cache: Dict[str, Tuple[Optional[int], Optional[int]]] = {}
        self.artist_period_context: Optional[Dict[str, Optional[int]]] = None
        
        # Stats tracking
        self.request_counts = {
            'discogs': 0,
            'musicbrainz': 0
        }

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
                limit=20,          # Max connections (higher than default)
                ssl=False,         # Skip SSL verification for performance
                use_dns_cache=True,
                ttl_dns_cache=300  # Cache DNS results for 5 minutes
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
            
            self.console_logger.info(f"API Stats - Discogs: {discogs_stats['total_requests']} requests, "
                                        f"avg wait: {discogs_stats['avg_wait_time']:.2f}s")
            self.console_logger.info(f"API Stats - MusicBrainz: {mb_stats['total_requests']} requests, "
                                        f"avg wait: {mb_stats['avg_wait_time']:.2f}s")
            
            await self.session.close()
            self.console_logger.info("External API session closed")

    async def _make_api_request(self, 
                                api_name: str,
                                url: str,
                                headers: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """
        Make an API request with rate limiting and error handling.
        
        Args:
            api_name: API name for rate limiting ('musicbrainz' or 'discogs')
            url: Full URL to request
            headers: Request headers
            
        Returns:
            Response data as dictionary or None if request failed
            
        Example:
            >>> data = await service._make_api_request('musicbrainz', 'https://musicbrainz.org/ws/2/artist/...')
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
                
                # Handle errors
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
            self.error_logger.warning(f"{api_name.title()} request timed out: {url}")
            return None
        except Exception as e:
            self.error_logger.error(f"Error making {api_name} request: {e}")
            return None
        finally:
            # Always release the rate limiter semaphore
            limiter.release()

    async def get_album_year(self, artist: str, album: str, current_library_year: Optional[str] = None) -> Optional[str]:
        """
        Determine the original release year for an album using optimized API calls.
        
        This method queries both MusicBrainz and Discogs concurrently, clusters the results,
        and selects the most appropriate year.
        
        Args:
            artist: Artist name
            album: Album name
            current_library_year: Year currently in the library (if any)
            
        Returns:
            The determined original release year as a string, or None if not found
            
        Example:
            >>> year = await service.get_album_year("Vildhjarta", "Thousands of Evils", "2013")
            >>> print(f"Original release year: {year}")
        """
        # Normalize inputs to improve matching
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)
        
        self.console_logger.info(f"Searching for original release year: '{artist_norm} - {album_norm}' (current: {current_library_year or 'none'})")
        
        # Special handling for very recent/future releases
        current_year = datetime.now().year
        next_year = current_year + 1
            
        if current_library_year:
            try:
                library_year_int = int(current_library_year)
                # If the library year is current or future year, trust it
                if library_year_int >= current_year:
                    self.console_logger.info(f"Current library year {current_library_year} indicates a very recent or upcoming release. Trusting library data.")
                    return current_library_year
            except ValueError:
                pass

        # Get artist's activity period for better year evaluation
        start_year, end_year = await self.get_artist_activity_period(artist_norm)
        
        # Set artist period context for this request
        self.artist_period_context = {
            'start_year': start_year,
            'end_year': end_year
        }
        
        self.console_logger.info(f"Artist activity period: {start_year or 'unknown'} - {end_year or 'present'}")
        
        try:
            # Make API calls concurrently - this is a key optimization
            api_tasks = [
                self._get_scored_releases_from_musicbrainz(artist_norm, album_norm),
                self._get_scored_releases_from_discogs(artist_norm, album_norm)
            ]
            
            # Wait for both API calls to complete
            results = await asyncio.gather(*api_tasks, return_exceptions=True)
            
            # Process results
            musicbrainz_data = results[0] if isinstance(results[0], list) else []
            discogs_data = results[1] if len(results) > 1 and isinstance(results[1], list) else []
            
            # Combine releases from both sources
            all_releases = []
            if musicbrainz_data:
                all_releases.extend(musicbrainz_data)
            if discogs_data:
                all_releases.extend(discogs_data)
            
            # Nothing found
            if not all_releases:
                self.console_logger.info(f"No release data found for '{artist} - {album}'")
                return current_library_year  # Return existing year if no data found
            
            # Cluster releases by years
            year_clusters = {}
            
            for release in all_releases:
                year = release.get('year')
                if not year:
                    continue
                    
                # Cluster releases by year
                if year not in year_clusters:
                    year_clusters[year] = []
                year_clusters[year].append(release)
            
            # Log year clusters
            self.console_logger.info(f"Year clusters: {', '.join(sorted(year_clusters.keys()))}")
            
            # Calculate total score for each year
            year_scores = {}
            for year, releases in year_clusters.items():
                # Take top-3 releases by score for each year
                top_releases = sorted(releases, key=lambda x: x.get('score', 0), reverse=True)[:3]
                
                # Total score is the sum of scores of top-3 releases
                year_scores[year] = sum(r.get('score', 0) for r in top_releases)
                
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
                            return sorted_years[0][0]
                        
                        return current_library_year
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
                            return earliest_valid_year
                
                self.console_logger.info(f"Selected best year by score: {best_year} (score: {best_score})")
                
                # Final check: if the selected year is much newer than the current library year,
                # and the current library year scores reasonably well, stick with the library year
                if current_library_year and best_year != current_library_year:
                    try:
                        best_year_int = int(best_year)
                        current_year_int = int(current_library_year)
                        
                        # If current year is earlier and scores at least 70% of best year
                        if (current_year_int < best_year_int and 
                            year_scores.get(current_library_year, 0) >= best_score * 0.7):
                            
                            # If significant difference (>3 years) and current year is valid
                            if (best_year_int - current_year_int > 3 and 
                                self._is_valid_year(current_library_year, 60)):
                                
                                self.console_logger.info(
                                    f"Keeping current library year {current_library_year} instead of newer {best_year} " +
                                    f"(scores: {year_scores.get(current_library_year, 0)} vs {best_score})"
                                )
                                return current_library_year
                    except ValueError:
                        pass
                        
                return best_year
            
            return current_library_year  # Return existing year if processing failed
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

        Example:
            >>> start, end = await service.get_artist_activity_period("Agalloch")
            >>> print(start, end)  # e.g., 1995, 2016
        """
        # Check cache first to avoid redundant API calls
        cache_key = f"activity_{artist.lower()}"
        
        # Return cached data if available
        if cache_key in self.activity_cache:
            return self.activity_cache[cache_key]
        
        self.console_logger.info(f"Determining activity period for artist: {artist}")
        
        try:
            # Search for the artist first
            artist_encoded = urllib.parse.quote_plus(artist)
            search_url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{artist_encoded}&fmt=json"
            
            data = await self._make_api_request('musicbrainz', search_url)
            if not data:
                self.console_logger.warning(f"Cannot determine activity period for {artist}: Artist not found")
                self.activity_cache[cache_key] = (None, None)
                return None, None
                
            artists = data.get("artists", [])
            
            if not artists:
                self.console_logger.warning(f"No artist found for query: {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None
                
            # Take first artist match
            artist_id = artists[0].get("id")
            
            # Check if the API provides lifespan directly
            begin_date = artists[0].get("life-span", {}).get("begin")
            end_date = artists[0].get("life-span", {}).get("end")
            ended = artists[0].get("life-span", {}).get("ended")
            
            # If we have complete lifespan data, use it
            if begin_date:
                try:
                    start_year = int(begin_date.split("-")[0])
                    end_year = int(end_date.split("-")[0]) if end_date else None
                    
                    self.console_logger.info(f"Found artist lifespan: {start_year} - {end_year or 'present'}")
                    self.activity_cache[cache_key] = (start_year, end_year)
                    return start_year, end_year
                except (ValueError, IndexError):
                    # If there's an error parsing the dates, fallback to releases
                    pass
            
            # Get release groups for this artist as a fallback
            releases_url = f"https://musicbrainz.org/ws/2/release-group?artist={artist_id}&fmt=json"
            
            data = await self._make_api_request('musicbrainz', releases_url)
            if not data:
                self.console_logger.warning(f"Failed to get releases for {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None
                
            release_groups = data.get("release-groups", [])
            
            if not release_groups:
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
                self.console_logger.warning(f"No release years found for {artist}")
                self.activity_cache[cache_key] = (None, None)
                return None, None
                
            # Find the earliest and latest years
            start_year = min(years)
            end_year = max(years)
            
            # Check if the latest year is recent,
            # in which case the artist might still be active
            current_year = datetime.now().year
            if end_year >= current_year - 3:  # Consider still active if released in last 3 years
                end_year = None  # None indicates "still active"
            
            self.console_logger.info(f"Determined activity period for {artist}: {start_year} - {end_year or 'present'}")
            
            # Store in cache to avoid repeated API calls
            self.activity_cache[cache_key] = (start_year, end_year)
            return start_year, end_year
                
        except Exception as e:
            self.error_logger.error(f"Error determining activity period for '{artist}': {e}")
            self.activity_cache[cache_key] = (None, None)
            return None, None

    def _score_original_release(self, release: Dict[str, Any], artist: str, album: str) -> int:
        """
        Compute a score (0-100) for a release based on several factors:
        - Basic starting score.
        - Album title matching.
        - Release status.
        - Reissue flag.
        - Country of origin.
        - Year validation using artist activity period if available.
        - Packaging type.

        Args:
            release (dict): Release data from API.
            artist (str): Searched artist name.
            album (str): Searched album name.

        Returns:
            int: Score between 0 and 100.

        Example:
            >>> score = service._score_original_release(release_data, "Vildhjarta", "Thousands of Evils")
            >>> print(score)  # Returns higher score for 2013 release vs 2022 reissue
        """
        # Get scoring weights from config or use defaults
        scoring_config = self.config.get('year_retrieval', {}).get('scoring', {})
        
        score = scoring_config.get('default_base_score', 20)
        
        # Album title matching
        release_title = release.get('title', '').lower()
        album_norm = album.lower()
        if release_title == album_norm:
            score += scoring_config.get('exact_match_bonus', 20)
            self.console_logger.debug(f"Exact match bonus: +{scoring_config.get('exact_match_bonus', 20)}")
        elif release_title in album_norm or album_norm in release_title:
            score += scoring_config.get('partial_match_bonus', 10)
            self.console_logger.debug(f"Partial match bonus: +{scoring_config.get('partial_match_bonus', 10)}")
        
        # Release status
        status = (release.get('status') or '').lower()
        if status == 'official':
            score += scoring_config.get('official_status_bonus', 15)
            self.console_logger.debug(f"Official status bonus: +{scoring_config.get('official_status_bonus', 15)}")
        elif status == 'bootleg':
            score += scoring_config.get('bootleg_penalty', -40)
            self.console_logger.debug(f"Bootleg penalty: {scoring_config.get('bootleg_penalty', -40)}")
        
        # Reissue flag - Apply a stronger penalty
        if release.get('is_reissue', False):
            reissue_penalty = scoring_config.get('reissue_penalty', -40)
            score += reissue_penalty
            self.console_logger.debug(f"Reissue penalty: {reissue_penalty}")
        
        # Special formats penalties (additional check)
        format_details = release.get('format_details', '').lower()
        special_flags = release.get('special_flags', [])
        
        if any(flag.lower() in ['reissue', 'remaster', 'repress'] for flag in special_flags):
            special_format_penalty = scoring_config.get('special_format_penalty', -15)
            score += special_format_penalty
            self.console_logger.debug(f"Special format penalty (reissue flags): {special_format_penalty}")
        
        # Country of origin
        country = (release.get('country') or '').lower()
        if country in ['us', 'usa', 'united states']:
            score += scoring_config.get('us_country_bonus', 10)
            self.console_logger.debug(f"US country bonus: +{scoring_config.get('us_country_bonus', 10)}")
        
        # Year check (relative to the band's active period)
        year_str = release.get('year', '')
        if year_str:
            try:
                year = int(year_str)
                current_year = datetime.now().year
                
                # Check for future dates (impossible for original release)
                if year > current_year:
                    future_penalty = scoring_config.get('future_date_penalty', -50)
                    score += future_penalty
                    self.console_logger.debug(f"Future date penalty: {future_penalty}")
                    return max(0, score)  # Immediately return with a low score
                
                # IMPORTANT: Give substantial penalty to extremely recent releases
                # This helps avoid choosing 2022 over 2013 for Vildhjarta's case
                if year >= current_year - 3:
                    very_recent_penalty = scoring_config.get('very_recent_penalty', -25)
                    score += very_recent_penalty
                    self.console_logger.debug(f"Very recent release penalty: {very_recent_penalty}")
                
                # Use the artist period context if available
                if self.artist_period_context:
                    start_year = self.artist_period_context.get('start_year')
                    end_year = self.artist_period_context.get('end_year')
                    
                    if start_year:
                        # Calculate how close this release is to the artist's earliest release
                        # The closer to the start_year, the higher the score
                        if year >= start_year:
                            years_after_start = year - start_year
                            if years_after_start == 0:
                                # This is the earliest possible release year
                                earliest_year_bonus = scoring_config.get('earliest_year_bonus', 30)
                                score += earliest_year_bonus
                                self.console_logger.debug(f"Earliest possible year bonus: +{earliest_year_bonus}")
                            elif years_after_start <= 2:
                                # Very early release
                                early_release_bonus = scoring_config.get('early_release_bonus', 20)
                                score += early_release_bonus
                                self.console_logger.debug(f"Early release bonus: +{early_release_bonus}")
                            elif years_after_start <= 5:
                                # Moderately early release
                                score += 10
                                self.console_logger.debug(f"Moderately early release bonus: +10")
                            else:
                                # Later release, apply a penalty based on how far from start year
                                late_release_penalty = min(30, years_after_start * 2)
                                score -= late_release_penalty
                                self.console_logger.debug(f"Late release penalty: -{late_release_penalty}")
                        else:
                            # Year before artist's known start - might be incorrect data
                            score -= 15
                            self.console_logger.debug(f"Pre-artist period penalty: -15")
                            
                        # If artist is still active (end_year is None)
                        if end_year is None:
                            if start_year <= year <= current_year:
                                score += scoring_config.get('active_period_bonus', 20)
                                self.console_logger.debug(f"Active period bonus: +{scoring_config.get('active_period_bonus', 20)}")
                            if year >= current_year - 2:
                                score += scoring_config.get('recent_release_penalty', -10)
                                self.console_logger.debug(f"Recent release penalty: {scoring_config.get('recent_release_penalty', -10)}")
                        # If artist is no longer active
                        else:
                            if start_year <= year <= end_year:
                                score += scoring_config.get('active_period_bonus', 20)
                                self.console_logger.debug(f"Active period bonus: +{scoring_config.get('active_period_bonus', 20)}")
                            elif year > end_year:
                                post_breakup_penalty = scoring_config.get('post_breakup_penalty', -20)
                                score += post_breakup_penalty
                                self.console_logger.debug(f"Post-breakup penalty: {post_breakup_penalty}")
                    else:
                        # Fallback to general rules if no activity period info
                        if 1950 <= year <= 2010:
                            score += scoring_config.get('valid_year_bonus', 15)
                            self.console_logger.debug(f"Valid year bonus: +{scoring_config.get('valid_year_bonus', 15)}")
                        elif year > 2018:
                            very_new_penalty = scoring_config.get('very_new_year_penalty', -5)
                            score += very_new_penalty
                            self.console_logger.debug(f"Very new year penalty: {very_new_penalty}")
                else:
                    # Fallback if context not available
                    if 1950 <= year <= 2010:
                        score += scoring_config.get('valid_year_bonus', 15)
                        self.console_logger.debug(f"Valid year bonus: +{scoring_config.get('valid_year_bonus', 15)}")
                    elif year > 2018:
                        score += scoring_config.get('very_new_year_penalty', -5)
                        self.console_logger.debug(f"Very new year penalty: {scoring_config.get('very_new_year_penalty', -5)}")
            except ValueError:
                pass
        
        # Packaging analysis
        packaging = (release.get('packaging') or '').lower()
        if packaging in ['digipak', 'digibook']:
            score += scoring_config.get('digipak_penalty', -5)
            self.console_logger.debug(f"Digipak penalty: {scoring_config.get('digipak_penalty', -5)}")
        elif packaging in ['jewel case']:
            score += scoring_config.get('jewel_case_bonus', 5)
            self.console_logger.debug(f"Jewel case bonus: +{scoring_config.get('jewel_case_bonus', 5)}")
        
        # Log the final score calculation
        self.console_logger.info(f"Final score for {release.get('title')} ({year_str}): {score}")
        
        return max(0, min(score, 100))  # Limit to 0-100 range
            
    async def _get_scored_releases_from_musicbrainz(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from MusicBrainz using the enhanced request method.

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
            
            # First try release group search
            search_url = f"https://musicbrainz.org/ws/2/release-group/?query=artist:{artist_encoded} AND releasegroup:{album_encoded}&fmt=json"
            
            data = await self._make_api_request('musicbrainz', search_url)
            if not data:
                self.console_logger.warning(f"MusicBrainz release group search failed for {artist} - {album}")
                return []
                
            release_groups = data.get("release-groups", [])
            
            # If no release groups found, try release search
            if not release_groups:
                search_url = f"https://musicbrainz.org/ws/2/release/?query=artist:{artist_encoded} AND release:{album_encoded}&fmt=json"
                data = await self._make_api_request('musicbrainz', search_url)
                
                if not data:
                    self.console_logger.warning(f"MusicBrainz release search failed for {artist} - {album}")
                    return []
                    
                releases = data.get("releases", [])
                
                if not releases:
                    self.console_logger.info(f"No results from MusicBrainz for '{artist} - {album}'")
                    return []
            else:
                # Found release groups, get releases for the first/best one
                self.console_logger.info(f"Found {len(release_groups)} release groups for {artist} - {album}")
                top_release_group = release_groups[0]
                release_group_id = top_release_group.get("id")
                
                releases_url = f"https://musicbrainz.org/ws/2/release?release-group={release_group_id}&fmt=json"
                data = await self._make_api_request('musicbrainz', releases_url)
                
                if not data:
                    self.console_logger.warning(f"Failed to get releases for group {release_group_id}")
                    releases = []
                else:
                    releases = data.get("releases", [])
                    
                    # Log useful information about the release group
                    primary_type = top_release_group.get("primary-type", "Unknown")
                    first_release_date = top_release_group.get("first-release-date", "Unknown")
                    self.console_logger.info(f"Release group info - Type: {primary_type}, First release date: {first_release_date}")
            
            # Now score all the valid releases
            for release in releases:
                # Extract year from date
                year = None
                release_date = release.get("date", "")
                if release_date:
                    year_match = re.search(r'^(\d{4})', release_date)
                    if year_match:
                        year = year_match.group(1)
                
                # Create a standardized release data structure for scoring
                is_reissue = any(kw in release.get('title', '').lower() for kw in ['reissue', 'remaster', 'anniversary', 'edition'])
                
                release_data = {
                    'source': 'musicbrainz',
                    'title': release.get('title', ''),
                    'type': release.get('primary-type', '') or release.get('release-group', {}).get('primary-type', ''),
                    'status': release.get('status', ''),
                    'year': year,
                    'country': release.get('country', ''),
                    'packaging': release.get('packaging', ''),
                    'is_reissue': is_reissue,
                    'score': 0
                }
                
                # Only score if we have a year
                if release_data['year']:
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
                is_reissue = any(kw.lower() in title.lower() for kw in [
                    'reissue', 'remaster', 'anniversary', 'edition', 'repress', 're-issue', 're-release'
                ])
                
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
                    f"Discogs release: {title} ({item.get('year', 'Unknown')}) - " +
                    f"Country: {item.get('country', 'Unknown')}, Format: {formats_str}" +
                    (f", Tags: {', '.join(special_flags)}" if special_flags else "")
                )
                
                # Determine if this is likely a reissue based on format details
                format_based_reissue = any(kw.lower() in ' '.join(special_flags).lower() for kw in [
                    'reissue', 'remaster', 'repress', 'anniversary', 'deluxe', 're-press', 're-issue'
                ])
                
                # Sometimes reissues are marked in the comments
                comments = item.get('comment', '')
                comments_suggest_reissue = False
                if comments and isinstance(comments, str):
                    comments_suggest_reissue = any(kw.lower() in comments.lower() for kw in [
                        'reissue', 'remaster', 'repress', 'anniversary', 'deluxe', 're-press', 're-issue'
                    ])
                
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
                    'score': 0
                }
                
                if release_data['year']:
                    release_data['score'] = self._score_original_release(release_data, artist, album)
                    scored_releases.append(release_data)
                    
            # Sort by score
            scored_releases.sort(key=lambda x: x['score'], reverse=True)
            
            # Log top results
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"Discogs #{i+1}: {r['title']} ({r['year']}) - " +
                    f"Country: {r.get('country', 'Unknown')}, " +
                    f"Format: {r.get('format', 'Unknown')}, " +
                    f"Is Reissue: {r.get('is_reissue')}, " +
                    f"Score: {r['score']}"
                )
                
            self.console_logger.info(f"Scored {len(scored_releases)} results from Discogs")
            return scored_releases
            
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from Discogs for '{artist} - {album}': {e}")
            return []

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
                result = result[:-len(suffix)]
        result = re.sub(r'[^\w\s]', ' ', result)
        result = re.sub(r'\s+', ' ', result).strip()
        return result

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