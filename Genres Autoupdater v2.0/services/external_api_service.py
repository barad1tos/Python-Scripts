#!/usr/bin/env python3

"""
External API Service for Music Metadata

This module provides the `ExternalApiService` class, designed to interact with external music metadata APIs
(MusicBrainz, Discogs, and optionally Last.fm) to retrieve and determine the original release year of albums.

Key Features:
- **Multi-API Integration:** Fetches data from MusicBrainz, Discogs, and Last.fm.
- **Advanced Rate Limiting:** Uses `EnhancedRateLimiter` with a moving window approach to maximize throughput
    while adhering to API limits.
- **Concurrency Control:** Manages concurrent API calls effectively.
- **Sophisticated Scoring:** Employs a detailed scoring algorithm (`_score_original_release`) to evaluate
    potential releases and determine the most likely original release year, considering factors like release type,
    status, region, artist activity period, and MusicBrainz Release Group data.
- **Artist Context:** Utilizes artist activity period and region (fetched from MusicBrainz and cached) to
    improve scoring accuracy.
- **Data Aggregation:** Combines results from multiple APIs for a more robust determination.
- **Normalization & Validation:** Normalizes artist/album names for better matching and validates years.
- **Error Handling & Retries:** Implements robust error handling and retry logic for API requests.
- **Caching:** Caches artist activity period and region to reduce redundant API calls.
- **Statistics & Logging:** Tracks API usage statistics and provides detailed logging for diagnostics.
- **Prerelease Handling:** Includes logic (`should_update_album_year`) to avoid updating years for albums
    marked as prerelease in the user's library, potentially marking them for future verification.

Classes:
- `EnhancedRateLimiter`: A standalone rate limiter class.
- `ExternalApiService`: The main service class orchestrating API interactions and year determination.

Example Usage:
        >>> import asyncio
        >>> import logging
        >>> from services.external_api_service import ExternalApiService
        >>>
        >>> # Assume config is loaded and loggers are configured
        >>> # config = load_my_config()
        >>> # console_logger = logging.getLogger('console')
        >>> # error_logger = logging.getLogger('error')
        >>> # service = ExternalApiService(config, console_logger, error_logger)
        >>>
        >>> async def main():
        ...     # await service.initialize() # Initialize the HTTP session
        ...     # try:
        ...     #     # Example: Get year for a well-known album
        ...     #     artist = "Example Artist"
        ...     #     album = "Example Album"
        ...     #     current_year = "1999" # Optional: provide current library year
        ...     #     # pending_service = PendingVerificationService(...) # Optional
        ...     #
        ...     #     year, is_definitive = await service.get_album_year(
        ...     #         artist, album, current_year #, pending_service
        ...     #     )
        ...     #
        ...     #     if year:
        ...     #         print(f"Determined year for '{artist} - {album}': {year} (Definitive: {is_definitive})")
        ...     #     else:
        ...     #         print(f"Could not determine year for '{artist} - {album}'.")
        ...     #
        ...     # finally:
        ...     #     await service.close() # Close the session and log stats
        >>>
        >>> # asyncio.run(main())
"""

import asyncio
import logging
import random
import re
import time
import urllib.parse

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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
        total_requests (int): Counter for total requests made through this limiter.
        total_wait_time (float): Cumulative wait time due to rate limiting.
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
            requests_per_window (int): Maximum requests allowed in the time window. Must be > 0.
            window_size (float): Time window in seconds. Must be > 0.
            max_concurrent (int): Maximum concurrent requests allowed. Must be > 0.
            logger (Optional[logging.Logger]): Logger for rate limiting events. Defaults to module logger.

        Raises:
            ValueError: If parameters are not positive numbers/integers.

        Example:
            >>> limiter = EnhancedRateLimiter(60, 60.0, 5)  # 60 requests/minute, max 5 concurrent
        """
        if not isinstance(requests_per_window, int) or requests_per_window <= 0:
            raise ValueError("requests_per_window must be a positive integer")
        if not isinstance(window_size, (int, float)) or window_size <= 0:
            raise ValueError("window_size must be a positive number")
        if not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")

        self.requests_per_window = requests_per_window
        self.window_size = float(window_size)
        # Using list as deque for simplicity, but collections.deque might be more performant for large windows
        self.request_timestamps: List[float] = []
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.logger = logger or logging.getLogger(__name__)
        self.total_requests: int = 0
        self.total_wait_time: float = 0.0

    async def acquire(self) -> float:
        """
        Acquire permission to make a request, waiting if necessary due to rate limits or concurrency limits.

        Returns:
            float: The wait time in seconds (0 if no wait was needed for rate limiting).

        Example:
            >>> wait_time = await limiter.acquire()
            >>> if wait_time > 0:
            >>>     print(f"Waited {wait_time:.3f}s for rate limiting")
        """
        # Wait for rate limit first
        rate_limit_wait_time = await self._wait_if_needed()
        self.total_requests += 1
        self.total_wait_time += rate_limit_wait_time

        # Then wait for concurrency semaphore
        await self.semaphore.acquire()

        return rate_limit_wait_time

    def release(self) -> None:
        """
        Release the concurrency semaphore after the request is completed or failed.

        Example:
            >>> try:
            ...    # make request
            ... finally:
            ...    limiter.release()
        """
        self.semaphore.release()

    async def _wait_if_needed(self) -> float:
        """
        Internal method to check rate limits and wait if necessary.

        Uses a monotonic clock for accurate time interval calculations.

        Returns:
            float: Wait time in seconds (0 if no wait needed).
        """
        now = time.monotonic()  # Use monotonic clock for interval timing

        # Clean up old timestamps outside the window
        # This check ensures we don't keep growing the list indefinitely
        # It iterates from the left (oldest) and stops when a timestamp is within the window
        while self.request_timestamps and now - self.request_timestamps[0] > self.window_size:
            self.request_timestamps.pop(0)

        # If we've reached the limit, calculate wait time until the oldest request expires
        if len(self.request_timestamps) >= self.requests_per_window:
            oldest_timestamp = self.request_timestamps[0]
            wait_duration = (oldest_timestamp + self.window_size) - now

            # Only wait if the calculated duration is positive
            if wait_duration > 0:
                # Use 3 decimal places for milliseconds precision in logs
                self.logger.debug(
                    f"Rate limit reached ({len(self.request_timestamps)}/{self.requests_per_window} "
                    f"in {self.window_size}s). Waiting {wait_duration:.3f}s"
                )
                await asyncio.sleep(wait_duration)
                # After waiting, check again recursively in case multiple requests waited simultaneously
                # The recursive call handles scenarios where the wait wasn't quite enough
                # and adds the new wait time to the original
                return wait_duration + await self._wait_if_needed()
            # If wait_duration is <= 0, it means the slot freed up while we were checking.
            # No need to wait, but we should remove the expired timestamp before proceeding.
            # This case is implicitly handled by the while loop above if run again, or the check below.

        # Record this request's timestamp *after* potential waiting and cleanup
        self.request_timestamps.append(time.monotonic())
        return 0.0  # No waiting was required in this pass

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about rate limiter usage.

        Returns:
            Dict[str, Any]: Dictionary with statistics including:
                - total_requests (int): Total requests processed by this limiter.
                - total_wait_time (float): Total seconds spent waiting due to rate limits.
                - avg_wait_time (float): Average wait time per request.
                - current_window_usage (int): Number of requests in the current time window.
                - max_requests_per_window (int): Configured limit for the window.

        Example:
            >>> stats = limiter.get_stats()
            >>> print(f"Made {stats['total_requests']} requests with {stats['avg_wait_time']:.3f}s average wait")
        """
        # Ensure current window usage is up-to-date by cleaning expired timestamps
        now = time.monotonic()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts <= self.window_size]

        return {
            "total_requests": self.total_requests,
            "total_wait_time": self.total_wait_time,
            "avg_wait_time": self.total_wait_time / max(1, self.total_requests),  # Avoid division by zero
            "current_window_usage": len(self.request_timestamps),
            "max_requests_per_window": self.requests_per_window,
        }


class ExternalApiService:
    """
    Enhanced service for interacting with MusicBrainz, Discogs, and Last.fm APIs.

    Uses `EnhancedRateLimiter` for precise rate control, revised scoring for year determination,
    and includes artist context (activity period, region) for better accuracy.
    """

    def __init__(self, config: Dict[str, Any], console_logger: logging.Logger, error_logger: logging.Logger):
        """
        Initialize the API service with configuration and loggers.
        Correctly reads nested configuration parameters.

        Args:
            config (Dict[str, Any]): Application configuration dictionary.
            console_logger (logging.Logger): Logger for console messages.
            error_logger (logging.Logger): Logger for error messages.

        Raises:
            ValueError: If essential configuration sections or keys are missing or invalid.
        """
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.session: Optional[aiohttp.ClientSession] = None

        # --- Configuration Extraction ---
        year_config = config.get("year_retrieval", {})
        if not isinstance(year_config, dict):
            # Log critical error and raise, as service cannot function without this section
            self.error_logger.critical("Configuration error: 'year_retrieval' section missing or invalid.")
            raise ValueError("Configuration error: 'year_retrieval' section missing or invalid.")

        # --- Correctly read nested API Auth settings ---
        api_auth_config = year_config.get("api_auth", {})
        if not isinstance(api_auth_config, dict):
            self.error_logger.critical("Configuration error: 'year_retrieval.api_auth' subsection missing or invalid.")
            raise ValueError("Configuration error: 'year_retrieval.api_auth' subsection missing or invalid.")

        self.discogs_token = api_auth_config.get('discogs_token')
        self.musicbrainz_app_name = api_auth_config.get('musicbrainz_app_name', "MusicGenreUpdater/UnknownVersion")
        self.contact_email = api_auth_config.get('contact_email')  # Required for MB
        self.lastfm_api_key = api_auth_config.get('lastfm_api_key')
        self.use_lastfm = bool(self.lastfm_api_key) and api_auth_config.get(
            "use_lastfm", True
        )  # Check use_lastfm within api_auth too? Or keep top level? Let's check top level year_config for this toggle.
        self.use_lastfm = bool(self.lastfm_api_key) and year_config.get(
            "use_lastfm", True
        )  # Corrected: use_lastfm toggle remains directly under year_retrieval

        # User-Agent setup
        if not self.contact_email:
            self.error_logger.critical(
                "Configuration error: 'contact_email' is missing in 'year_retrieval.api_auth'. "
                "MusicBrainz API usage requires a valid contact email or URL."
            )
            self.user_agent = self.musicbrainz_app_name  # Fallback
        else:
            self.user_agent = f"{self.musicbrainz_app_name} ({self.contact_email})"
        # --------------------------------------------

        # --- Rate Limiter Initialization ---
        rate_limits_config = year_config.get("rate_limits", {})
        processing_config = year_config.get("processing", {})
        logic_config = year_config.get("logic", {})
        self.scoring_config = year_config.get("scoring", {})  # Load scoring config

        try:
            concurrent_calls = max(1, rate_limits_config.get("concurrent_api_calls", 5))  # Read from rate_limits subsection
            self.rate_limiters = {
                'discogs': EnhancedRateLimiter(
                    requests_per_window=max(1, rate_limits_config.get("discogs_requests_per_minute", 25)),
                    window_size=60.0,
                    max_concurrent=concurrent_calls,
                    logger=console_logger,
                ),
                'musicbrainz': EnhancedRateLimiter(
                    requests_per_window=max(1, rate_limits_config.get("musicbrainz_requests_per_second", 1)),
                    window_size=1.0,
                    max_concurrent=concurrent_calls,
                    logger=console_logger,
                ),
                'lastfm': EnhancedRateLimiter(
                    requests_per_window=max(1, rate_limits_config.get("lastfm_requests_per_second", 5)),
                    window_size=1.0,
                    max_concurrent=concurrent_calls,
                    logger=console_logger,
                ),
            }
        except ValueError as e:
            self.error_logger.critical(f"Invalid rate limiter configuration: {e}")
            raise ValueError(f"Invalid rate limiter configuration: {e}") from e
        # ----------------------------------

        # --- Year Validation Parameters ---
        self.min_valid_year = logic_config.get('min_valid_year', 1900)
        self.definitive_score_threshold = logic_config.get('definitive_score_threshold', 85)
        self.definitive_score_diff = logic_config.get('definitive_score_diff', 15)
        self.current_year = datetime.now().year
        # ----------------------------------

        # --- Caching ---
        self.activity_cache: Dict[str, Tuple[Tuple[Optional[int], Optional[int]], float]] = {}
        self.region_cache: Dict[str, Tuple[Optional[str], float]] = {}
        self.cache_ttl_days = processing_config.get("cache_ttl_days", 30)  # Read from processing subsection
        self.artist_period_context: Optional[Dict[str, Optional[int]]] = None
        # ----------------------------------

        # --- Statistics Tracking ---
        self.request_counts = {'discogs': 0, 'musicbrainz': 0, 'lastfm': 0}
        self.api_call_durations: Dict[str, List[float]] = {'discogs': [], 'musicbrainz': [], 'lastfm': []}
        # ----------------------------------

        # Ensure scoring_config is a dict (already done above)
        if not isinstance(self.scoring_config, dict):
            self.console_logger.warning("Scoring configuration missing or invalid, using default scores.")
            self.scoring_config = {}

    async def initialize(self, force: bool = False) -> None:
        """
        Initialize the aiohttp ClientSession.

        Creates a new session if one doesn't exist, is closed, or if `force` is True.
        Sets default User-Agent and Accept headers.

        Args:
            force (bool): If True, close the existing session and create a new one.
        """
        if force and self.session and not self.session.closed:
            await self.session.close()
            self.session = None  # Ensure it's recreated below

        if not self.session or self.session.closed:
            # Sensible timeouts
            timeout = aiohttp.ClientTimeout(total=45, connect=15, sock_connect=15, sock_read=30)
            # Connector with reasonable limits
            connector = aiohttp.TCPConnector(
                limit_per_host=10,  # Limit connections per host endpoint
                limit=50,  # Limit total connections in the pool
                ssl=True,  # Default to verifying SSL certs
                force_close=True,  # Close connections after request - potentially less efficient but more robust against stale connections
                use_dns_cache=True,
                ttl_dns_cache=300,  # Cache DNS for 5 minutes
            )
            # Default headers for all requests made by this session
            headers = {"User-Agent": self.user_agent, "Accept": "application/json", "Accept-Encoding": "gzip, deflate"}  # Request compression

            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers)
            self.console_logger.info(f"External API session initialized with User-Agent: {self.user_agent}" + (" (forced)" if force else ""))

    async def close(self) -> None:
        """
        Close the aiohttp ClientSession and log API usage statistics.
        """
        if self.session and not self.session.closed:
            # Log API statistics before closing
            self.console_logger.info("--- API Call Statistics ---")
            total_api_calls = 0
            total_api_time = 0.0
            for api_name, limiter in self.rate_limiters.items():
                # Ensure stats are up-to-date before logging
                stats = limiter.get_stats()
                durations = self.api_call_durations.get(api_name, [])
                avg_duration = sum(durations) / max(1, len(durations)) if durations else 0.0
                total_api_calls += stats['total_requests']
                total_api_time += sum(durations)
                self.console_logger.info(
                    f"API: {api_name.title():<12} | "
                    f"Requests: {stats['total_requests']:<5} | "
                    f"Avg Wait: {stats['avg_wait_time']:.3f}s | "  # Increased precision
                    f"Avg Duration: {avg_duration:.3f}s"  # Increased precision
                )

            if total_api_calls > 0:
                avg_total_duration = total_api_time / total_api_calls
                self.console_logger.info(f"Total API Calls: {total_api_calls}, Average Call Duration: {avg_total_duration:.3f}s")
            else:
                self.console_logger.info("No API calls were made during this session.")
            self.console_logger.info("---------------------------")

            await self.session.close()
            self.console_logger.info("External API session closed")

    async def _make_api_request(
        self,
        api_name: str,
        url: str,
        params: Optional[Dict[str, Union[str, int]]] = None,  # Allow int params
        headers_override: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
        timeout_override: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Make an API request with rate limiting, error handling, and retry logic.
        Includes enhanced logging for diagnosing failures.

        Args:
            api_name (str): API name ('musicbrainz', 'discogs', 'lastfm').
            url (str): Full URL to request.
            params (Optional[Dict[str, Union[str, int]]]): Optional query parameters.
            headers_override (Optional[Dict[str, str]]): Headers to override session defaults.
            max_retries (int): Max retry attempts for 5xx/429/timeouts.
            base_delay (float): Base delay for exponential backoff.
            timeout_override (Optional[int]): Specific total timeout for this request.

        Returns:
            Optional[Dict[str, Any]]: Parsed JSON response or None on failure.
        """
        if not self.session or self.session.closed:
            await self.initialize()
        if not self.session or self.session.closed:
            self.error_logger.error(f"[{api_name}] Session not available for request to {url}")
            return None

        request_headers = self.session.headers.copy()
        if api_name == 'discogs' and self.discogs_token:
            request_headers['Authorization'] = f'Discogs token={self.discogs_token}'
        if headers_override:
            request_headers.update(headers_override)

        limiter = self.rate_limiters.get(api_name)
        if not limiter:
            self.error_logger.error(f"No rate limiter configured for API: {api_name}")
            return None

        request_timeout = aiohttp.ClientTimeout(total=timeout_override) if timeout_override else self.session.timeout
        last_exception: Optional[Exception] = None
        # Construct full URL with params for logging *before* the loop
        log_url = url + (f"?{urllib.parse.urlencode(params or {}, safe=':/')}" if params else "")

        for attempt in range(max_retries + 1):
            wait_time = 0.0
            start_time = 0.0
            elapsed = 0.0
            response_status = -1
            response_text_snippet = "[No Response Body]"  # Default snippet

            try:
                wait_time = await limiter.acquire()
                if wait_time > 0.1:
                    self.console_logger.debug(f"[{api_name}] Waited {wait_time:.3f}s for rate limiting")

                self.request_counts[api_name] = self.request_counts.get(api_name, 0) + 1
                start_time = time.monotonic()

                async with self.session.get(url, params=params, headers=request_headers, timeout=request_timeout) as response:
                    elapsed = time.monotonic() - start_time
                    response_status = response.status
                    self.api_call_durations[api_name].append(elapsed)

                    # --- ADDED: Log headers being sent for Discogs ---
                    # Note: Logging headers here, after the request is sent but before processing response.
                    # If logging *before* sending is desired, move this block before the `async with`.
                    if api_name == 'discogs':
                        # Log only relevant headers for security/brevity if needed, but let's log all for now
                        self.console_logger.debug(f"[discogs] Sending Headers: {request_headers}")
                    # ------------------------------------------------

                    response_text_snippet = "[Could not read text]"
                    try:
                        # Read limited text for logging ALL Discogs responses
                        if api_name == 'discogs':
                            raw_text = await response.text(encoding='utf-8', errors='ignore')
                            response_text_snippet = raw_text[:500]  # Log more characters for Discogs
                            self.console_logger.debug(f"====== DISCOGS RAW RESPONSE (Status: {response_status}) ======")
                            self.console_logger.debug(response_text_snippet)
                            self.console_logger.debug("====== END DISCORS RAW RESPONSE ====== ")
                        else:
                            # Keep shorter snippet for other APIs unless needed
                            response_text_snippet = await response.text(encoding='utf-8', errors='ignore')
                            response_text_snippet = response_text_snippet[:200]
                    except Exception as read_err:
                        response_text_snippet = f"[Error Reading Response: {read_err}]"
                        self.error_logger.warning(f"[{api_name}] Failed to read response body: {read_err}")

                    # Log basic request outcome
                    self.console_logger.debug(f"[{api_name}] Request (Attempt {attempt+1}): {log_url} - Status: {response_status} ({elapsed:.3f}s)")

                    # --- Handle Response Status ---
                    # Retry on 429 or 5xx
                    if response_status == 429 or response_status >= 500:
                        last_exception = aiohttp.ClientResponseError(
                            response.request_info, response.history, status=response_status, message=response_text_snippet
                        )
                        if attempt < max_retries:
                            delay = base_delay * (2**attempt) * (0.8 + random.random() * 0.4)
                            retry_after_header = response.headers.get('Retry-After')
                            if retry_after_header:
                                try:
                                    retry_after_seconds = int(retry_after_header)
                                    delay = max(delay, retry_after_seconds)
                                    self.console_logger.warning(f"[{api_name}] Respecting Retry-After header: {retry_after_seconds}s")
                                except ValueError:
                                    pass
                            self.console_logger.warning(
                                f"[{api_name}] Status {response_status}, retrying {attempt+1}/{max_retries} "
                                f"in {delay:.2f}s. URL: {url}. Snippet: {response_text_snippet}"
                            )
                            await asyncio.sleep(delay)
                            continue
                        else:
                            self.error_logger.error(
                                f"[{api_name}] Request failed after {max_retries + 1} attempts with status {response_status}. "
                                f"URL: {url}. Snippet: {response_text_snippet}"
                            )
                            return None

                    # Fail definitively on other errors (e.g., 404, 401, 403)
                    if not response.ok:
                        self.error_logger.warning(
                            f"[{api_name}] API request failed with status {response_status}. URL: {url}. Snippet: {response_text_snippet}"
                        )
                        last_exception = aiohttp.ClientResponseError(
                            response.request_info, response.history, status=response_status, message=response_text_snippet
                        )
                        return None

                    # Success (2xx) - Try to parse JSON
                    try:
                        if 'application/json' in response.headers.get('Content-Type', ''):
                            # Use the already read text if possible, otherwise re-read with response.json()
                            # This avoids reading twice but requires care if response_text_snippet was truncated
                            # Let's stick to response.json() for safety unless performance is critical
                            return await response.json()
                        else:
                            self.error_logger.warning(
                                f"[{api_name}] Received non-JSON response from {url}. "
                                f"Content-Type: {response.headers.get('Content-Type')}. Snippet: {response_text_snippet}"
                            )
                            return None
                    except (aiohttp.ContentTypeError, ValueError, Exception) as json_error:  # Broader catch for json issues
                        self.error_logger.error(
                            f"[{api_name}] Error parsing JSON response from {url}: {json_error}. Snippet: {response_text_snippet}"
                        )
                        last_exception = json_error
                        return None

            except asyncio.TimeoutError as timeout_error:
                elapsed = time.monotonic() - start_time if start_time > 0 else 0.0
                self.api_call_durations[api_name].append(elapsed)
                last_exception = timeout_error
                if attempt < max_retries:
                    delay = base_delay * (2**attempt) * (0.8 + random.random() * 0.4)
                    self.console_logger.warning(
                        f"[{api_name}] Request timed out after {elapsed:.2f}s (Attempt {attempt+1}), retrying in {delay:.2f}s: {url}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    self.error_logger.error(f"[{api_name}] Request timed out after {max_retries + 1} attempts ({elapsed:.2f}s): {url}")
                    return None
            except aiohttp.ClientError as client_error:
                elapsed = time.monotonic() - start_time if start_time > 0 else 0.0
                self.api_call_durations[api_name].append(elapsed)
                self.error_logger.error(f"[{api_name}] Client error during request to {url} (Attempt {attempt+1}): {client_error}")
                last_exception = client_error
                if attempt < max_retries and isinstance(client_error, (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError)):
                    delay = base_delay * (2**attempt) * (0.8 + random.random() * 0.4)
                    await asyncio.sleep(delay)
                    continue
                else:
                    return None
            except Exception as e:
                elapsed = time.monotonic() - start_time if start_time > 0 else 0.0
                self.api_call_durations[api_name].append(elapsed)
                self.error_logger.exception(f"[{api_name}] Unexpected error making request to {url} (Attempt {attempt+1}): {e}")
                last_exception = e
                return None
            finally:
                if limiter:
                    limiter.release()

        # Should not be reached
        self.error_logger.error(f"[{api_name}] Request loop ended without returning for URL: {url}. Last exception: {last_exception}")
        return None

    # `should_update_album_year` remains the same as provided in the previous response.
    # Ensure it gets `pending_verification_service` passed correctly.
    def should_update_album_year(
        self,
        tracks: List[Dict[str, str]],
        artist: str = "",
        album: str = "",
        current_library_year: str = "",
        pending_verification_service=None,  # Pass the service instance
    ) -> bool:
        """
        Determines whether to update the year for an album based on the status of its tracks.

        Args:
            tracks (List[Dict[str, str]]): List of track dictionaries for the album.
            artist (str): Artist name (for logging).
            album (str): Album name (for logging).
            current_library_year (str): Current year value in the library.
            pending_verification_service: Instance of PendingVerificationService or None.

        Returns:
            bool: True if the year can be updated based on track status, False otherwise.
        """
        if not tracks:  # If no tracks provided, assume update is ok (or handle as error?)
            return True

        prerelease_count = sum(1 for track in tracks if track.get("trackStatus", "").lower() == "prerelease")
        subscription_count = sum(1 for track in tracks if track.get("trackStatus", "").lower() == "subscription")
        total_tracks = len(tracks)

        # If there is at least one track in prerelease status
        if prerelease_count > 0:
            # If there are no subscription tracks, OR if prerelease tracks are the majority (>50%)
            if subscription_count == 0 or prerelease_count * 2 > total_tracks:
                self.console_logger.info(
                    f"Album '{artist} - {album}' has {prerelease_count}/{total_tracks} tracks in prerelease status. "
                    f"Keeping current year: {current_library_year or 'N/A'}"
                )

                # Mark for future verification if service is available
                if pending_verification_service:
                    try:
                        # Use original artist/album names for consistency if available, else normalized
                        pending_verification_service.mark_for_verification(artist, album)
                    except Exception as e_pvs:
                        self.error_logger.error(f"Failed to mark '{artist} - {album}' for verification: {e_pvs}")

                return False  # Do not update year if prerelease conditions met

        return True  # OK to update year based on track status

    # `get_album_year` remains the same as provided in the previous response (using the revised logic).
    async def get_album_year(
        self, artist: str, album: str, current_library_year: Optional[str] = None, pending_verification_service=None
    ) -> Tuple[Optional[str], bool]:
        """
        Determine the original release year for an album using optimized API calls and revised scoring.

        This REVISED method prioritizes MusicBrainz release group dates, uses an improved
        scoring system across MusicBrainz, Discogs, and Last.fm, and avoids biasing
        towards the current library year.

        Args:
            artist (str): Artist name.
            album (str): Album name.
            current_library_year (Optional[str]): Year currently in the library (used for context only).
            pending_verification_service: Instance of PendingVerificationService or None.

        Returns:
            Tuple[Optional[str], bool]: (determined_year, is_definitive)
            - determined_year: The determined original release year as a string, or None if not found/determined.
            - is_definitive: True if the result is considered highly confident based on scoring thresholds.
        """
        # Normalize inputs for API matching but keep original for logging/reporting
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)
        log_artist = artist if artist != artist_norm else artist_norm  # Prefer original if different
        log_album = album if album != album_norm else album_norm

        self.console_logger.info(f"Searching for original release year: '{log_artist} - {log_album}' (current: {current_library_year or 'none'})")

        # --- Pre-computation Steps ---
        start_year: Optional[int] = None
        end_year: Optional[int] = None
        artist_region: Optional[str] = None
        try:
            # 1. Get artist's activity period for context (cached)
            start_year, end_year = await self.get_artist_activity_period(artist_norm)
            self.artist_period_context = {'start_year': start_year, 'end_year': end_year}  # Set context for this run
            activity_log = f"({start_year or '?'} - {end_year or 'present'})" if start_year or end_year else "(activity period unknown)"
            self.console_logger.info(f"Artist activity period context: {activity_log}")

            # 2. Get artist's likely region for scoring context (cached)
            artist_region = await self._get_artist_region(artist_norm)
            if artist_region:
                self.console_logger.info(f"Artist region context: {artist_region.upper()}")
        except Exception as context_err:
            self.error_logger.warning(f"Error fetching artist context for '{log_artist}': {context_err}")
            # Continue without context, scoring will be less accurate

        # --- Fetch Data Concurrently ---
        try:
            api_tasks = [
                # Pass artist_region context to API methods
                self._get_scored_releases_from_musicbrainz(artist_norm, album_norm, artist_region),
                self._get_scored_releases_from_discogs(artist_norm, album_norm, artist_region),
            ]
            if self.use_lastfm:
                # Last.fm method doesn't use region context currently
                api_tasks.append(self._get_scored_releases_from_lastfm(artist_norm, album_norm))

            # Wait for all API calls to complete, collecting results or exceptions
            results = await asyncio.gather(*api_tasks, return_exceptions=True)

            # --- Process Results ---
            all_releases: List[Dict[str, Any]] = []
            api_sources_found: Set[str] = set()

            api_names = ['musicbrainz', 'discogs', 'lastfm'] if self.use_lastfm else ['musicbrainz', 'discogs']
            for i, result in enumerate(results):
                api_name = api_names[i]
                if isinstance(result, Exception):
                    # Log API call failures clearly
                    self.error_logger.warning(f"API call to {api_name} failed for '{log_artist} - {log_album}': {type(result).__name__}: {result}")
                elif isinstance(result, list) and result:
                    all_releases.extend(result)
                    api_sources_found.add(api_name)
                    self.console_logger.info(f"Received {len(result)} scored releases from {api_name.title()}")
                elif not result:  # Explicitly check for empty list/None
                    self.console_logger.info(f"No results from {api_name.title()} for '{log_artist} - {log_album}'")

            if not all_releases:
                self.console_logger.warning(f"No release data found from any API for '{log_artist} - {log_album}'")
                # Mark for verification only if we have a potential year to keep
                if pending_verification_service and current_library_year and self._is_valid_year(current_library_year):
                    try:
                        pending_verification_service.mark_for_verification(artist, album)
                        self.console_logger.info(
                            f"Marked '{log_artist} - {log_album}' for future verification (no API data). Using current year: {current_library_year}"
                        )
                    except Exception as e_pvs:
                        self.error_logger.error(f"Failed to mark '{artist} - {album}' for verification: {e_pvs}")
                    return current_library_year, False  # Return current year, not definitive
                else:
                    # No current year or it's invalid, and no API data -> cannot determine
                    return None, False

            # --- Aggregate Scores by Year ---
            year_scores = defaultdict(list)  # Store list of scores for each year
            valid_years_found: Set[str] = set()

            for release in all_releases:
                year = release.get('year')
                score = release.get('score', 0)
                # Check if year is valid before adding
                if self._is_valid_year(year):
                    year_scores[year].append(score)
                    valid_years_found.add(year)
                # else: # Optional: log discarded invalid years
                #     self.console_logger.debug(f"Discarding release with invalid year: {release.get('title')} ({year})")

            if not year_scores:
                self.console_logger.warning(f"No valid years found after processing API results for '{log_artist} - {log_album}'")
                if pending_verification_service and current_library_year and self._is_valid_year(current_library_year):
                    try:
                        pending_verification_service.mark_for_verification(artist, album)
                    except Exception as e_pvs:
                        self.error_logger.error(f"Failed to mark '{artist} - {album}' for verification: {e_pvs}")
                    return current_library_year, False
                else:
                    return None, False

            # --- Determine Best Year based on Aggregated Scores ---

            # Calculate the maximum score achieved for each year found
            final_year_scores: Dict[str, int] = {year: max(scores) for year, scores in year_scores.items()}

            # Sort years: primarily by score (desc), secondarily by year (asc - prefer earlier for ties)
            sorted_years = sorted(
                final_year_scores.items(), key=lambda item: (-item[1], int(item[0]))  # Note: int(item[0]) assumes year is numeric string
            )

            # Log the ranked years and their highest scores
            log_scores = ", ".join([f"{y}:{s}" for y, s in sorted_years[:5]])  # Log top 5 for brevity
            self.console_logger.info(f"Ranked year scores (Year:MaxScore): {log_scores}" + ("..." if len(sorted_years) > 5 else ""))

            # --- Select Final Year and Determine Confidence ---
            best_year, best_score = sorted_years[0]
            is_definitive = False

            # Condition 1: Score meets the absolute threshold
            high_score_met = best_score >= self.definitive_score_threshold

            # Condition 2: Score is significantly higher than the next best year (if one exists)
            significant_diff_met = True  # Default to True if only one year was found
            if len(sorted_years) > 1:
                second_best_score = sorted_years[1][1]
                score_difference = best_score - second_best_score
                significant_diff_met = score_difference >= self.definitive_score_diff
                self.console_logger.debug(f"Score difference to next best year: {score_difference} (Threshold: {self.definitive_score_diff})")
            else:
                self.console_logger.debug("Only one candidate year found.")

            # Final definitive status depends on both conditions
            is_definitive = high_score_met and significant_diff_met

            self.console_logger.info(
                f"Selected year: {best_year} (Score: {best_score}). "
                f"Definitive? {is_definitive} (Score Met: {high_score_met}, Diff Met: {significant_diff_met})"
            )

            # If not definitive, mark for potential future verification
            if not is_definitive and pending_verification_service:
                try:
                    pending_verification_service.mark_for_verification(artist, album)
                    self.console_logger.info(f"Result for '{log_artist} - {log_album}' is not definitive. Marked for future verification.")
                except Exception as e_pvs:
                    self.error_logger.error(f"Failed to mark '{artist} - {album}' for verification: {e_pvs}")

            # Return the best year found and whether it's considered definitive
            return best_year, is_definitive

        except Exception as e:
            # Catch unexpected errors during the main processing logic
            self.error_logger.exception(f"Unexpected error in get_album_year for '{log_artist} - {log_album}': {e}")
            # Fallback: return current library year if valid, otherwise None; mark as not definitive
            if current_library_year and self._is_valid_year(current_library_year):
                return current_library_year, False
            else:
                return None, False
        finally:
            # Crucial: Clear the context after the function finishes or errors out
            self.artist_period_context = None

    # `get_artist_activity_period` remains the same as provided in the previous response.
    async def get_artist_activity_period(self, artist_norm: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Retrieve the period of activity for an artist from MusicBrainz, with caching.

        Args:
            artist_norm (str): The normalized artist's name.

        Returns:
            Tuple[Optional[int], Optional[int]]: (start_year, end_year).
            If end_year is None, the artist is likely still active. Returns (None, None) on failure.
        """
        cache_key = f"activity_{artist_norm}"
        cache_ttl_seconds = self.cache_ttl_days * 86400
        current_time = time.monotonic()

        # Check cache first
        if cache_key in self.activity_cache:
            (start_year, end_year), timestamp = self.activity_cache[cache_key]
            if current_time - timestamp < cache_ttl_seconds:
                self.console_logger.debug(f"Using cached activity period for '{artist_norm}': {start_year or '?'} - {end_year or 'present'}")
                return start_year, end_year
            else:
                self.console_logger.debug(f"Activity cache expired for '{artist_norm}'")
                del self.activity_cache[cache_key]  # Remove expired entry

        # Clean cache periodically if it grows too large (simple strategy)
        if len(self.activity_cache) > 500:  # Example threshold
            self._clean_expired_cache(self.activity_cache, cache_ttl_seconds, "activity")

        self.console_logger.debug(f"Fetching activity period for artist: {artist_norm}")
        start_year_result: Optional[int] = None
        end_year_result: Optional[int] = None

        try:
            # Search MusicBrainz for the artist
            params = {"query": f'artist:"{artist_norm}"', "fmt": "json", "limit": "5"}  # Limit results
            search_url = "https://musicbrainz.org/ws/2/artist/"
            data = await self._make_api_request('musicbrainz', search_url, params=params)

            if not data or "artists" not in data or not data["artists"]:
                self.console_logger.warning(f"Cannot determine activity period for '{artist_norm}': Artist not found in MusicBrainz.")
                # Cache the negative result
                self.activity_cache[cache_key] = ((None, None), current_time)
                return None, None

            # --- Find the best matching artist ---
            # Simple approach: use the first result if its name matches closely enough.
            # More complex logic could compare aliases, types (Person/Group), disambiguation, score etc.
            best_match_artist = data["artists"][0]
            artist_name_found = best_match_artist.get("name", "Unknown")
            artist_id = best_match_artist.get("id")

            # Optional: Log if the best match name differs significantly
            if artist_name_found.lower() != artist_norm.lower():
                self.console_logger.debug(f"Best MusicBrainz match for '{artist_norm}' is '{artist_name_found}' (ID: {artist_id})")

            # --- Extract lifespan ---
            life_span = best_match_artist.get("life-span", {})
            if isinstance(life_span, dict):  # Ensure it's a dictionary
                begin_date_str = life_span.get("begin")
                end_date_str = life_span.get("end")
                ended = life_span.get("ended", False)  # Check if the 'ended' flag is true

                # Parse start year
                if begin_date_str:
                    try:
                        # Extract only the year part (YYYY)
                        year_part = begin_date_str.split("-")[0]
                        if len(year_part) == 4 and year_part.isdigit():
                            start_year_result = int(year_part)
                        else:
                            self.console_logger.warning(f"Could not parse year from begin date '{begin_date_str}' for {artist_name_found}")
                    except (ValueError, IndexError, TypeError):
                        self.console_logger.warning(f"Error parsing begin date '{begin_date_str}' for {artist_name_found}")

                # Parse end year
                if end_date_str:
                    try:
                        year_part = end_date_str.split("-")[0]
                        if len(year_part) == 4 and year_part.isdigit():
                            end_year_result = int(year_part)
                        else:
                            self.console_logger.warning(f"Could not parse year from end date '{end_date_str}' for {artist_name_found}")
                    except (ValueError, IndexError, TypeError):
                        self.console_logger.warning(f"Error parsing end date '{end_date_str}' for {artist_name_found}")
                elif not ended and start_year_result is not None:
                    # If the 'ended' flag is explicitly false or missing, and we have a start year,
                    # assume the artist is still active (end_year remains None).
                    end_year_result = None
                elif ended and start_year_result is not None:
                    # If 'ended' is true but no end_date_str is present, it's ambiguous.
                    # We could try to infer from last release, but for simplicity, let's treat as active (None).
                    self.console_logger.debug(f"Artist '{artist_name_found}' marked as ended but no end date found in life-span.")
                    end_year_result = None  # Treat as still active or recently ended

            # Store result (even if None) in cache
            self.activity_cache[cache_key] = ((start_year_result, end_year_result), current_time)
            self.console_logger.debug(f"Determined activity period for '{artist_norm}': {start_year_result or '?'} - {end_year_result or 'present'}")
            return start_year_result, end_year_result

        except Exception as e:
            self.error_logger.exception(f"Error determining activity period for '{artist_norm}': {e}")
            # Cache failure case
            self.activity_cache[cache_key] = ((None, None), current_time)
            return None, None

    # `_get_artist_region` remains the same as provided in the previous response.
    async def _get_artist_region(self, artist_norm: str) -> Optional[str]:
        """
        Determine the region (country) of an artist using MusicBrainz API, with caching.

        Args:
            artist_norm (str): Normalized artist name.

        Returns:
            Optional[str]: Lowercase ISO 3166-1 alpha-2 country code (e.g., "us", "gb", "jp") or None if unknown.
        """
        cache_key = f"region_{artist_norm}"
        cache_ttl_seconds = self.cache_ttl_days * 86400  # Use same TTL as activity period
        current_time = time.monotonic()

        # Check cache first
        if cache_key in self.region_cache:
            region, timestamp = self.region_cache[cache_key]
            if current_time - timestamp < cache_ttl_seconds:
                self.console_logger.debug(f"Using cached region '{region}' for artist '{artist_norm}'")
                return region
            else:
                self.console_logger.debug(f"Region cache expired for '{artist_norm}'")
                del self.region_cache[cache_key]

        # Clean cache periodically
        if len(self.region_cache) > 500:
            self._clean_expired_cache(self.region_cache, cache_ttl_seconds, "region")

        region_result: Optional[str] = None
        try:
            # Query MusicBrainz API for artist data, limiting to the top result
            params = {"query": f'artist:"{artist_norm}"', "fmt": "json", "limit": "1"}
            search_url = "https://musicbrainz.org/ws/2/artist/"
            data = await self._make_api_request('musicbrainz', search_url, params=params)

            if data and "artists" in data and data["artists"]:
                best_artist = data["artists"][0]

                # 1. Check the 'country' field first (direct ISO code)
                country_code = best_artist.get("country")
                if country_code and isinstance(country_code, str) and len(country_code) == 2:
                    region_result = country_code.lower()
                    self.console_logger.debug(f"Found region '{region_result}' via 'country' field for '{artist_norm}'")
                else:
                    # 2. Fallback to 'area' information if 'country' is missing/invalid
                    area = best_artist.get("area")
                    if area and isinstance(area, dict):
                        # Check if area is explicitly a country
                        if area.get("type") == "Country":
                            # Try to get ISO code from the area itself
                            iso_codes = area.get("iso-3166-1-codes")
                            if iso_codes and isinstance(iso_codes, list) and len(iso_codes) > 0:
                                # Use the first ISO code found
                                region_result = iso_codes[0].lower()
                                self.console_logger.debug(f"Found region '{region_result}' via area ISO codes for '{artist_norm}'")
                            # else: # Optional: Could try parsing area.name, but less reliable
                            #     self.console_logger.debug(f"Area found for '{artist_norm}' but no ISO code: {area.get('name')}")
                        # else: # Area is not a country (e.g., city, subdivision) - ignore for region
                        #    self.console_logger.debug(
                        #        f"Area found for '{artist_norm}' is not a country: {area.get('name')} "
                        #        f"(Type: {area.get('type')})"
                        #    )
            else:
                # Artist not found in MusicBrainz
                self.console_logger.debug(f"No artist found in MusicBrainz to determine region for '{artist_norm}'")

        except Exception as e:
            # Log errors during API request or processing
            self.error_logger.warning(f"Error retrieving artist region for '{artist_norm}': {e}")

        # Cache the result (even if it's None) before returning
        self.region_cache[cache_key] = (region_result, current_time)
        return region_result

    # `_score_original_release` remains the same as provided in the previous response.
    # file: services/external_api_service.py

    # Replace the existing _score_original_release function with this one:
    def _score_original_release(self, release: Dict[str, Any], artist_norm: str, album_norm: str, artist_region: Optional[str]) -> int:
        """
        REVISED scoring function prioritizing original release indicators (v3).

        Assigns a score based on how likely the `release` dictionary represents
        the original release of the album defined by `artist_norm` and `album_norm`.
        Prioritizes MusicBrainz Release Group date and penalizes later releases more heavily.

        Args:
            release (Dict[str, Any]): Release data dictionary from API processing functions.
            artist_norm (str): Normalized artist name being searched.
            album_norm (str): Normalized album name being searched.
            artist_region (Optional[str]): Lowercase country code of the artist (if known).

        Returns:
            int: Score (typically 0-100+, higher is better).
        """
        # Ensure scoring_config is a dict, fallback to empty if not found or invalid
        scoring_cfg = self.scoring_config if isinstance(self.scoring_config, dict) else {}

        # --- Initialization ---
        score: int = scoring_cfg.get('base_score', 10)  # Start with base score
        score_components: List[str] = []  # For debugging

        # Extract key fields
        release_title_orig = release.get('title', '')
        release_artist_orig = release.get('artist', '')
        year_str = release.get('year', '')
        source = release.get('source', 'unknown')
        release_title_norm = self._normalize_name(release_title_orig)
        release_artist_norm = self._normalize_name(release_artist_orig)

        # --- 1. Core Match Quality ---
        # Artist Match Bonus
        artist_match_bonus = 0
        if release_artist_norm and release_artist_norm == artist_norm:
            artist_match_bonus = scoring_cfg.get('artist_exact_match_bonus', 20)  # Reduced
            score += artist_match_bonus
            score_components.append(f"Artist Exact Match: +{artist_match_bonus}")

        # Album Title Match Bonus/Penalty
        title_match_bonus = 0
        title_penalty = 0

        def simple_norm(text: str) -> str:
            return re.sub(r'[^\w\s]', '', text.lower()).strip()

        comp_release_title = simple_norm(release_title_norm)
        comp_album_norm = simple_norm(album_norm)

        if comp_release_title == comp_album_norm:
            title_match_bonus = scoring_cfg.get('album_exact_match_bonus', 25)  # Reduced
            score += title_match_bonus
            score_components.append(f"Album Exact Match: +{title_match_bonus}")
            if artist_match_bonus > 0:  # Only apply perfect if artist also matched
                perfect_match_bonus = scoring_cfg.get('perfect_match_bonus', 10)  # Reduced
                score += perfect_match_bonus
                score_components.append(f"Perfect Artist+Album Match: +{perfect_match_bonus}")
        else:
            # Handle variations (e.g., "Album (Deluxe)")
            if comp_release_title.startswith(comp_album_norm) and re.match(
                r'^[\(\[][^)\]]+[\)\]]$', comp_release_title[len(comp_album_norm) :].strip()  # noqa: E203
            ):
                title_match_bonus = scoring_cfg.get('album_variation_bonus', 10)
                score += title_match_bonus
                score_components.append(f"Album Variation (Suffix): +{title_match_bonus}")
            elif comp_album_norm.startswith(comp_release_title) and re.match(
                r'^[\(\[][^)\]]+[\)\]]$', comp_album_norm[len(comp_release_title) :].strip()  # noqa: E203
            ):
                title_match_bonus = scoring_cfg.get('album_variation_bonus', 10)
                score += title_match_bonus
                score_components.append(f"Album Variation (Search Suffix): +{title_match_bonus}")
            # Penalize substring inclusion (less likely original)
            elif comp_album_norm in comp_release_title or comp_release_title in comp_album_norm:
                title_penalty = scoring_cfg.get('album_substring_penalty', -15)  # Reduced penalty
                score += title_penalty
                score_components.append(f"Album Substring Mismatch: {title_penalty}")
            else:  # Penalize unrelated titles
                title_penalty = scoring_cfg.get('album_unrelated_penalty', -40)  # Reduced penalty
                score += title_penalty
                score_components.append(f"Album Unrelated: {title_penalty}")

        # --- 2. Release Characteristics ---
        # MusicBrainz Release Group First Date Match (VERY important)
        rg_first_date_str = release.get('releasegroup_first_date')
        rg_first_year: Optional[int] = None
        if source == 'musicbrainz' and rg_first_date_str:
            try:
                rg_year_str = rg_first_date_str.split('-')[0]
                if len(rg_year_str) == 4 and rg_year_str.isdigit():
                    rg_first_year = int(rg_year_str)
                    # Strong bonus if release year matches the RG first year
                    if year_str and rg_year_str == year_str:
                        rg_match_bonus = scoring_cfg.get('mb_release_group_match_bonus', 50)  # Increased!
                        score += rg_match_bonus
                        score_components.append(f"MB RG First Date Match: +{rg_match_bonus}")
            except (IndexError, ValueError, TypeError):
                pass

        # Release Type (Album preferred)
        release_type = str(release.get('type', '')).lower()
        type_bonus = 0
        type_penalty = 0
        if 'album' in release_type:
            type_bonus = scoring_cfg.get('type_album_bonus', 15)
            score_components.append(f"Type Album: +{type_bonus}")
        elif any(t in release_type for t in ['ep', 'single']):
            type_penalty = scoring_cfg.get('type_ep_single_penalty', -10)
            score_components.append(f"Type EP/Single: {type_penalty}")
        elif any(t in release_type for t in ['compilation', 'live', 'soundtrack', 'remix']):
            type_penalty = scoring_cfg.get('type_compilation_live_penalty', -25)
            score_components.append(f"Type Comp/Live/Remix/Soundtrack: {type_penalty}")
        score += type_bonus + type_penalty

        # Release Status (Official preferred)
        status = str(release.get('status', '')).lower()
        status_bonus = 0
        status_penalty = 0
        if status == 'official':
            status_bonus = scoring_cfg.get('status_official_bonus', 10)
            score_components.append(f"Status Official: +{status_bonus}")
        elif any(s in status for s in ['bootleg', 'unofficial', 'pseudorelease']):
            status_penalty = scoring_cfg.get('status_bootleg_penalty', -50)
            score_components.append(f"Status Bootleg/Unofficial: {status_penalty}")
        elif status == 'promotion':
            status_penalty = scoring_cfg.get('status_promo_penalty', -20)
            score_components.append(f"Status Promo: {status_penalty}")
        score += status_bonus + status_penalty

        # Reissue Indicator Penalty
        if release.get('is_reissue', False):
            reissue_penalty = scoring_cfg.get('reissue_penalty', -30)
            score += reissue_penalty
            score_components.append(f"Reissue Indicator: {reissue_penalty}")

        # --- 3. Contextual Factors ---
        year = -1
        year_diff_penalty = 0
        is_valid_year_format = False
        if year_str and year_str.isdigit() and len(year_str) == 4:
            is_valid_year_format = True
            try:
                year = int(year_str)
            except ValueError:
                is_valid_year_format = False

        if not is_valid_year_format:
            score = 0  # Invalid year format invalidates score
            score_components.append("Year Invalid Format: score=0")
        else:
            # Check year range
            if not (self.min_valid_year <= year <= self.current_year + 5):
                score = 0
                score_components.append("Year Out of Range: score=0")
            else:
                # Apply Artist Activity Period Context
                if self.artist_period_context:
                    start_year = self.artist_period_context.get('start_year')
                    end_year = self.artist_period_context.get('end_year')

                    # Penalty if year is before artist start (allow 1 year grace)
                    if start_year and year < start_year - 1:
                        years_before = start_year - year
                        penalty_val = min(50, 5 + (years_before - 1) * 5)
                        score += scoring_cfg.get('year_before_start_penalty', -penalty_val)
                        score_components.append(
                            f"Year Before Start ({years_before} yrs): {scoring_cfg.get('year_before_start_penalty', -penalty_val)}"
                        )

                    # Penalty if year is after artist end (allow 3 years grace)
                    if end_year and year > end_year + 3:
                        years_after = year - end_year
                        penalty_val = min(40, 5 + (years_after - 3) * 3)
                        score += scoring_cfg.get('year_after_end_penalty', -penalty_val)
                        score_components.append(f"Year After End ({years_after} yrs): {scoring_cfg.get('year_after_end_penalty', -penalty_val)}")

                    # Bonus if year is near artist start
                    if start_year and 0 <= (year - start_year) <= 1:
                        score += scoring_cfg.get('year_near_start_bonus', 20)
                        score_components.append(f"Year Near Start: +{scoring_cfg.get('year_near_start_bonus', 20)}")

                # *** NEW: Penalty based on difference from RG First Year ***
                if rg_first_year and year > rg_first_year + 1:  # If release year is >1yr after RG first year
                    year_diff = year - rg_first_year
                    # Apply a scaling penalty, e.g., -5 points per year difference after the first year
                    penalty_scale = scoring_cfg.get('year_diff_penalty_scale', -5)
                    max_penalty = scoring_cfg.get('year_diff_max_penalty', -40)
                    year_diff_penalty = max(max_penalty, (year_diff - 1) * penalty_scale)
                    score += year_diff_penalty
                    score_components.append(f"Year Diff from RG Date ({year_diff} yrs): {year_diff_penalty}")

        # Country / Region Match
        release_country = release.get('country', '').lower()
        if artist_region and release_country:
            if release_country == artist_region:
                score += scoring_cfg.get('country_artist_match_bonus', 10)
                score_components.append(
                    f"Country Matches Artist Region ({artist_region.upper()}): +{scoring_cfg.get('country_artist_match_bonus', 10)}"
                )
            elif release_country in scoring_cfg.get(
                'major_market_codes', ['us', 'gb', 'uk', 'de', 'jp', 'fr']
            ):  # Check against configured major markets
                score += scoring_cfg.get('country_major_market_bonus', 5)
                score_components.append(f"Country Major Market ({release_country.upper()}): +{scoring_cfg.get('country_major_market_bonus', 5)}")

        # --- 4. Source Reliability ---
        source_adjustment = 0
        if source == 'musicbrainz':
            source_adjustment = scoring_cfg.get('source_mb_bonus', 5)
        elif source == 'discogs':
            source_adjustment = scoring_cfg.get('source_discogs_bonus', 2)
        elif source == 'lastfm':
            source_adjustment = scoring_cfg.get('source_lastfm_penalty', -5)
        if source_adjustment != 0:
            score += source_adjustment
            score_components.append(f"Source {source.title()}: {source_adjustment:+}")

        # Final score adjustment
        final_score = max(0, score)  # Score cannot be negative

        # --- Debug Logging ---
        # Log details only if score is potentially decisive or problematic
        if final_score > self.definitive_score_threshold - 20 or year_diff_penalty < 0 or title_penalty < 0:
            debug_log_msg = f"Score Calculation for '{release_title_orig}' ({year_str}) [{source}]:\n"
            debug_log_msg += "\n".join([f"  - {comp}" for comp in score_components])
            debug_log_msg += f"\n  ==> Final Score: {final_score}"
            # Use DEBUG level for potentially verbose output
            self.console_logger.debug(debug_log_msg)

        return final_score

    # `_get_scored_releases_from_musicbrainz` remains the same as provided in the previous response.
    async def _get_scored_releases_from_musicbrainz(self, artist_norm: str, album_norm: str, artist_region: Optional[str]) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from MusicBrainz, prioritizing Release Group search.
        Includes fallback search strategies if the initial precise query fails.

        Args:
            artist_norm (str): Normalized artist name.
            album_norm (str): Normalized album name.
            artist_region (Optional[str]): Artist's region code for scoring.

        Returns:
            List[Dict[str, Any]]: List of scored releases, sorted descending by score.
        """
        scored_releases: List[Dict[str, Any]] = []
        release_groups = []  # Initialize list to store found release groups

        try:
            # Helper for Lucene escaping
            def escape_lucene(term: str) -> str:
                term = term.replace('\\', '\\\\')
                for char in r'+-&|!(){}[]^"~*?:\/':
                    term = term.replace(char, f'\\{char}')
                return term

            base_search_url = "https://musicbrainz.org/ws/2/release-group/"
            common_params = {"fmt": "json", "limit": "10"}  # Common params for searches

            # --- Attempt 1: Precise Fielded Search (Primary Strategy) ---
            primary_query = f'artist:"{escape_lucene(artist_norm)}" AND releasegroup:"{escape_lucene(album_norm)}"'
            params_rg1 = {**common_params, "query": primary_query}
            log_rg_url1 = base_search_url + "?" + urllib.parse.urlencode(params_rg1, safe=':/')
            self.console_logger.debug(f"[musicbrainz] Attempt 1 URL: {log_rg_url1}")

            rg_data1 = await self._make_api_request('musicbrainz', base_search_url, params=params_rg1)

            if rg_data1 and rg_data1.get("count", 0) > 0 and rg_data1.get("release-groups"):
                self.console_logger.debug("Attempt 1 (Precise Search) successful.")
                release_groups = rg_data1.get("release-groups", [])
            else:
                self.console_logger.warning(
                    f"[musicbrainz] Attempt 1 (Precise Search) failed or yielded no results for query: {primary_query}. Trying fallback."
                )

                # --- Attempt 2: Broader Search (Keywords without fields) ---
                fallback_query1 = f'"{escape_lucene(artist_norm)}" AND "{escape_lucene(album_norm)}"'
                params_rg2 = {**common_params, "query": fallback_query1}
                log_rg_url2 = base_search_url + "?" + urllib.parse.urlencode(params_rg2, safe=':/')
                self.console_logger.debug(f"[musicbrainz] Attempt 2 URL: {log_rg_url2}")

                rg_data2 = await self._make_api_request('musicbrainz', base_search_url, params=params_rg2)

                if rg_data2 and rg_data2.get("count", 0) > 0 and rg_data2.get("release-groups"):
                    self.console_logger.debug("Attempt 2 (Broad Search) successful.")
                    release_groups = rg_data2.get("release-groups", [])
                else:
                    self.console_logger.warning(
                        f"[musicbrainz] Attempt 2 (Broad Search) failed or yielded no results for query: {fallback_query1}. Trying last fallback."
                    )

                    # --- Attempt 3: Search by Album Title Only ---
                    fallback_query2 = f'"{escape_lucene(album_norm)}"'  # Search only by album title keyword
                    params_rg3 = {**common_params, "query": fallback_query2}
                    log_rg_url3 = base_search_url + "?" + urllib.parse.urlencode(params_rg3, safe=':/')
                    self.console_logger.debug(f"[musicbrainz] Attempt 3 URL: {log_rg_url3}")

                    rg_data3 = await self._make_api_request('musicbrainz', base_search_url, params=params_rg3)

                    if rg_data3 and rg_data3.get("count", 0) > 0 and rg_data3.get("release-groups"):
                        self.console_logger.debug("Attempt 3 (Album Title Only Search) successful. Will filter by artist.")
                        release_groups = rg_data3.get("release-groups", [])
                    else:
                        self.console_logger.warning(f"[musicbrainz] All search attempts failed for '{artist_norm} - {album_norm}'.")
                        return []  # Return empty list if all searches fail

            # --- Filter Results if Broader Search Was Used ---
            # We need to ensure the artist credit matches "Abney Park" if we used fallback searches
            filtered_release_groups = []
            if not (rg_data1 and rg_data1.get("count", 0) > 0):  # If primary search failed, filter results from fallback
                self.console_logger.debug(f"Filtering {len(release_groups)} fallback results for artist '{artist_norm}'...")
                for rg in release_groups:
                    artist_credit = rg.get("artist-credit", [])
                    if artist_credit and isinstance(artist_credit, list):
                        # Check if the first artist name in the credit matches closely
                        # Using a simple lowercase comparison for now
                        first_artist = artist_credit[0]
                        if isinstance(first_artist, dict) and first_artist.get("name", "").lower() == artist_norm.lower():
                            filtered_release_groups.append(rg)
                        else:  # Log skipped groups?
                            skipped_artist = first_artist.get("name", "Unknown") if isinstance(first_artist, dict) else "Unknown"
                            self.console_logger.debug(
                                f"Skipping RG '{rg.get('title')}' due to artist mismatch ('{skipped_artist}' != '{artist_norm}')"
                            )
                self.console_logger.info(f"Found {len(filtered_release_groups)} release groups matching artist after filtering fallback results.")
                if not filtered_release_groups:
                    return []  # No relevant groups found after filtering
                release_groups = filtered_release_groups  # Use the filtered list
            else:
                # If primary search worked, we assume the artist match is already good
                self.console_logger.info(f"Using {len(release_groups)} results from primary MusicBrainz search.")

            # --- Process Filtered/Found Release Groups ---
            processed_release_ids: Set[str] = set()
            MAX_GROUPS_TO_PROCESS = 3  # Limit processing if multiple groups found
            groups_to_process = release_groups[:MAX_GROUPS_TO_PROCESS]
            release_fetch_tasks = []
            rg_info_map = {idx: groups_to_process[idx] for idx in range(len(groups_to_process))}  # Map index to RG data

            for idx, rg in enumerate(groups_to_process):
                rg_id = rg.get("id")
                if not rg_id:
                    continue
                release_params = {"release-group": rg_id, "inc": "media", "fmt": "json", "limit": "50"}
                release_search_url = "https://musicbrainz.org/ws/2/release/"
                release_fetch_tasks.append(self._make_api_request('musicbrainz', release_search_url, params=release_params))

            release_results = await asyncio.gather(*release_fetch_tasks, return_exceptions=True)

            # --- Score Fetched Releases ---
            for idx, result in enumerate(release_results):
                rg_info_full = rg_info_map.get(idx)  # Get full RG data using the index map
                if not rg_info_full:
                    continue
                rg_id = rg_info_full["rg_id"] = rg_info_full.get("id")  # Store ID in mapped dict too

                # Extract details needed for scoring/processing from rg_info_full
                rg_first_date = rg_info_full.get("first-release-date")
                rg_primary_type = rg_info_full.get("primary-type")
                rg_artist_credit = rg_info_full.get("artist-credit", [])
                rg_artist_name = ""
                if rg_artist_credit and isinstance(rg_artist_credit, list):
                    first_artist = rg_artist_credit[0]
                    if isinstance(first_artist, dict):
                        rg_artist_name = first_artist.get("name", "")

                if isinstance(result, Exception):
                    self.error_logger.warning(f"Failed to fetch releases for MB RG ID {rg_id}: {result}")
                    continue
                if not result or "releases" not in result:
                    if result is not None:
                        self.console_logger.warning(f"No release data found for MB RG ID {rg_id}")
                    continue

                releases = result.get("releases", [])
                for release in releases:
                    release_id = release.get("id")
                    if not release_id or release_id in processed_release_ids:
                        continue
                    processed_release_ids.add(release_id)

                    year: Optional[str] = None
                    release_date_str = release.get("date", "")
                    if release_date_str:
                        year_match = re.match(r'^(\d{4})', release_date_str)
                        if year_match:
                            year = year_match.group(1)

                    status_val = release.get("status")  # Getting the value, can be None
                    status = status_val.lower() if isinstance(status_val, str) else ""  # We translate to lowercase only if it is a string

                    country_val = release.get("country")  # Getting the value, can be None
                    country = country_val.lower() if isinstance(country_val, str) else ""  # Convert to lowercase only if it is a string

                    release_title = release.get("title", "")
                    release_title = release.get("title", "")
                    media = release.get("media", [])
                    format_details = media[0].get("format", "") if media and isinstance(media, list) else ""
                    reissue_keywords = self.scoring_config.get('reissue_keywords', []) if isinstance(self.scoring_config, dict) else []
                    is_reissue = any(kw.lower() in release_title.lower() for kw in reissue_keywords)

                    release_info = {
                        'source': 'musicbrainz',
                        'id': release_id,
                        'title': release_title,
                        'artist': rg_artist_name or artist_norm,  # Use artist from RG credit
                        'year': year,
                        'type': rg_primary_type or release.get("release-group", {}).get("primary-type"),  # Use RG type
                        'status': status,
                        'country': country,
                        'format_details': format_details,
                        'is_reissue': is_reissue,
                        'releasegroup_id': rg_id,
                        'releasegroup_first_date': rg_first_date,  # Pass RG first date
                        'score': 0,
                    }

                    if self._is_valid_year(release_info['year']):
                        release_info['score'] = self._score_original_release(release_info, artist_norm, album_norm, artist_region)
                        scored_releases.append(release_info)

            # --- Final Sorting and Logging ---
            scored_releases.sort(key=lambda x: x['score'], reverse=True)

            self.console_logger.info(f"Found {len(scored_releases)} scored releases from MusicBrainz for '{artist_norm} - {album_norm}'")
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"  MB #{i+1}: {r['title']} ({r['year']}) - Type: {r['type']}, Status: {r['status']}, "
                    f"Score: {r['score']} (RG First Date: {r.get('releasegroup_first_date')})"
                )

            return scored_releases

        except Exception as e:
            self.error_logger.exception(f"Error retrieving/scoring from MusicBrainz for '{artist_norm} - {album_norm}': {e}")
            return []

    # `_get_scored_releases_from_discogs` remains the same as provided in the previous response.
    async def _get_scored_releases_from_discogs(self, artist_norm: str, album_norm: str, artist_region: Optional[str]) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from Discogs.
        Uses a combined query parameter 'q' and includes basic result validation.
        Fixes Flake8 warnings F841 and E501.

        Args:
            artist_norm (str): Normalized artist name.
            album_norm (str): Normalized album name.
            artist_region (Optional[str]): Artist's region code for scoring.

        Returns:
            List[Dict[str, Any]]: List of scored release dictionaries, sorted by score descending.
        """
        scored_releases: List[Dict[str, Any]] = []
        try:
            # Use combined query parameter 'q'
            search_query = f"{artist_norm} {album_norm}"
            params = {"q": search_query, "type": "release", "per_page": "25"}
            search_url = "https://api.discogs.com/database/search"

            log_discogs_url = search_url + "?" + urllib.parse.urlencode(params, safe=':/')
            self.console_logger.debug(f"[discogs] Search URL: {log_discogs_url}")

            data = await self._make_api_request('discogs', search_url, params=params)

            # --- ADDED DEBUG LOGGING ---
            self.console_logger.debug(f"[discogs] Data received from _make_api_request: Type={type(data)}")
            if isinstance(data, dict):
                # Log keys and maybe first part of results if present
                self.console_logger.debug(f"[discogs] Received data keys: {list(data.keys())}")
                results_preview = data.get('results', 'N/A')
                if isinstance(results_preview, list):
                    self.console_logger.debug(
                        f"[discogs] Received {len(results_preview)} results. First result preview: {str(results_preview[:1])[:300]}"
                    )
                else:
                    self.console_logger.debug("[discogs] 'results' key not found or not a list.")
            elif data is None:
                self.console_logger.debug("[discogs] _make_api_request returned None.")
            # ---------------------------

            # --- Process Discogs Results (logic remains the same) ---
            if not data or "results" not in data:
                self.console_logger.warning(f"[discogs] Search failed or no results for query: '{search_query}'")
                return []

            results = data.get('results', [])
            if not results:
                self.console_logger.info(f"[discogs] No results found for query: '{search_query}'")
                return []

            self.console_logger.debug(f"Found {len(results)} potential Discogs matches for query: '{search_query}'")

            scoring_cfg = self.scoring_config if isinstance(self.scoring_config, dict) else {}
            reissue_keywords = scoring_cfg.get('reissue_keywords', [])

            for item in results:
                year_str = str(item.get('year', ''))
                release_title_full = item.get('title', '')
                title_parts = release_title_full.split(' - ', 1)
                release_artist = self._normalize_name(title_parts[0].strip()) if len(title_parts) > 1 else artist_norm
                actual_release_title = (
                    self._normalize_name(title_parts[1].strip()) if len(title_parts) > 1 else self._normalize_name(release_title_full)
                )

                # --- Stricter validation after broad search ---
                # Removed unused threshold variables
                artist_matches = artist_norm.lower() in release_artist.lower() or release_artist.lower() in artist_norm.lower()
                # Require high album title similarity or perfect artist match for less strict title check
                # Using simple containment check for now
                album_matches = album_norm.lower() in actual_release_title.lower() or actual_release_title.lower() in album_norm.lower()

                # Break long condition into multiple lines for readability (E501 fix)
                artist_mismatch_skip = not artist_matches
                album_mismatch_skip = not album_matches and (release_artist.lower() != artist_norm.lower())

                if artist_mismatch_skip:
                    self.console_logger.debug(
                        f"[discogs] Skipping result '{release_title_full}' due to artist mismatch " f"('{release_artist}' vs search '{artist_norm}')"
                    )
                    continue
                if album_mismatch_skip:
                    self.console_logger.debug(
                        f"[discogs] Skipping result '{release_title_full}' due to album mismatch "
                        f"('{actual_release_title}' vs search '{album_norm}') and imperfect artist match"
                    )
                    continue
                # ----------------------------------------------------

                formats = item.get('formats', [])
                format_names: List[str] = []
                format_descriptions: List[str] = []
                if isinstance(formats, list):
                    for fmt in formats:
                        if isinstance(fmt, dict):
                            if fmt.get('name'):
                                format_names.append(fmt['name'])
                            if fmt.get('descriptions'):
                                descriptions = fmt['descriptions']
                                if isinstance(descriptions, list):
                                    format_descriptions.extend(descriptions)

                format_details_str = ", ".join(format_names + format_descriptions)

                is_reissue = False
                title_lower = actual_release_title.lower()
                desc_lower = " ".join(format_descriptions).lower()
                if any(kw.lower() in title_lower for kw in reissue_keywords):
                    is_reissue = True
                if not is_reissue and any(kw.lower() in desc_lower for kw in reissue_keywords):
                    is_reissue = True
                if not is_reissue and any(d.lower() in ['reissue', 'remastered', 'repress', 'remaster'] for d in format_descriptions):
                    is_reissue = True

                release_type = "Album"
                format_names_lower = [fn.lower() for fn in format_names]
                desc_lower_list = [d.lower() for d in format_descriptions]
                if any(ft in format_names_lower for ft in ['lp', 'album']) or 'album' in desc_lower_list:
                    release_type = "Album"
                elif any(ft in format_names_lower for ft in ['ep']) or 'ep' in desc_lower_list:
                    release_type = "EP"
                elif any(ft in format_names_lower for ft in ['single']) or 'single' in desc_lower_list:
                    release_type = "Single"
                elif any(ft in format_names_lower for ft in ['compilation']) or 'compilation' in desc_lower_list:
                    release_type = "Compilation"

                release_info = {
                    'source': 'discogs',
                    'id': item.get('id'),
                    'title': actual_release_title,
                    'artist': release_artist,
                    'year': year_str,
                    'type': release_type,
                    'status': 'Official',
                    'country': item.get('country', '').lower(),
                    'format_details': format_details_str,
                    'is_reissue': is_reissue,
                    'score': 0,
                }

                if self._is_valid_year(release_info['year']):
                    release_info['score'] = self._score_original_release(release_info, artist_norm, album_norm, artist_region)
                    min_score = scoring_cfg.get('discogs_min_broad_score', 20)
                    if release_info['score'] > min_score:
                        scored_releases.append(release_info)

            scored_releases.sort(key=lambda x: x['score'], reverse=True)

            self.console_logger.info(f"Found {len(scored_releases)} scored releases from Discogs for query: '{search_query}'")
            for i, r in enumerate(scored_releases[:3]):
                # Break long log line (E501 fix)
                self.console_logger.info(
                    f"  Discogs #{i+1}: {r.get('title', '')} ({r.get('year', '')}) - "
                    f"Type: {r.get('type', '')}, Reissue: {r.get('is_reissue')}, "
                    f"Score: {r.get('score')}"
                )

            return scored_releases

        except Exception as e:
            self.error_logger.exception(f"Error retrieving/scoring from Discogs for '{artist_norm} - {album_norm}': {e}")
            return []

    # `_get_scored_releases_from_lastfm` remains the same as provided in the previous response.
    async def _get_scored_releases_from_lastfm(self, artist_norm: str, album_norm: str) -> List[Dict[str, Any]]:
        """
        Retrieve album release year from Last.fm and return a scored release dictionary.

        Args:
            artist_norm (str): Normalized artist name.
            album_norm (str): Normalized album name.

        Returns:
            List[Dict[str, Any]]: List containing zero or one scored release dictionary.
                                    Last.fm typically provides only one primary result per lookup.
        """
        scored_releases: List[Dict[str, Any]] = []
        if not self.use_lastfm:
            return []  # Skip if Last.fm is disabled

        try:
            self.console_logger.debug(f"Searching Last.fm for: '{artist_norm} - {album_norm}'")

            # Prepare API request parameters
            url = "https://ws.audioscrobbler.com/2.0/"
            params = {
                "method": "album.getInfo",
                "artist": artist_norm,  # Use normalized names for lookup consistency
                "album": album_norm,
                "api_key": self.lastfm_api_key,
                "format": "json",
                "autocorrect": "1",  # Enable Last.fm autocorrection
            }

            # Make the API request
            data = await self._make_api_request('lastfm', url, params=params)

            # Validate response
            if not data:
                self.console_logger.warning(f"Last.fm getInfo failed (no data) for '{artist_norm} - {album_norm}'")
                return []
            if "error" in data:  # Check for Last.fm specific errors
                self.console_logger.warning(f"Last.fm API error {data.get('error')}: {data.get('message', 'Unknown Last.fm error')}")
                return []
            if "album" not in data or not isinstance(data["album"], dict):
                # Ensure the main 'album' object exists
                self.console_logger.warning(f"Last.fm response missing 'album' object for '{artist_norm} - {album_norm}'")
                return []

            # Extract year information from the album data
            album_data = data["album"]
            year_info = self._extract_year_from_lastfm_data(album_data)  # Use helper function

            if year_info and year_info.get('year'):
                year = year_info['year']
                source_detail = year_info.get('source_detail', 'unknown')

                # Basic assumptions for Last.fm data (less detailed than MB/Discogs)
                release_type = "Album"  # Assume Album unless tags suggest otherwise (complex to implement reliably)
                status = "Official"  # Assume Official

                # Prepare data structure for scoring
                release_info = {
                    'source': 'lastfm',
                    'id': album_data.get('mbid'),  # MusicBrainz ID from Last.fm, if available
                    'title': album_data.get('name', album_norm),  # Use name from response
                    'artist': album_data.get('artist', artist_norm),  # Use artist from response
                    'year': year,
                    'type': release_type,
                    'status': status,
                    'country': '',  # Not provided by Last.fm getInfo
                    'format_details': '',  # Not provided
                    'is_reissue': False,  # Difficult to determine reliably
                    'source_detail': source_detail,  # Where the year came from (wiki, date, tags)
                    'score': 0,
                }

                # Score this single result (passing None for artist_region)
                if self._is_valid_year(release_info['year']):
                    release_info['score'] = self._score_original_release(release_info, artist_norm, album_norm, None)
                    scored_releases.append(release_info)
                    self.console_logger.info(
                        f"Scored LastFM Release: '{release_info['title']}' ({release_info['year']}) "
                        f"Source: {source_detail}, Score: {release_info['score']}"
                    )

        except Exception as e:
            # Catch any unexpected errors during the process
            self.error_logger.exception(f"Error retrieving/scoring from Last.fm for '{artist_norm} - {album_norm}': {e}")

        # Return the list (usually empty or with one scored item)
        return scored_releases

    # `_extract_year_from_lastfm_data` remains the same as provided in the previous response.
    def _extract_year_from_lastfm_data(self, album_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Extract the most likely year from the Last.fm album data structure.

        Prioritizes explicit release date, then wiki content patterns, then tags.

        Args:
            album_data (Dict[str, Any]): The 'album' object from the Last.fm API response.

        Returns:
            Optional[Dict[str, str]]: A dictionary {'year': YYYY, 'source_detail': 'source'} or None if no year found.
        """
        year: Optional[str] = None
        source_detail: str = "unknown"

        # --- Priority 1: Explicit 'releasedate' field ---
        release_date_str = album_data.get("releasedate", "").strip()
        if release_date_str:
            # Extract year (YYYY) using regex, handles various date formats
            year_match = re.search(r'\b(\d{4})\b', release_date_str)
            if year_match:
                potential_year = year_match.group(1)
                # Validate the extracted year
                if self._is_valid_year(potential_year):
                    year = potential_year
                    source_detail = "releasedate"
                    self.console_logger.debug(f"LastFM Year: {year} from 'releasedate' field")
                    return {'year': year, 'source_detail': source_detail}

        # --- Priority 2: Wiki Content ---
        wiki = album_data.get("wiki")
        # Check if wiki exists and is a dictionary with 'content'
        if wiki and isinstance(wiki, dict) and isinstance(wiki.get("content"), str) and wiki["content"].strip():
            wiki_content = wiki["content"]
            # Define patterns to search for year information, from more specific to less
            patterns = [
                # Specific phrases like "Originally released in/on YYYY"
                r'(?:originally\s+)?released\s+(?:in|on)\s+(?:(?:\d{1,2}\s+)?'
                r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|'
                r'Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+)?(\d{4})',
                # Phrases like "YYYY release" or "a YYYY album"
                r'\b(19\d{2}|20\d{2})\s+(?:release|album)\b',
                # Common date formats like "Month DD, YYYY" or "DD Month YYYY"
                r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|'
                r'Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+(\d{4})\b',
                r'\b\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|'
                r'Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{4})\b',
                # Year in parentheses (less reliable, lower priority)
                # r'\((\d{4})\)' # Commented out - too prone to errors (e.g., (C) 2005)
            ]
            for pattern in patterns:
                match = re.search(pattern, wiki_content, re.IGNORECASE)
                if match:
                    # Find the actual year group (could be group 1 or 2 depending on pattern)
                    potential_year = next((g for g in match.groups() if g is not None), None)
                    if self._is_valid_year(potential_year):
                        year = potential_year
                        source_detail = "wiki_content"
                        self.console_logger.debug(f"LastFM Year: {year} from wiki content (pattern: '{pattern}')")
                        return {'year': year, 'source_detail': source_detail}

        # --- Priority 3: Tags ---
        tags_container = album_data.get("tags")  # First, we get the value for 'tags'
        tags_data = []  # By default - an empty list

        # Check if tags_container is a dictionary before calling .get('tag')
        if isinstance(tags_container, dict):
            # If it's a dictionary, try to get 'tag', otherwise - an empty list
            tags_data = tags_container.get("tag", [])
        # If it's a list, we can use it directly
        elif isinstance(tags_container, list):
            tags_data = tags_container

        # Make sure that tags_data is a list (in case 'tag' contains a single dictionary, not a list)
        if isinstance(tags_data, dict):
            tags_data = [tags_data]
        elif not isinstance(tags_data, list):  # If it's still not a list, reset to empty
            tags_data = []

        if tags_data and isinstance(tags_data, list):
            year_tags = defaultdict(int)  # Count frequency of year tags
            for tag in tags_data:
                if isinstance(tag, dict) and 'name' in tag:
                    tag_name = tag['name']
                    # Check if tag is a 4-digit string representing a valid year
                    if len(tag_name) == 4 and tag_name.isdigit():
                        if self._is_valid_year(tag_name):
                            year_tags[tag_name] += 1

            if year_tags:
                # Choose the most frequent year tag
                # If tied, could optionally prefer earlier year, but most frequent is simpler
                best_year_tag = max(year_tags, key=year_tags.get)
                year = best_year_tag
                source_detail = "tags"
                self.console_logger.debug(f"LastFM Year: {year} from tags (most frequent)")
                return {'year': year, 'source_detail': source_detail}

        # --- No valid year found ---
        self.console_logger.debug(
            f"No year information extracted from Last.fm data for '{album_data.get('artist', '')} - {album_data.get('name', '')}'"
        )
        return None

    # `_normalize_name` remains the same as provided in the previous response.
    def _normalize_name(self, name: str) -> str:
        """
        Normalize a name by removing common suffixes and extra characters for better API matching.

        Args:
            name (str): The original name.

        Returns:
            str: Normalized name, or empty string if input is empty/None.
        """
        if not name or not isinstance(name, str):
            return ""

        # Patterns for suffixes/versions to remove (case-insensitive)
        # More specific patterns first
        suffixes_to_strip = [
            # Common Remaster/Edition markers in parens/brackets
            (
                r'\s+[\(\[](?:\d{4}\s+)?(?:Remaster(?:ed)?|Deluxe(?: Edition)?|'
                r'Expanded(?: Edition)?|Legacy Edition|Anniversary Edition|'
                r'Bonus Tracks?|Digital Version)[\)\]]'
            ),
            # Standalone Remaster/Deluxe tags (less common, more risky)
            # r'\s+-\s+(?:Remastered|Deluxe Edition)$',
            # EP/Single suffixes
            r'\s+(?:-\s+)?(?:EP|Single)$',
            r'\s+[\(\[]EP[\)\]]$',
            r'\s+[\(\[]Single[\)\]]$',
        ]
        result = name
        for suffix_pattern in suffixes_to_strip:
            # Use re.IGNORECASE for case-insensitive removal
            result = re.sub(suffix_pattern + r'\s*$', '', result, flags=re.IGNORECASE).strip()

        # General cleanup: remove most punctuation (keep hyphens), normalize whitespace
        # Keep basic Latin letters, numbers, whitespace, and hyphens
        result = re.sub(r'[^\w\s\-]', '', result)
        result = re.sub(r'\s+', ' ', result).strip()

        # Avoid returning empty string if original name wasn't empty
        return result if result else name

    # `_clean_expired_cache` remains the same as provided in the previous response.
    def _clean_expired_cache(self, cache: dict, ttl_seconds: float, cache_name: str = "Generic"):
        """
        Removes expired entries from a timestamped cache dictionary.

        Args:
            cache (dict): The cache dictionary to clean. Expected format: {key: (value, timestamp)}.
            ttl_seconds (float): The time-to-live in seconds.
            cache_name (str): Name of the cache for logging purposes.
        """
        if not isinstance(cache, dict):
            return  # Safety check

        current_time = time.monotonic()
        # Find keys of expired entries
        keys_to_delete = [
            key
            for key, (_, timestamp) in cache.items()
            # Ensure timestamp is a number before comparison
            if isinstance(timestamp, (int, float)) and (current_time - timestamp >= ttl_seconds)
        ]

        if keys_to_delete:
            for key in keys_to_delete:
                try:
                    del cache[key]
                except KeyError:
                    pass  # Ignore if key already removed elsewhere
            self.console_logger.debug(f"Cleaned {len(keys_to_delete)} expired entries from {cache_name} cache.")

    # `_is_valid_year` remains the same as provided in the previous response.
    def _is_valid_year(self, year_str: Optional[str]) -> bool:
        """
        Basic check if a string represents a plausible release year (4 digits, within range).

        Args:
            year_str (Optional[str]): The year string to validate.

        Returns:
            bool: True if the string is a valid 4-digit year within the configured range, False otherwise.
        """
        if not year_str or not isinstance(year_str, str) or not year_str.isdigit() or len(year_str) != 4:
            return False
        try:
            year = int(year_str)
            # Check against configured min year and allow a few years into the future
            return self.min_valid_year <= year <= (self.current_year + 5)
        except ValueError:
            # Should not happen due to isdigit, but included for safety
            return False
