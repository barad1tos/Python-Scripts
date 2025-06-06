#!/usr/bin/env python3

"""External API Service for Music Metadata.

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
- **Artist Context:** Utilizes artist activity period and region (fetched from MusicBrainz and cached via CacheService)
    to improve scoring accuracy.
- **Data Aggregation:** Combines results from multiple APIs for a more robust determination.
- **Normalization & Validation:** Normalizes artist/album names for better matching and validates years.
- **Error Handling & Retries:** Implements robust error handling and retry logic for API requests.
- **Dependency Injection:** Uses injected services for caching and verification tracking rather than
    managing these concerns internally.
- **Statistics & Logging:** Tracks API usage statistics and provides detailed logging for diagnostics.
- **Prerelease Handling:** Includes logic (`should_update_album_year`) to avoid updating years for albums
    marked as prerelease in the user's library, marking them for future verification.

Classes:
- `EnhancedRateLimiter`: A standalone rate limiter class.
- `ExternalApiService`: The main service class orchestrating API interactions and year determination.

Dependencies:
- `CacheService`: Provides caching functionality for artist metadata.
- `PendingVerificationService`: Tracks albums that need future verification.
"""

import asyncio
import logging
import random
import re
import time
import urllib.parse

from collections import defaultdict
from datetime import datetime
from typing import Any

import aiohttp

from services.cache_service import CacheService
from services.pending_verification import PendingVerificationService

# --- Constants ---
WAIT_TIME_LOG_THRESHOLD = 0.1
HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVER_ERROR = 500
MAX_LOGGED_YEARS = 5
YEAR_LENGTH = 4
REGION_CODE_LENGTH = 2

# Use a cryptographically secure random generator for jitter
SECURE_RANDOM = random.SystemRandom()

class EnhancedRateLimiter:
    """Advanced rate limiter using a moving window approach.

    This ensures maximum throughput while strictly adhering to API rate limits.
    """

    def __init__(
        self,
        requests_per_window: int,
        window_size: float,
        max_concurrent: int = 3,
        logger: logging.Logger | None = None,
    ):
        """Initialize the rate limiter."""
        if not isinstance(requests_per_window, int) or requests_per_window <= 0:
            raise ValueError("requests_per_window must be a positive integer")
        if not isinstance(window_size, int | float) or window_size <= 0:
            raise ValueError("window_size must be a positive number")
        if not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")

        self.requests_per_window = requests_per_window
        self.window_size = float(window_size)
        # Using list as deque for simplicity, but collections.deque might be more performant for large windows
        self.request_timestamps: list[float] = []
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.logger = logger or logging.getLogger(__name__)
        self.total_requests: int = 0
        self.total_wait_time: float = 0.0

    async def acquire(self) -> float:
        """Acquire permission to make a request, waiting if necessary due to rate limits or concurrency limits."""
        # Wait for rate limit first
        rate_limit_wait_time = await self._wait_if_needed()
        self.total_requests += 1
        self.total_wait_time += rate_limit_wait_time

        # Then wait for concurrency semaphore
        await self.semaphore.acquire()

        return rate_limit_wait_time

    def release(self) -> None:
        """Release the concurrency semaphore after the request is completed or failed."""
        self.semaphore.release()

    async def _wait_if_needed(self) -> float:
        """Check rate limits and wait if necessary."""
        now = time.monotonic()  # Use monotonic clock for interval timing

        # Clean up old timestamps outside the window
        # This check ensures we don't keep growing the list indefinitely
        # It iterates from the left (oldest) and stops when a timestamp is within the window
        while (
            self.request_timestamps
            and now - self.request_timestamps[0] > self.window_size
        ):
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

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about rate limiter usage."""
        # Ensure current window usage is up-to-date by cleaning expired timestamps
        now = time.monotonic()
        self.request_timestamps = [
            ts for ts in self.request_timestamps if now - ts <= self.window_size
        ]

        return {
            "total_requests": self.total_requests,
            "total_wait_time": self.total_wait_time,
            "avg_wait_time": self.total_wait_time
            / max(1, self.total_requests),  # Avoid division by zero
            "current_window_usage": len(self.request_timestamps),
            "max_requests_per_window": self.requests_per_window,
        }


class ExternalApiService:
    """Enhanced service for interacting with MusicBrainz, Discogs, and Last.fm APIs.

    Uses `EnhancedRateLimiter` for precise rate control, revised scoring for year determination,
    and includes artist context (activity period, region) for better accuracy.
    """

    def __init__(
        self,
        config: dict[str, Any],
        console_logger: logging.Logger,
        error_logger: logging.Logger,
        # >>> Added dependencies as constructor arguments
        cache_service: CacheService,
        pending_verification_service: PendingVerificationService,
        # <<< End added arguments
    ):
        """Initialize the API service with configuration, loggers, and dependencies."""
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.session: aiohttp.ClientSession | None = None

        # >>> Stored dependencies as instance attributes
        self.cache_service = cache_service
        self.pending_verification_service = pending_verification_service
        # <<< End storing attributes

        # --- Configuration Extraction ---
        year_config = config.get("year_retrieval", {})
        if not isinstance(year_config, dict):
            # Log critical error and raise, as service cannot function without this section
            self.error_logger.critical(
                "Configuration error: 'year_retrieval' section missing or invalid."
            )
            raise ValueError(
                "Configuration error: 'year_retrieval' section missing or invalid."
            )

        # --- Correctly read nested API Auth settings ---
        api_auth_config = year_config.get("api_auth", {})
        if not isinstance(api_auth_config, dict):
            self.error_logger.critical(
                "Configuration error: 'year_retrieval.api_auth' subsection missing or invalid."
            )
            raise ValueError(
                "Configuration error: 'year_retrieval.api_auth' subsection missing or invalid."
            )

        # --- Receive and validate the Discogs token ---
        self.discogs_token = api_auth_config.get("discogs_token")
        if not self.discogs_token or self.discogs_token.startswith("${"):
            self.error_logger.error(
                "Discogs token is missing or not properly loaded from environment variables"
            )

        # --- Receive and validate the contact email (required for MusicBrainz) ---
        self.musicbrainz_app_name = api_auth_config.get(
            "musicbrainz_app_name", "MusicGenreUpdater/UnknownVersion"
        )
        self.contact_email = api_auth_config.get("contact_email")
        if not self.contact_email or self.contact_email.startswith("${"):
            self.error_logger.error(
                "Contact email is missing or not properly loaded from environment variables"
            )
            self.contact_email = "no-email-provided@example.com"  # Fallback value

        # --- Get and validate the Last.fm API key ---
        self.lastfm_api_key = api_auth_config.get("lastfm_api_key")
        self.use_lastfm = bool(
            self.lastfm_api_key and not self.lastfm_api_key.startswith("${")
        ) and year_config.get("use_lastfm", True)

        # --- User-Agent setup ---
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
            concurrent_calls_value = rate_limits_config.get("concurrent_api_calls", 5)
            # Ensure we always have a valid positive integer
            concurrent_calls = (
                max(1, int(concurrent_calls_value))
                if concurrent_calls_value is not None
                else 5
            )
            self.rate_limiters = {
                "discogs": EnhancedRateLimiter(
                    requests_per_window=max(
                        1, rate_limits_config.get("discogs_requests_per_minute", 25)
                    ),
                    window_size=60.0,
                    max_concurrent=concurrent_calls,
                    logger=console_logger,
                ),
                "musicbrainz": EnhancedRateLimiter(
                    requests_per_window=max(
                        1, rate_limits_config.get("musicbrainz_requests_per_second", 1)
                    ),
                    window_size=1.0,
                    max_concurrent=concurrent_calls,
                    logger=console_logger,
                ),
                "lastfm": EnhancedRateLimiter(
                    requests_per_window=max(
                        1, rate_limits_config.get("lastfm_requests_per_second", 5)
                    ),
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
        self.min_valid_year = logic_config.get("min_valid_year", 1900)
        self.definitive_score_threshold = logic_config.get(
            "definitive_score_threshold", 85
        )
        self.definitive_score_diff = logic_config.get("definitive_score_diff", 15)
        self.current_year = datetime.now().year
        # ----------------------------------

        # --- Caching ---
        # Removed activity_cache and region_cache from here.
        # ExternalApiService will now use methods from the injected CacheService
        # self.activity_cache: Dict[str, Tuple[Tuple[Optional[int], Optional[int]], float]] = {}
        # self.region_cache: Dict[str, Tuple[Optional[str], float]] = {}
        self.cache_ttl_days = processing_config.get(
            "cache_ttl_days", 30
        )  # Read from processing subsection
        self.artist_period_context: dict[str, int | None] | None = None
        # ----------------------------------

        # --- Statistics Tracking ---
        self.request_counts = {"discogs": 0, "musicbrainz": 0, "lastfm": 0}
        self.api_call_durations: dict[str, list[float]] = {
            "discogs": [],
            "musicbrainz": [],
            "lastfm": [],
        }
        # ----------------------------------

        # Ensure scoring_config is a dict (already done above)
        if not isinstance(self.scoring_config, dict):
            self.console_logger.warning(
                "Scoring configuration missing or invalid, using default scores."
            )
            self.scoring_config = {}

        # Removed this line - initialize_async is a method, not an attribute to be reassigned
        # self.external_api_service.initialize_async = self.external_api_service.initialize

    async def initialize(self, force: bool = False) -> None:
        """Initialize the aiohttp ClientSession."""
        if force and self.session and not self.session.closed:
            await self.session.close()
            self.session = None  # Ensure it's recreated below

        if not self.session or self.session.closed:
            # Sensible timeouts
            timeout = aiohttp.ClientTimeout(
                total=45, connect=15, sock_connect=15, sock_read=30
            )
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
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            }  # Request compression

            self.session = aiohttp.ClientSession(
                timeout=timeout, connector=connector, headers=headers
            )
            self.console_logger.info(
                f"External API session initialized with User-Agent: {self.user_agent}"
                + (" (forced)" if force else "")
            )

    async def close(self) -> None:
        """Close the aiohttp ClientSession and log API usage statistics."""
        if self.session and not self.session.closed:
            # Log API statistics before closing
            self.console_logger.info("--- API Call Statistics ---")
            total_api_calls = 0
            total_api_time = 0.0
            for api_name, limiter in self.rate_limiters.items():
                # Ensure stats are up-to-date before logging
                stats = limiter.get_stats()
                durations = self.api_call_durations.get(api_name, [])
                avg_duration = (
                    sum(durations) / max(1, len(durations)) if durations else 0.0
                )
                total_api_calls += stats["total_requests"]
                total_api_time += sum(durations)
                self.console_logger.info(
                    f"API: {api_name.title():<12} | "
                    f"Requests: {stats['total_requests']:<5} | "
                    f"Avg Wait: {stats['avg_wait_time']:.3f}s | "  # Increased precision
                    f"Avg Duration: {avg_duration:.3f}s"  # Increased precision
                )

            if total_api_calls > 0:
                avg_total_duration = total_api_time / total_api_calls
                self.console_logger.info(
                    f"Total API Calls: {total_api_calls}, Average Call Duration: {avg_total_duration:.3f}s"
                )
            else:
                self.console_logger.info("No API calls were made during this session.")
            self.console_logger.info("---------------------------")

            await self.session.close()
            self.console_logger.info("External API session closed")

    async def _make_api_request(
        self,
        api_name: str,
        url: str,
        params: dict[str, str] | None = None,
        headers_override: dict[str, str] | None = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
        timeout_override: float | None = None,
    ) -> dict[str, Any] | None:
        """Make an API request with rate limiting, error handling, and retry logic."""
        cache_key = (
            "api_request",
            api_name,
            url,
            tuple(sorted((params or {}).items())),
        )
        cache_ttl_seconds = self.cache_ttl_days * 86400

        cached_response = await self.cache_service.get_async(cache_key)
        if cached_response is not None:
            if isinstance(cached_response, dict):  # Ensure it's a dictionary
                if cached_response != {}:
                    self.console_logger.debug(
                        f"Using cached response for {api_name} request to {url}"
                    )
                    return cached_response
                self.console_logger.debug(
                    f"Cached empty response for {api_name} request to {url}"
                )
                return None
            self.console_logger.warning(
                f"Unexpected cached response type for {api_name} request to {url}: {type(cached_response).__name__}"
            )
            # Clear invalid cache entry
            self.cache_service.invalidate(cache_key)
        if not self.session or self.session.closed:
            self.error_logger.error(
                f"[{api_name}] Session not available for request to {url}. Initialize method was not called or failed."
            )
            return None

        request_headers = self.session.headers.copy()
        if api_name == "discogs":
            if not self.discogs_token or self.discogs_token.startswith("${"):
                self.error_logger.error(
                    "Discogs token is missing or not properly loaded from environment variables"
                )
                return None
            request_headers["Authorization"] = f"Discogs token={self.discogs_token}"
            if "User-Agent" not in request_headers and hasattr(self, "user_agent"):
                request_headers["User-Agent"] = self.user_agent

        if headers_override:
            request_headers.update(headers_override)

        limiter = self.rate_limiters.get(api_name)
        if not limiter:
            self.error_logger.error(f"No rate limiter configured for API: {api_name}")
            return None

        request_timeout = (
            aiohttp.ClientTimeout(total=timeout_override)
            if timeout_override
            else self.session.timeout
        )
        last_exception: Exception | None = None
        log_url = url + (f"?{urllib.parse.urlencode(params or {}, safe=':/')}" if params else "")
        result = None

        for attempt in range(max_retries + 1):
            wait_time = 0.0
            start_time = 0.0
            elapsed = 0.0
            response_status = -1
            response_text_snippet = "[No Response Body]"

            try:
                wait_time = await limiter.acquire()
                if wait_time > WAIT_TIME_LOG_THRESHOLD:
                    self.console_logger.debug(
                        f"[{api_name}] Waited {wait_time:.3f}s for rate limiting"
                    )

                self.request_counts[api_name] = self.request_counts.get(api_name, 0) + 1
                start_time = time.monotonic()

                async with self.session.get(
                    url, params=params, headers=request_headers, timeout=request_timeout
                ) as response:
                    elapsed = time.monotonic() - start_time
                    response_status = response.status
                    self.api_call_durations[api_name].append(elapsed)

                    if api_name == "discogs":
                        self.console_logger.debug(
                            f"[discogs] Sending Headers: {request_headers}"
                        )

                    # Read response text
                    response_text_snippet = "[Could not read text]"
                    try:
                        raw_text = await response.text(encoding="utf-8", errors="ignore")
                        response_text_snippet = raw_text[:500] if api_name == "discogs" else raw_text[:200]

                        if api_name == "discogs":
                            self.console_logger.debug(
                                f"====== DISCOGS RAW RESPONSE (Status: {response_status}) ======"
                            )
                            self.console_logger.debug(response_text_snippet)
                            self.console_logger.debug("====== END DISCOGS RAW RESPONSE ====== ")
                    except Exception as read_err:
                        response_text_snippet = f"[Error Reading Response: {read_err}]"
                        self.error_logger.warning(
                            f"[{api_name}] Failed to read response body: {read_err}"
                        )

                    self.console_logger.debug(
                        f"[{api_name}] Request (Attempt {attempt + 1}): {log_url} - "
                        f"Status: {response_status} ({elapsed:.3f}s)"
                    )

                    # Handle response status
                    if response_status == HTTP_TOO_MANY_REQUESTS or response_status >= HTTP_SERVER_ERROR:
                        last_exception = aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response_status,
                            message=response_text_snippet,
                        )
                        if attempt < max_retries:
                            delay = base_delay * (2**attempt) * (0.8 + SECURE_RANDOM.random() * 0.4)
                            if retry_after := response.headers.get("Retry-After"):
                                try:
                                    delay = max(delay, int(retry_after))
                                    self.console_logger.warning(
                                        f"[{api_name}] Respecting Retry-After header: {retry_after}s"
                                    )
                                except ValueError:
                                    pass
                            self.console_logger.warning(
                                f"[{api_name}] Status {response_status}, retrying {attempt + 1}/{max_retries} "
                                f"in {delay:.2f}s. URL: {url}"
                            )
                            await asyncio.sleep(delay)
                            continue
                        break

                    if not response.ok:
                        last_exception = aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response_status,
                            message=response_text_snippet,
                        )
                        self.error_logger.warning(
                            f"[{api_name}] API request failed with status {response_status}. "
                            f"URL: {url}. Snippet: {response_text_snippet}"
                        )
                        break

                    # Process successful response
                    if "application/json" in response.headers.get("Content-Type", ""):
                        result = await self._parse_json_response(response, api_name, url, response_text_snippet)
                    else:
                        self.error_logger.warning(
                            f"[{api_name}] Received non-JSON response from {url}. "
                            f"Content-Type: {response.headers.get('Content-Type')}"
                        )
                    break

            except (TimeoutError, aiohttp.ClientError) as e:
                elapsed = time.monotonic() - start_time if start_time > 0 else 0.0
                self.api_call_durations[api_name].append(elapsed)
                last_exception = e

                if attempt < max_retries and isinstance(e, aiohttp.ClientConnectorError | aiohttp.ServerDisconnectedError | asyncio.TimeoutError):
                    delay = base_delay * (2**attempt) * (0.8 + SECURE_RANDOM.random() * 0.4)
                    self.console_logger.warning(
                        f"[{api_name}] {type(e).__name__}, retrying {attempt + 1}/{max_retries} in {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)
                    continue

                # If we get here, we've either exceeded max retries or it's not a retryable error
                self.error_logger.error(
                    f"[{api_name}] Request failed after {attempt + 1} attempts: {e}"
                )
                break

            except Exception as e:
                elapsed = time.monotonic() - start_time if start_time > 0 else 0.0
                self.api_call_durations[api_name].append(elapsed)
                self.error_logger.exception(
                    f"[{api_name}] Unexpected error making request to {url}: {e}"
                )
                last_exception = e
                break

            finally:
                if limiter:
                    limiter.release()

        if result is None and last_exception:
            self.error_logger.error(
                f"[{api_name}] Request failed for URL: {url}. Last exception: {last_exception}"
            )
        await self.cache_service.set_async(
            cache_key,
            result if result is not None else {},
            ttl=cache_ttl_seconds,
        )
        return result

    async def _parse_json_response(
        self,
        response: aiohttp.ClientResponse,
        api_name: str,
        url: str,
        snippet: str
    ) -> dict[str, Any] | None:
        """Parse JSON response and ensure it is a dict, logging if not.

        Args:
            response: The aiohttp response object
            api_name: Name of the API for logging
            url: The request URL for logging
            snippet: Response text snippet for error logging

        Returns:
            Parsed JSON as dict if successful, None otherwise

        """
        try:
            data = await response.json()
            if isinstance(data, dict):
                return data
            self.error_logger.warning(
                f"[{api_name}] JSON response is not a dict (type: {type(data).__name__}) from {url}. "
                f"Snippet: {str(data)[:200]}"
            )
        except Exception as exc:
            self.error_logger.error(
                f"[{api_name}] Error parsing JSON response from {url}: {exc}. "
                f"Snippet: {snippet[:200]}"
            )
        return None

    async def _safe_mark_for_verification(
        self,
        artist: str,
        album: str,
        *,
        fire_and_forget: bool = False,
    ) -> None:
        """Safely mark an album for verification, optionally fire-and-forget."""
        if not self.pending_verification_service:
            return
        try:
            if fire_and_forget:
                task = asyncio.create_task(
                    self.pending_verification_service.mark_for_verification(
                        artist,
                        album,
                    )
                )
                if not hasattr(self, "_pending_tasks"):
                    self._pending_tasks = set()
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
            else:
                await self.pending_verification_service.mark_for_verification(
                    artist,
                    album,
                )
        except Exception as exc:
            self.error_logger.error(
                f"Failed to mark '{artist} - {album}' for verification: {exc}"
            )

    def should_update_album_year(
        self,
        tracks: list[dict[str, str]],
        artist: str = "",
        album: str = "",
        current_library_year: str = "",
        # Removed pending_verification_service from here, use self.pending_verification_service
        # pending_verification_service=None,
    ) -> bool:
        """Determine whether to update the year for an album based on the status of its tracks."""
        if not tracks:
            return True

        prerelease_count = sum(
            1
            for track in tracks
            if track.get("trackStatus", "").lower() == "prerelease"
        )
        subscription_count = sum(
            1
            for track in tracks
            if track.get("trackStatus", "").lower() == "subscription"
        )
        total_tracks = len(tracks)

        # If there is at least one track in prerelease status
        if prerelease_count > 0:
            # If there are no subscription tracks, OR if prerelease tracks are the majority (>50%)
            if subscription_count == 0 or prerelease_count * 2 > total_tracks:
                self.console_logger.info(
                    f"Album '{artist} - {album}' has {prerelease_count}/{total_tracks} tracks in prerelease status. "
                    f"Keeping current year: {current_library_year or 'N/A'}"
                )

                # Mark for future verification using the helper
                task = asyncio.create_task(
                    self._safe_mark_for_verification(
                        artist,
                        album,
                        fire_and_forget=True,
                    )
                )
                if not hasattr(self, "_pending_tasks"):
                    self._pending_tasks = set()
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)

                return False  # Do not update year if prerelease conditions met

        return True  # OK to update year based on track status

    async def get_album_year(
        self,
        artist: str,
        album: str,
        current_library_year: str | None = None,
        # Removed pending_verification_service from here, use self.pending_verification_service
        # pending_verification_service=None
    ) -> tuple[str | None, bool]:
        """Determine the original release year for an album using optimized API calls and revised scoring."""
        # Normalize inputs for API matching but keep original for logging/reporting
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)
        log_artist = (
            artist if artist != artist_norm else artist_norm
        )  # Prefer original if different
        log_album = album if album != album_norm else album_norm

        self.console_logger.info(
            f"Searching for original release year: '{log_artist} - {log_album}' (current: {current_library_year or 'none'})"
        )

        # --- Pre-computation Steps ---
        start_year: int | None = None
        end_year: int | None = None
        artist_region: str | None = None
        try:
            # 1. Get artist's activity period for context (cached)
            # Use self.cache_service
            start_year, end_year = await self.get_artist_activity_period(artist_norm)
            self.artist_period_context = {
                "start_year": start_year,
                "end_year": end_year,
            }  # Set context for this run
            activity_log = (
                f"({start_year or '?'} - {end_year or 'present'})"
                if start_year or end_year
                else "(activity period unknown)"
            )
            self.console_logger.info(f"Artist activity period context: {activity_log}")

            # 2. Get artist's likely region for scoring context (cached)
            # Use self.cache_service
            artist_region = await self._get_artist_region(artist_norm)
            if artist_region:
                self.console_logger.info(
                    f"Artist region context: {artist_region.upper()}"
                )
        except Exception as context_err:
            self.error_logger.warning(
                f"Error fetching artist context for '{log_artist}': {context_err}"
            )
            # Continue without context, scoring will be less accurate

        # --- Fetch Data Concurrently ---
        result_year: str | None = None
        is_definitive = False
        try:
            api_tasks = [
                self._get_scored_releases_from_musicbrainz(
                    artist_norm,
                    album_norm,
                    artist_region,
                ),
                self._get_scored_releases_from_discogs(
                    artist_norm,
                    album_norm,
                    artist_region,
                ),
            ]
            if self.use_lastfm:
                api_tasks.append(
                    self._get_scored_releases_from_lastfm(artist_norm, album_norm)
                )

            results = await asyncio.gather(*api_tasks, return_exceptions=True)

            all_releases: list[dict[str, Any]] = []
            api_sources_found: set[str] = set()

            api_names = (
                ["musicbrainz", "discogs", "lastfm"]
                if self.use_lastfm
                else ["musicbrainz", "discogs"]
            )
            for i, result in enumerate(results):
                api_name = api_names[i]
                if isinstance(result, Exception):
                    # Log API call failures clearly
                    self.error_logger.warning(
                        f"API call to {api_name} failed for '{log_artist} - {log_album}': {type(result).__name__}: {result}"
                    )
                elif isinstance(result, list) and result:
                    all_releases.extend(result)
                    api_sources_found.add(api_name)
                    self.console_logger.info(
                        f"Received {len(result)} scored releases from {api_name.title()}"
                    )
                elif not result:  # Explicitly check for empty list/None
                    self.console_logger.info(
                        f"No results from {api_name.title()} for '{log_artist} - {log_album}'"
                    )

            if not all_releases:
                self.console_logger.warning(
                    f"No release data found from any API for '{log_artist} - {log_album}'"
                )
                await self._safe_mark_for_verification(artist, album)
                if current_library_year and self._is_valid_year(current_library_year):
                    result_year = current_library_year
                return result_year, is_definitive

            # --- Aggregate Scores by Year ---
            year_scores = defaultdict(list)  # Store list of scores for each year
            valid_years_found: set[str] = set()

            for release in all_releases:
                year = str(release["year"]) if release.get("year") is not None else None
                score = release.get("score", 0)
                # Check if year is valid before adding
                if year and self._is_valid_year(year):
                    year_scores[year].append(score)
                    valid_years_found.add(year)
                # else: # Optional: log discarded invalid years
                #    self.console_logger.debug(f"Discarding release with invalid year: {release.get('title')} ({year})")

            if not year_scores:
                self.console_logger.warning(
                    f"No valid years found after processing API results for '{log_artist} - {log_album}'"
                )
                await self._safe_mark_for_verification(artist, album)
                if current_library_year and self._is_valid_year(current_library_year):
                    result_year = current_library_year
                return result_year, is_definitive

            # --- Determine Best Year based on Aggregated Scores ---

            # Calculate the maximum score achieved for each year found
            final_year_scores: dict[str, int] = {
                year: max(scores) for year, scores in year_scores.items()
            }

            # Sort years: primarily by score (desc), secondarily by year (asc - prefer earlier for ties)
            sorted_years = sorted(
                final_year_scores.items(), key=lambda item: (-item[1], int(item[0]))
            )

            # Log the ranked years and their highest scores
            log_scores = ", ".join(
                [f"{y}:{s}" for y, s in sorted_years[:MAX_LOGGED_YEARS]]
            )  # Log top entries for brevity
            self.console_logger.info(
                f"Ranked year scores (Year:MaxScore): {log_scores}"
                + ("..." if len(sorted_years) > MAX_LOGGED_YEARS else "")
            )

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
                self.console_logger.debug(
                    f"Score difference to next best year: {score_difference} (Threshold: {self.definitive_score_diff})"
                )
            else:
                self.console_logger.debug("Only one candidate year found.")

            # Final definitive status depends on both conditions
            is_definitive = high_score_met and significant_diff_met

            self.console_logger.info(
                f"Selected year: {best_year} (Score: {best_score}). "
                f"Definitive? {is_definitive} (Score Met: {high_score_met}, Diff Met: {significant_diff_met})"
            )

            if not is_definitive:
                await self._safe_mark_for_verification(artist, album)

            result_year = best_year

        except Exception as e:
            self.error_logger.exception(
                f"Unexpected error in get_album_year for '{log_artist} - {log_album}': {e}"
            )
            if current_library_year and self._is_valid_year(current_library_year):
                result_year = current_library_year
            is_definitive = False
        finally:
            # Crucial: Clear the context after the function finishes or errors out
            self.artist_period_context = None

        return result_year, is_definitive

    async def get_artist_activity_period(
        self, artist_norm: str
    ) -> tuple[int | None, int | None]:
        """Retrieve the period of activity for an artist from MusicBrainz, with caching."""
        cache_key = f"activity_{artist_norm}"
        cache_ttl_seconds = self.cache_ttl_days * 86400

        # Check cache first using self.cache_service
        # CacheService.get_async handles the TTL check internally
        cached_data = await self.cache_service.get_async(cache_key)
        if cached_data:
            start_year, end_year = cached_data
            self.console_logger.debug(
                f"Using cached activity period for '{artist_norm}': {start_year or '?'} - {end_year or 'present'}"
            )
            return start_year, end_year
        else:
            self.console_logger.debug(
                f"Activity cache miss for '{artist_norm}', fetching from MusicBrainz."
            )

        start_year_result: int | None = None
        end_year_result: int | None = None

        try:
            # Search MusicBrainz for the artist
            params: dict[str, str] = {
                "query": f'artist:"{artist_norm}"',
                "fmt": "json",
                "limit": "10",  # Ensure all values are strings to match the type hint
            }  # Limit results
            search_url = "https://musicbrainz.org/ws/2/artist/"
            data = await self._make_api_request(
                "musicbrainz", search_url, params={k: str(v) for k, v in params.items()}
            )

            if not data or "artists" not in data or not data["artists"]:
                self.console_logger.warning(
                    f"Cannot determine activity period for '{artist_norm}': Artist not found in MusicBrainz."
                )
                # Cache the negative result using self.cache_service
                await self.cache_service.set_async(
                    cache_key, (None, None), ttl=3600
                )  # Cache negative result for 1 hour
                return None, None

            # --- Find the best matching artist ---
            best_match_artist = data["artists"][0]
            artist_name_found = best_match_artist.get("name", "Unknown")
            artist_id = best_match_artist.get("id")

            # Optional: Log if the best match name differs significantly
            if artist_name_found.lower() != artist_norm.lower():
                self.console_logger.debug(
                    f"Best MusicBrainz match for '{artist_norm}' is '{artist_name_found}' (ID: {artist_id})"
                )

            # --- Extract lifespan ---
            life_span = best_match_artist.get("life-span", {})
            if isinstance(life_span, dict):  # Ensure it's a dictionary
                begin_date_str = life_span.get("begin")
                end_date_str = life_span.get("end")
                ended = life_span.get(
                    "ended", False
                )  # Check if the 'ended' flag is true

                # Parse start year
                if begin_date_str:
                    try:
                        # Extract only the year part (YYYY)
                        year_part = begin_date_str.split("-")[0]
                        if len(year_part) == YEAR_LENGTH and year_part.isdigit():
                            start_year_result = int(year_part)
                        else:
                            self.console_logger.warning(
                                f"Could not parse year from begin date '{begin_date_str}' for {artist_name_found}"
                            )
                    except (ValueError, IndexError, TypeError):
                        self.console_logger.warning(
                            f"Error parsing begin date '{begin_date_str}' for {artist_name_found}"
                        )

                # Parse end year
                if end_date_str:
                    try:
                        year_part = end_date_str.split("-")[0]
                        if len(year_part) == YEAR_LENGTH and year_part.isdigit():
                            end_year_result = int(year_part)
                        else:
                            self.console_logger.warning(
                                f"Could not parse year from end date '{end_date_str}' for {artist_name_found}"
                            )
                    except (ValueError, IndexError, TypeError):
                        self.console_logger.warning(
                            f"Error parsing end date '{end_date_str}' for {artist_name_found}"
                        )
                elif not ended and start_year_result is not None:
                    # If the 'ended' flag is explicitly false or missing, and we have a start year,
                    # assume the artist is still active (end_year remains None).
                    end_year_result = None
                elif ended and start_year_result is not None:
                    # If 'ended' is true but no end_date_str is present, it's ambiguous.
                    # We could try to infer from last release, but for simplicity, let's treat as active (None).
                    self.console_logger.debug(
                        f"Artist '{artist_name_found}' marked as ended but no end date found in life-span."
                    )
                    end_year_result = None  # Treat as still active or recently ended

            # Store result (even if None) in cache using self.cache_service
            # Cache positive results with the configured TTL, negative/partial results with shorter TTL
            if start_year_result is not None or end_year_result is not None:
                await self.cache_service.set_async(
                    cache_key,
                    (start_year_result, end_year_result),
                    ttl=cache_ttl_seconds,
                )
            else:
                await self.cache_service.set_async(
                    cache_key, (None, None), ttl=3600
                )  # Cache absence of result for 1 hour

            self.console_logger.debug(
                f"Determined activity period for '{artist_norm}': {start_year_result or '?'} - {end_year_result or 'present'}"
            )
            return start_year_result, end_year_result

        except Exception as e:
            self.error_logger.exception(
                f"Error determining activity period for '{artist_norm}': {e}"
            )
            # Cache failure case using self.cache_service
            await self.cache_service.set_async(
                cache_key, (None, None), ttl=3600
            )  # Cache failure for 1 hour
            return None, None

    async def _get_artist_region(self, artist_norm: str) -> str | None:
        """Determine the region (country) of an artist using MusicBrainz API, with caching."""
        cache_key = f"region_{artist_norm}"
        cache_ttl_seconds = (
            self.cache_ttl_days * 86400
        )  # Use same TTL as activity period

        # Check cache first using self.cache_service
        # CacheService.get_async handles the TTL check internally
        cached_data = await self.cache_service.get_async(cache_key)
        if cached_data and isinstance(cached_data, str):
            region: str = cached_data
            self.console_logger.debug(
                f"Using cached region '{region}' for artist '{artist_norm}'"
            )
            return region
        else:
            self.console_logger.debug(
                f"Region cache miss for '{artist_norm}', fetching from MusicBrainz."
            )

        region_result: str | None = None
        try:
            # Query MusicBrainz API for artist data, limiting to the top result
            params = {"query": f'artist:"{artist_norm}"', "fmt": "json", "limit": 1}
            search_url = "https://musicbrainz.org/ws/2/artist/"
            data = await self._make_api_request(
                "musicbrainz", search_url, params={k: str(v) for k, v in params.items()}
            )

            if data and "artists" in data and data["artists"]:
                best_artist = data["artists"][0]

                # 1. Check the 'country' field first (direct ISO code)
                country_code = best_artist.get("country")
                if (
                    country_code
                    and isinstance(country_code, str)
                    and len(country_code) == REGION_CODE_LENGTH
                ):
                    region_result = country_code.lower()
                    self.console_logger.debug(
                        f"Found region '{region_result}' via 'country' field for '{artist_norm}'"
                    )
                else:
                    # 2. Fallback to 'area' information if 'country' is missing/invalid
                    area = best_artist.get("area")
                    if area and isinstance(area, dict):
                        # Check if area is explicitly a country
                        if area.get("type") == "Country":
                            # Try to get ISO code from the area itself
                            iso_codes = area.get("iso-3166-1-codes")
                            if (
                                iso_codes
                                and isinstance(iso_codes, list)
                                and len(iso_codes) > 0
                            ):
                                # Use the first ISO code found
                                region_result = iso_codes[0].lower()
                                self.console_logger.debug(
                                    f"Found region '{region_result}' via area ISO codes for '{artist_norm}'"
                                )
            else:
                # Artist not found in MusicBrainz
                self.console_logger.debug(
                    f"No artist found in MusicBrainz to determine region for '{artist_norm}'"
                )

        except Exception as e:
            # Log errors during API request or processing
            self.error_logger.warning(
                f"Error retrieving artist region for '{artist_norm}': {e}"
            )

        # Cache the result (even if it's None) before returning using self.cache_service
        await self.cache_service.set_async(
            cache_key, region_result, ttl=cache_ttl_seconds
        )
        return region_result

    def _score_original_release(
        self,
        release: dict[str, Any],
        artist_norm: str,
        album_norm: str,
        artist_region: str | None,
    ) -> int:
        """REVISED scoring function prioritizing original release indicators (v3)."""
        # Ensure scoring_config is a dict, fallback to empty if not found or invalid
        scoring_cfg = (
            self.scoring_config if isinstance(self.scoring_config, dict) else {}
        )

        # --- Initialization ---
        score: int = scoring_cfg.get("base_score", 10)  # Start with base score
        score_components: list[str] = []  # For debugging

        # Extract key fields
        release_title_orig = release.get("title", "")
        release_artist_orig = release.get("artist", "")
        year_str = release.get("year", "")
        source = release.get("source", "unknown")
        release_title_norm = self._normalize_name(release_title_orig)
        release_artist_norm = self._normalize_name(release_artist_orig)

        # --- 1. Core Match Quality ---
        # Artist Match Bonus
        artist_match_bonus = 0
        if release_artist_norm and release_artist_norm == artist_norm:
            artist_match_bonus = scoring_cfg.get(
                "artist_exact_match_bonus", 20
            )  # Reduced
            score += artist_match_bonus
            score_components.append(f"Artist Exact Match: +{artist_match_bonus}")

        # Album Title Match Bonus/Penalty
        title_match_bonus = 0
        title_penalty = 0

        def simple_norm(text: str) -> str:
            return re.sub(r"[^\w\s]", "", text.lower()).strip()

        comp_release_title = simple_norm(release_title_norm)
        comp_album_norm = simple_norm(album_norm)

        if comp_release_title == comp_album_norm:
            title_match_bonus = scoring_cfg.get(
                "album_exact_match_bonus", 25
            )  # Reduced
            score += title_match_bonus
            score_components.append(f"Album Exact Match: +{title_match_bonus}")
            if artist_match_bonus > 0:  # Only apply perfect if artist also matched
                perfect_match_bonus = scoring_cfg.get(
                    "perfect_match_bonus", 10
                )  # Reduced
                score += perfect_match_bonus
                score_components.append(
                    f"Perfect Artist+Album Match: +{perfect_match_bonus}"
                )
        elif comp_release_title.startswith(comp_album_norm) and re.match(
            # Handle variations (e.g., "Album (Deluxe)")
            r"^[\(\[][^)\]]+[\)\]]$",
            comp_release_title[len(comp_album_norm) :].strip(),
        ):
            title_match_bonus = scoring_cfg.get("album_variation_bonus", 10)
            score += title_match_bonus
            score_components.append(
                f"Album Variation (Suffix): +{title_match_bonus}"
            )
        elif comp_album_norm.startswith(comp_release_title) and re.match(
            r"^[\(\[][^)\]]+[\)\]]$",
            comp_album_norm[len(comp_release_title):].strip(),
        ):
            title_match_bonus = scoring_cfg.get("album_variation_bonus", 10)
            score += title_match_bonus
            score_components.append(
                f"Album Variation (Search Suffix): +{title_match_bonus}"
            )
        # Penalize substring inclusion (less likely original)
        elif (
            comp_album_norm in comp_release_title
            or comp_release_title in comp_album_norm
        ):
            title_penalty = scoring_cfg.get(
                "album_substring_penalty", -15
            )  # Reduced penalty
            score += title_penalty
            score_components.append(f"Album Substring Mismatch: {title_penalty}")
        else:  # Penalize unrelated titles
            title_penalty = scoring_cfg.get(
                "album_unrelated_penalty", -40
            )  # Reduced penalty
            score += title_penalty
            score_components.append(f"Album Unrelated: {title_penalty}")

        # --- 2. Release Characteristics ---
        # MusicBrainz Release Group First Date Match (VERY important)
        rg_first_date_str = release.get("releasegroup_first_date")
        rg_first_year: int | None = None
        if source == "musicbrainz" and rg_first_date_str:
            try:
                rg_year_str = rg_first_date_str.split("-")[0]
                if len(rg_year_str) == YEAR_LENGTH and rg_year_str.isdigit():
                    rg_first_year = int(rg_year_str)
                    # Strong bonus if release year matches the RG first year
                    if year_str and rg_year_str == year_str:
                        rg_match_bonus = scoring_cfg.get(
                            "mb_release_group_match_bonus", 50
                        )  # Increased!
                        score += rg_match_bonus
                        score_components.append(
                            f"MB RG First Date Match: +{rg_match_bonus}"
                        )
            except (IndexError, ValueError, TypeError):
                pass

        # Release Type (Album preferred)
        release_type = str(release.get("type", "")).lower()
        type_bonus = 0
        type_penalty = 0
        if "album" in release_type:
            type_bonus = scoring_cfg.get("type_album_bonus", 15)
            score_components.append(f"Type Album: +{type_bonus}")
        elif any(t in release_type for t in ["ep", "single"]):
            type_penalty = scoring_cfg.get("type_ep_single_penalty", -10)
            score_components.append(f"Type EP/Single: {type_penalty}")
        elif any(
            t in release_type for t in ["compilation", "live", "soundtrack", "remix"]
        ):
            type_penalty = scoring_cfg.get("type_compilation_live_penalty", -25)
            score_components.append(f"Type Comp/Live/Remix/Soundtrack: {type_penalty}")
        score += type_bonus + type_penalty

        # Release Status (Official preferred)
        status = str(release.get("status", "")).lower()
        status_bonus = 0
        status_penalty = 0
        if status == "official":
            status_bonus = scoring_cfg.get("status_official_bonus", 10)
            score_components.append(f"Status Official: +{status_bonus}")
        elif any(s in status for s in ["bootleg", "unofficial", "pseudorelease"]):
            status_penalty = scoring_cfg.get("status_bootleg_penalty", -50)
            score_components.append(f"Status Bootleg/Unofficial: {status_penalty}")
        elif status == "promotion":
            status_penalty = scoring_cfg.get("status_promo_penalty", -20)
            score_components.append(f"Status Promo: {status_penalty}")
        score += status_bonus + status_penalty

        # Reissue Indicator Penalty
        if release.get("is_reissue", False):
            reissue_penalty = scoring_cfg.get("reissue_penalty", -30)
            score += reissue_penalty
            score_components.append(f"Reissue Indicator: {reissue_penalty}")

        # --- 3. Contextual Factors ---
        year = -1
        year_diff_penalty = 0
        is_valid_year_format = False
        if year_str and year_str.isdigit() and len(year_str) == YEAR_LENGTH:
            is_valid_year_format = True
            try:
                year = int(year_str)
            except ValueError:
                is_valid_year_format = False

        if not is_valid_year_format:
            score = 0  # Invalid year format invalidates score
            score_components.append("Year Invalid Format: score=0")
        elif not (self.min_valid_year <= year <= self.current_year + 5):
            # Check year range
            score = 0
            score_components.append("Year Out of Range: score=0")
        else:
            # Apply Artist Activity Period Context
                if self.artist_period_context:
                    start_year = self.artist_period_context.get("start_year")
                    end_year = self.artist_period_context.get("end_year")

                    # Penalty if year is before artist start (allow 1 year grace)
                    if start_year and year < start_year - 1:
                        years_before = start_year - year
                        penalty_val = min(50, 5 + (years_before - 1) * 5)
                        score += scoring_cfg.get(
                            "year_before_start_penalty", -penalty_val
                        )
                        score_components.append(
                            f"Year Before Start ({years_before} yrs): {scoring_cfg.get('year_before_start_penalty', -penalty_val)}"
                        )

                    # Penalty if year is after artist end (allow 3 years grace)
                    if end_year and year > end_year + 3:
                        years_after = year - end_year
                        penalty_val = min(40, 5 + (years_after - 3) * 3)
                        score += scoring_cfg.get("year_after_end_penalty", -penalty_val)
                        score_components.append(
                            f"Year After End ({years_after} yrs): {scoring_cfg.get('year_after_end_penalty', -penalty_val)}"
                        )

                    # Bonus if year is near artist start
                    if start_year and 0 <= (year - start_year) <= 1:
                        score += scoring_cfg.get("year_near_start_bonus", 20)
                        score_components.append(
                            f"Year Near Start: +{scoring_cfg.get('year_near_start_bonus', 20)}"
                        )

                # NEW: Penalty based on difference from RG First Year
                if (
                    rg_first_year and year > rg_first_year + 1
                ):  # If release year is >1yr after RG first year
                    year_diff = year - rg_first_year
                    # Apply a scaling penalty, e.g., -5 points per year difference after the first year
                    penalty_scale = scoring_cfg.get("year_diff_penalty_scale", -5)
                    max_penalty = scoring_cfg.get("year_diff_max_penalty", -40)
                    year_diff_penalty = max(
                        max_penalty, (year_diff - 1) * penalty_scale
                    )
                    score += year_diff_penalty
                    score_components.append(
                        f"Year Diff from RG Date ({year_diff} yrs): {year_diff_penalty}"
                    )

        # Country / Region Match
        release_country = release.get("country", "").lower()
        if artist_region and release_country:
            if release_country == artist_region:
                score += scoring_cfg.get("country_artist_match_bonus", 10)
                score_components.append(
                    f"Country Matches Artist Region ({artist_region.upper()}): "
                    f"+{scoring_cfg.get('country_artist_match_match_bonus', 10)}"  # Fixed typo
                )
            elif release_country in scoring_cfg.get(
                "major_market_codes", ["us", "gb", "uk", "de", "jp", "fr"]
            ):  # Check against configured major markets
                score += scoring_cfg.get("country_major_market_bonus", 5)
                score_components.append(
                    f"Country Major Market ({release_country.upper()}): +{scoring_cfg.get('country_major_market_bonus', 5)}"
                )

        # --- 4. Source Reliability ---
        source_adjustment = 0
        if source == "musicbrainz":
            source_adjustment = scoring_cfg.get("source_mb_bonus", 5)
        elif source == "discogs":
            source_adjustment = scoring_cfg.get("source_discogs_bonus", 2)
        elif source == "lastfm":
            source_adjustment = scoring_cfg.get("source_lastfm_penalty", -5)
        if source_adjustment != 0:
            score += source_adjustment
            score_components.append(f"Source {source.title()}: {source_adjustment:+}")

        # Final score adjustment
        final_score = max(0, score)  # Score cannot be negative

        # --- Debug Logging ---
        # Log details only if score is potentially decisive or problematic
        if (
            final_score > self.definitive_score_threshold - 20
            or year_diff_penalty < 0
            or title_penalty < 0
        ):
            debug_log_msg = f"Score Calculation for '{release_title_orig}' ({year_str}) [{source}]:\n"
            debug_log_msg += "\n".join([f"  - {comp}" for comp in score_components])
            debug_log_msg += f"\n  ==> Final Score: {final_score}"
            # Use DEBUG level for potentially verbose output
            self.console_logger.debug(debug_log_msg)

        return final_score

    async def _get_scored_releases_from_musicbrainz(
        self, artist_norm: str, album_norm: str, artist_region: str | None
    ) -> list[dict[str, Any]]:
        """Retrieve and score releases from MusicBrainz, prioritizing Release Group search.

        Includes fallback search strategies if the initial precise query fails.
        Includes improved artist matching after fallback searches.
        """
        scored_releases: list[dict[str, Any]] = []
        release_groups = []  # Initialize list to store found release groups

        try:
            # Helper for Lucene escaping
            def escape_lucene(term: str) -> str:
                term = term.replace("\\", "\\\\")
                for char in r'+-&|!(){}[]^"~*?:\/':
                    term = term.replace(char, f"\\{char}")
                return term

            base_search_url = "https://musicbrainz.org/ws/2/release-group/"
            # --- Attempt 1: Precise Fielded Search (Primary Strategy) ---
            primary_query = f'artist:"{escape_lucene(artist_norm)}" AND releasegroup:"{escape_lucene(album_norm)}"'
            params_rg1 = {
                "fmt": "json",
                "limit": "10",
                "query": primary_query,
            }  # Converted 10 to string
            log_rg_url1 = (
                base_search_url + "?" + urllib.parse.urlencode(params_rg1, safe=":/")
            )
            self.console_logger.debug(f"[musicbrainz] Attempt 1 URL: {log_rg_url1}")

            rg_data1 = await self._make_api_request(
                "musicbrainz", base_search_url, params=params_rg1
            )

            if (
                rg_data1
                and rg_data1.get("count", 0) > 0
                and rg_data1.get("release-groups")
            ):
                self.console_logger.debug("Attempt 1 (Precise Search) successful.")
                release_groups = rg_data1.get("release-groups", [])
            else:
                self.console_logger.warning(
                    f"[musicbrainz] Attempt 1 (Precise Search) failed or yielded no results for query: {primary_query}. Trying fallback."
                )

                # --- Attempt 2: Broader Search (Keywords without fields) ---
                fallback_query1 = (
                    f'"{escape_lucene(artist_norm)}" AND "{escape_lucene(album_norm)}"'
                )
                params_rg2 = {
                    "fmt": "json",
                    "limit": "10",
                    "query": fallback_query1,
                }  # Converted 10 to string
                log_rg_url2 = (
                    base_search_url
                    + "?"
                    + urllib.parse.urlencode(params_rg2, safe=":/")
                )
                self.console_logger.debug(f"[musicbrainz] Attempt 2 URL: {log_rg_url2}")

                rg_data2 = await self._make_api_request(
                    "musicbrainz", base_search_url, params=params_rg2
                )

                if (
                    rg_data2
                    and rg_data2.get("count", 0) > 0
                    and rg_data2.get("release-groups")
                ):
                    self.console_logger.debug("Attempt 2 (Broad Search) successful.")
                    release_groups = rg_data2.get("release-groups", [])
                else:
                    self.console_logger.warning(
                        f"[musicbrainz] Attempt 2 (Broad Search) failed or yielded no results for query: {fallback_query1}. Trying last fallback."
                    )

                    # --- Attempt 3: Search by Album Title Only ---
                    fallback_query2 = f'"{escape_lucene(album_norm)}"'  # Search only by album title keyword
                    params_rg3 = {
                        "fmt": "json",
                        "limit": "10",
                        "query": fallback_query2,
                    }  # Converted 10 to string
                    log_rg_url3 = (
                        base_search_url
                        + "?"
                        + urllib.parse.urlencode(params_rg3, safe=":/")
                    )
                    self.console_logger.debug(
                        f"[musicbrainz] Attempt 3 URL: {log_rg_url3}"
                    )

                    rg_data3 = await self._make_api_request(
                        "musicbrainz", base_search_url, params=params_rg3
                    )

                    if (
                        rg_data3
                        and rg_data3.get("count", 0) > 0
                        and rg_data3.get("release-groups")
                    ):
                        self.console_logger.debug(
                            "Attempt 3 (Album Title Only Search) successful. Will filter by artist."
                        )
                        release_groups = rg_data3.get("release-groups", [])
                    else:
                        self.console_logger.warning(
                            f"[musicbrainz] All search attempts failed for '{artist_norm} - {album_norm}'."
                        )
                        return []  # Return empty list if all searches fail

            # --- Filter Results if Broader Search Was Used ---
            # We need to ensure the artist credit matches the search artist if we used fallback searches
            filtered_release_groups = []
            # Check if primary search failed (meaning we used fallbacks)
            if not (rg_data1 and rg_data1.get("count", 0) > 0):
                self.console_logger.debug(
                    f"Filtering {len(release_groups)} fallback results for artist '{artist_norm}'..."
                )
                for rg in release_groups:
                    artist_credit = rg.get("artist-credit", [])
                    # Check if *any* artist in the artist-credit list matches the search artist (normalized)
                    artist_match_found = False
                    if artist_credit and isinstance(artist_credit, list):
                        for credit in artist_credit:
                            if isinstance(credit, dict) and credit.get("name"):
                                # Use _normalize_name for comparison
                                if self._normalize_name(credit["name"]) == artist_norm:
                                    artist_match_found = True
                                    break  # Found a match, no need to check further credits

                    if artist_match_found:
                        filtered_release_groups.append(rg)
                    else:
                        # Log skipped results with artist credit for debugging
                        ac_names = ", ".join(
                            [
                                c.get("name", "Unknown")
                                for c in artist_credit
                                if isinstance(c, dict)
                            ]
                        )
                        self.console_logger.debug(
                            f"[musicbrainz] Skipping RG '{rg.get('title')}' due to artist mismatch ('{ac_names}' vs search '{artist_norm}')"
                        )

                self.console_logger.info(
                    f"Found {len(filtered_release_groups)} release groups matching artist after filtering fallback results."
                )
                if not filtered_release_groups:
                    return []  # No relevant groups found after filtering
                release_groups = filtered_release_groups  # Use the filtered list
            else:
                # If primary search worked, we assume the artist match is already good
                self.console_logger.info(
                    f"Using {len(release_groups)} results from primary MusicBrainz search."
                )

            # --- Process Filtered/Found Release Groups ---
            processed_release_ids: set[str] = set()
            max_groups_to_process = 3  # Limit processing if multiple groups found
            groups_to_process = release_groups[:max_groups_to_process]
            release_fetch_tasks = []
            # Map index to RG data to retrieve full info after fetching releases
            rg_info_map = {
                idx: groups_to_process[idx] for idx in range(len(groups_to_process))
            }

            for rg in groups_to_process:
                rg_id = rg.get("id")
                if not rg_id:
                    continue
                # Use release search endpoint to get individual releases within the release group
                release_params = {
                    "release-group": rg_id,
                    "inc": "media",
                    "fmt": "json",
                    "limit": "50",
                }
                release_search_url = "https://musicbrainz.org/ws/2/release/"
                release_fetch_tasks.append(
                    self._make_api_request(
                        "musicbrainz", release_search_url, params=release_params
                    )
                )

            release_results = await asyncio.gather(
                *release_fetch_tasks, return_exceptions=True
            )

            # --- Score Fetched Releases ---
            for idx, result in enumerate(release_results):
                # Get full RG data using the index map to access RG-level info for scoring
                rg_info_full = rg_info_map.get(idx)
                if not rg_info_full:
                    continue
                rg_id = rg_info_full.get("id")  # Get RG ID

                # Extract details needed for scoring/processing from rg_info_full
                rg_first_date = rg_info_full.get("first-release-date")
                rg_primary_type = rg_info_full.get("primary-type")
                rg_artist_credit = rg_info_full.get("artist-credit", [])
                rg_artist_name = ""
                if rg_artist_credit and isinstance(rg_artist_credit, list):
                    # Use _normalize_name when extracting RG artist name for consistency
                    rg_artist_name_parts = []
                    for credit in rg_artist_credit:
                        if isinstance(credit, dict) and credit.get("name"):
                            rg_artist_name_parts.append(
                                self._normalize_name(credit["name"])
                            )
                            if credit.get("joinphrase"):
                                rg_artist_name_parts.append(credit["joinphrase"])
                    rg_artist_name = "".join(
                        rg_artist_name_parts
                    ).strip()  # Build normalized name

                if isinstance(result, Exception):
                    self.error_logger.warning(
                        f"Failed to fetch releases for MB RG ID {rg_id}: {result}"
                    )
                    continue
                if (
                    not result
                    or not isinstance(result, dict)
                    or "releases" not in result
                ):
                    if (
                        result is not None
                    ):  # Log only if the API call itself didn't return None
                        self.console_logger.warning(
                            f"No release data found for MB RG ID {rg_id}"
                        )
                    continue

                releases = result.get("releases", [])
                for release in releases:
                    release_id = release.get("id")
                    if not release_id or release_id in processed_release_ids:
                        continue
                    processed_release_ids.add(release_id)

                    year: str | None = None
                    release_date_str = release.get("date", "")
                    if release_date_str:
                        year_match = re.match(r"^(\d{4})", release_date_str)
                        if year_match:
                            year = year_match.group(1)

                    status_val = release.get("status")
                    status = status_val.lower() if isinstance(status_val, str) else ""

                    country_val = release.get("country")
                    country = (
                        country_val.lower() if isinstance(country_val, str) else ""
                    )

                    release_title = release.get("title", "")
                    media = release.get("media", [])
                    format_details = (
                        media[0].get("format", "")
                        if media and isinstance(media, list)
                        else ""
                    )

                    # Use reissue keywords from scoring config
                    scoring_cfg = (
                        self.scoring_config
                        if isinstance(self.scoring_config, dict)
                        else {}
                    )  # Ensure scoring_cfg is a dict
                    reissue_keywords = (
                        scoring_cfg.get("reissue_keywords", [])
                        if isinstance(scoring_cfg.get("reissue_detection"), dict)
                        else []
                    )  # Get from reissue_detection subsection if it exists
                    reissue_keywords.extend(
                        self.config.get("cleaning", {}).get("remaster_keywords", [])
                    )  # Include cleaning remaster keywords

                    is_reissue = False
                    title_lower = release_title.lower()
                    if any(kw.lower() in title_lower for kw in reissue_keywords):
                        is_reissue = True

                    # Also check format descriptions for reissue keywords
                    format_desc_lower = " ".join(
                        [
                            d.lower()
                            for fmt in media
                            if isinstance(fmt, dict)
                            and isinstance(fmt.get("descriptions"), list)
                            for d in fmt["descriptions"]
                        ]
                    ).lower()  # Convert descriptions to lower case and join

                    if not is_reissue and any(
                        kw.lower() in format_desc_lower for kw in reissue_keywords
                    ):
                        is_reissue = True

                    release_info = {
                        "source": "musicbrainz",
                        "id": release_id,
                        "title": release_title,
                        "artist": rg_artist_name
                        or artist_norm,  # Use normalized RG artist name or search artist
                        "year": year,
                        "type": rg_primary_type
                        or (
                            release.get("release-group", {}).get("primary-type")
                            if isinstance(release.get("release-group"), dict)
                            else None
                        ),  # Use RG type
                        "status": status,
                        "country": country,
                        "format_details": format_details,
                        "is_reissue": is_reissue,
                        "releasegroup_id": rg_id,
                        "releasegroup_first_date": rg_first_date,  # Pass RG first date
                        "score": 0,
                    }

                    # Only score if year is potentially valid
                    if year is not None and self._is_valid_year(year):
                        release_info["score"] = self._score_original_release(
                            release_info, artist_norm, album_norm, artist_region
                        )
                        scored_releases.append(release_info)
                    else:
                        self.console_logger.debug(
                            f"Skipping scoring for MB release '{release_info.get('title')}' "
                            f"due to invalid or missing year: {release_info.get('year')}"
                        )

            # --- Final Sorting and Logging ---
            # Sort by score descending, then year ascending for ties
            scored_releases.sort(
                key=lambda x: (-x["score"], int(x.get("year") or 0))
            )  # Ensure year is int for sorting

            self.console_logger.info(
                f"Found {len(scored_releases)} scored releases from MusicBrainz for '{artist_norm} - {album_norm}'"
            )
            for i, r in enumerate(scored_releases[:3]):
                # Break long log line
                self.console_logger.info(
                    f"  MB #{i + 1}: {r.get('title', '')} ({r.get('year', '')}) - "
                    f"Type: {r.get('type', '')}, Status: {r.get('status', '')}, "
                    f"Country: {r.get('country', '').upper()}, "
                    f"Score: {r.get('score')} (RG First Date: {r.get('releasegroup_first_date')})"
                )

            return scored_releases

        except Exception as e:
            self.error_logger.exception(
                f"Error retrieving/scoring from MusicBrainz for '{artist_norm} - {album_norm}': {e}"
            )
            return []

    async def _get_scored_releases_from_discogs(
        self, artist_norm: str, album_norm: str, artist_region: str | None
    ) -> list[dict[str, Any]]:
        """Retrieve and score releases from Discogs.

        Uses a combined query parameter 'q' and includes basic result validation with improved artist matching.
        """
        cache_key = f"discogs_{artist_norm}_{album_norm}"
        cache_ttl_seconds = self.cache_ttl_days * 86400

        cached_data = await self.cache_service.get_async(cache_key)
        if cached_data is not None:
            if isinstance(cached_data, list):
                self.console_logger.debug(
                    f"Using cached Discogs results for '{artist_norm} - {album_norm}'"
                )
                return cached_data
            self.console_logger.warning(
                f"Cached Discogs data for '{artist_norm} - {album_norm}' has unexpected type. Ignoring cache."
            )

        scored_releases: list[dict[str, Any]] = []
        try:
            # Use combined query parameter 'q'
            search_query = f"{artist_norm} {album_norm}"
            params = {"q": search_query, "type": "release", "per_page": "25"}
            search_url = "https://api.discogs.com/database/search"

            log_discogs_url = (
                search_url + "?" + urllib.parse.urlencode(params, safe=":/")
            )
            self.console_logger.debug(f"[discogs] Search URL: {log_discogs_url}")

            data = await self._make_api_request("discogs", search_url, params=params)

            # --- DEBUG LOGGING ---
            self.console_logger.debug(
                f"[discogs] Data received from _make_api_request: Type={type(data)}"
            )
            if isinstance(data, dict):
                self.console_logger.debug(
                    f"[discogs] Received data keys: {list(data.keys())}"
                )
                results_preview = data.get("results", "N/A")
                if isinstance(results_preview, list):
                    self.console_logger.debug(
                        f"[discogs] Received {len(results_preview)} results. First result preview: {str(results_preview[:1])[:300]}"
                    )
                else:
                    self.console_logger.debug(
                        "[discogs] 'results' key not found or not a list."
                    )
            elif data is None:
                self.console_logger.debug("[discogs] _make_api_request returned None.")

            # --- Process Discogs Results ---
            if not data or "results" not in data:
                self.console_logger.warning(
                    f"[discogs] Search failed or no results key in data for query: '{search_query}'"
                )
                await self.cache_service.set_async(cache_key, [], ttl=cache_ttl_seconds)
                return []

            results = data.get("results", [])
            if not results:
                self.console_logger.info(
                    f"[discogs] No results found for query: '{search_query}'"
                )
                await self.cache_service.set_async(cache_key, [], ttl=cache_ttl_seconds)
                return []

            self.console_logger.debug(
                f"Found {len(results)} potential Discogs matches for query: '{search_query}'"
            )

            scoring_cfg = (
                self.scoring_config if isinstance(self.scoring_config, dict) else {}
            )
            # Get reissue keywords from both reissue_detection and cleaning sections
            reissue_keywords = scoring_cfg.get("reissue_detection", {}).get(
                "reissue_keywords", []
            )
            reissue_keywords.extend(
                self.config.get("cleaning", {}).get("remaster_keywords", [])
            )  # Include cleaning remaster keywords

            for item in results:
                year_str = str(item.get("year", ""))
                release_title_full = item.get("title", "")
                # Discogs title is often "Artist - Album", need to parse correctly
                title_parts = release_title_full.split(" - ", 1)
                item_artist_raw = (
                    title_parts[0].strip() if len(title_parts) > 1 else ""
                )  # Artist part
                item_album_raw = (
                    title_parts[1].strip()
                    if len(title_parts) > 1
                    else release_title_full.strip()
                )  # Album part or full title

                # --- IMPROVED ARTIST MATCHING LOGIC ---
                # Normalize both the search artist and the artist part from the item title
                artist_norm_item = self._normalize_name(item_artist_raw)

                # Check if the normalized artist part from the item title matches the normalized search artist
                # Use simple equality on normalized names for stricter match after broad search
                artist_matches = (
                    artist_norm_item != "" and artist_norm_item == artist_norm
                )

                # Secondary check: if primary match failed, see if normalized search artist is
                # a significant part of the normalized item artist (e.g. "The Beatles" vs "Beatles, The")
                if not artist_matches and artist_norm_item != "" and artist_norm != "":
                    if (
                        artist_norm in artist_norm_item
                        or artist_norm_item in artist_norm
                    ):
                        # This is a weaker match, potentially give a small bonus or just accept as a match
                        # For now, let's treat as a match but maybe log it for review if needed
                        artist_matches = True
                        self.console_logger.debug(
                            f"[discogs] Found potential artist variation match for '{item_artist_raw}' vs search '{artist_norm}'"
                        )

                if not artist_matches:
                    self.console_logger.debug(
                        f"[discogs] Skipping result '{release_title_full}' due to artist mismatch "
                        f"('{item_artist_raw}' normalized to '{artist_norm_item}' vs search '{artist_norm}')"
                    )
                    continue

                # If artist matched well, but album title seems completely unrelated, we might still skip
                # Add a check to skip if the normalized album title from the item is very short or completely unrelated
                # (beyond simple substring check). This is harder without fuzzy matching.
                # For now, rely on the scoring function to penalize unrelated titles.
                # The scoring function uses simple_norm and checks for exact/substring match.
                # We rely on the scoring function's album title penalties to handle unrelated titles.
                # The filter here is primarily to ensure the artist part of the title is correct.

                formats = item.get("formats", [])
                format_names: list[str] = []
                format_descriptions: list[str] = []
                if isinstance(formats, list):
                    for fmt in formats:
                        if isinstance(fmt, dict):
                            if fmt.get("name"):
                                format_names.append(fmt["name"])
                            if fmt.get("descriptions"):
                                descriptions = fmt["descriptions"]
                                if isinstance(descriptions, list):
                                    format_descriptions.extend(descriptions)

                format_details_str = ", ".join(format_names + format_descriptions)

                is_reissue = False
                # Check reissue keywords in original title and format descriptions
                title_lower = (
                    release_title_full.lower()
                )  # Use full title for reissue check
                desc_lower = " ".join(format_descriptions).lower()
                if any(kw.lower() in title_lower for kw in reissue_keywords):
                    is_reissue = True
                if not is_reissue and any(
                    kw.lower() in desc_lower for kw in reissue_keywords
                ):
                    is_reissue = True
                # Also check specific common reissue terms in descriptions
                if not is_reissue and any(
                    d.lower() in ["reissue", "remastered", "repress", "remaster"]
                    for d in format_descriptions
                ):
                    is_reissue = True

                release_type = "Album"
                format_names_lower = [fn.lower() for fn in format_names]
                desc_lower_list = [d.lower() for d in format_descriptions]
                if (
                    any(ft in format_names_lower for ft in ["lp", "album"])
                    or "album" in desc_lower_list
                ):
                    release_type = "Album"
                elif (
                    any(ft in format_names_lower for ft in ["ep"])
                    or "ep" in desc_lower_list
                ):
                    release_type = "EP"
                elif (
                    any(ft in format_names_lower for ft in ["single"])
                    or "single" in desc_lower_list
                ):
                    release_type = "Single"
                elif (
                    any(ft in format_names_lower for ft in ["compilation"])
                    or "compilation" in desc_lower_list
                ):
                    release_type = "Compilation"

                release_info = {
                    "source": "discogs",
                    "id": item.get("id"),
                    "title": item_album_raw,  # Use the extracted album part
                    "artist": item_artist_raw,  # Use the extracted artist part
                    "year": year_str,
                    "type": release_type,
                    "status": "Official",  # Discogs results are typically official releases
                    "country": item.get("country", "").lower(),
                    "format_details": format_details_str,
                    "is_reissue": is_reissue,
                    "score": 0,
                }

                # Only score if year is potentially valid
                if self._is_valid_year(release_info["year"]):
                    # Pass the normalized search artist/album to scoring for consistent comparison
                    release_info["score"] = self._score_original_release(
                        release_info, artist_norm, album_norm, artist_region
                    )
                    min_score = scoring_cfg.get(
                        "discogs_min_broad_score", 20
                    )  # Use configured minimum score for broad matches
                    if release_info["score"] >= min_score:  # Use >= for inclusivity
                        scored_releases.append(release_info)
                    else:
                        self.console_logger.debug(
                            f"Skipping Discogs release '{release_info.get('title')}' "
                            f"due to low score: {release_info['score']} (Min required: {min_score})"
                        )  # Log skipped low scores
                else:
                    self.console_logger.debug(
                        f"Skipping scoring for Discogs release '{release_info.get('title')}' "
                        f"due to invalid or missing year: {release_info.get('year')}"
                    )

            # Sort by score descending, then year ascending for ties
            scored_releases.sort(
                key=lambda x: (-x["score"], int(x.get("year") or 0))
            )  # Ensure year is int for sorting

            self.console_logger.info(
                f"Found {len(scored_releases)} scored releases from Discogs for query: '{search_query}'"
            )
            for i, r in enumerate(scored_releases[:3]):
                # Break long log line
                self.console_logger.info(
                    f"  Discogs #{i + 1}: {r.get('artist', '')} - {r.get('title', '')} ({r.get('year', '')}) - "  # Log artist and title clearly
                    f"Type: {r.get('type', '')}, Country: {r.get('country', '').upper()}, Reissue: {r.get('is_reissue')}, "  # Added Country
                    f"Score: {r.get('score')}"
                )

            await self.cache_service.set_async(
                cache_key, scored_releases, ttl=cache_ttl_seconds
            )
            return scored_releases

        except Exception as e:
            self.error_logger.exception(
                f"Error retrieving/scoring from Discogs for '{artist_norm} - {album_norm}': {e}"
            )
            await self.cache_service.set_async(cache_key, [], ttl=cache_ttl_seconds)
            return []

    async def _get_scored_releases_from_lastfm(
        self, artist_norm: str, album_norm: str
    ) -> list[dict[str, Any]]:
        """Retrieve album release year from Last.fm and return a scored release dictionary."""
        scored_releases: list[dict[str, Any]] = []
        if not self.use_lastfm:
            return []  # Skip if Last.fm is disabled

        try:
            self.console_logger.debug(
                f"Searching Last.fm for: '{artist_norm} - {album_norm}'"
            )

            # Prepare API request parameters
            url = "https://ws.audioscrobbler.com/2.0/"
            params: dict[str, str] = {
                "method": "album.getInfo",
                "artist": str(artist_norm),  # Ensure string type
                "album": str(album_norm),    # Ensure string type
                "api_key": str(self.lastfm_api_key),  # Ensure string type
                "format": "json",
                "autocorrect": "1",  # Enable Last.fm autocorrection
            }


            # Make the API request with properly typed params
            data = await self._make_api_request("lastfm", url, params=params)

            # Validate response
            if not data:
                self.console_logger.warning(
                    f"Last.fm getInfo failed (no data) for '{artist_norm} - {album_norm}'"
                )
                return []
            if "error" in data:  # Check for Last.fm specific errors
                self.console_logger.warning(
                    f"Last.fm API error {data.get('error')}: {data.get('message', 'Unknown Last.fm error')}"
                )
                return []
            if "album" not in data or not isinstance(data["album"], dict):
                # Ensure the main 'album' object exists
                self.console_logger.warning(
                    f"Last.fm response missing 'album' object for '{artist_norm} - {album_norm}'"
                )
                return []

            # Extract year information from the album data
            album_data = data["album"]
            year = self._extract_year_from_lastfm_data(album_data)

            if year:
                source_detail = "lastfm"

                # Basic assumptions for Last.fm data (less detailed than MB/Discogs)
                release_type = "Album"  # Assume Album unless tags suggest otherwise (complex to implement reliably)
                status = "Official"  # Assume Official

                # Prepare data structure for scoring
                release_info = {
                    "source": "lastfm",
                    "id": album_data.get(
                        "mbid"
                    ),  # MusicBrainz ID from Last.fm, if available
                    "title": album_data.get(
                        "name", album_norm
                    ),  # Use name from response
                    "artist": album_data.get(
                        "artist", artist_norm
                    ),  # Use artist from response
                    "year": year,
                    "type": release_type,
                    "status": status,
                    "country": "",  # Not provided by Last.fm getInfo
                    "format_details": "",  # Not provided
                    "is_reissue": False,  # Difficult to determine reliably
                    "source_detail": source_detail,  # Where the year came from (wiki, date, tags)
                    "score": 0,
                }

                # Score this single result (passing None for artist_region)
                if self._is_valid_year(release_info["year"]):
                    release_info["score"] = self._score_original_release(
                        release_info, artist_norm, album_norm, None
                    )
                    scored_releases.append(release_info)
                    self.console_logger.info(
                        f"Scored LastFM Release: '{release_info['title']}' ({release_info['year']}) "
                        f"Source: {source_detail}, Score: {release_info['score']}"
                    )

        except Exception as e:
            # Catch any unexpected errors during the process
            self.error_logger.exception(
                f"Error retrieving/scoring from Last.fm for '{artist_norm} - {album_norm}': {e}"
            )

        # Return the list (usually empty or with one scored item)
        return scored_releases

    def _extract_year_from_lastfm_data(
        self, album_data: dict[str, Any]
    ) -> str | None:
        """Extract the most likely year from the Last.fm album data structure.

        Prioritizes explicit release date, then wiki content patterns, then tags.
        Returns the year as a string if found, otherwise None.
        """
        # --- Priority 1: Explicit 'releasedate' field ---
        release_date_str = album_data.get("releasedate", "").strip()
        if release_date_str:
            # Extract year (YYYY) using regex, handles various date formats
            year_match = re.search(r"\b(\d{4})\b", release_date_str)
            if year_match:
                potential_year = year_match.group(1)
                # Validate the extracted year
                if self._is_valid_year(potential_year):
                    self.console_logger.debug(
                        f"LastFM Year: {potential_year} from 'releasedate' field"
                    )
                    return potential_year

        # --- Priority 2: Wiki Content ---
        wiki = album_data.get("wiki")
        # Check if wiki exists and is a dictionary with 'content'
        if (
            wiki
            and isinstance(wiki, dict)
            and isinstance(wiki.get("content"), str)
            and wiki["content"].strip()
        ):
            wiki_content = wiki["content"]
            # Define patterns to search for year information, from more specific to less
            patterns = [
                # Specific phrases like "Originally released in/on YYYY"
                r"(?:originally\s+)?released\s+(?:in|on)\s+(?:(?:\d{1,2}\s+)?"
                r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
                r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+)?(\d{4})",
                # Phrases like "YYYY release" or "a YYYY album"
                r"\b(19\d{2}|20\d{2})\s+(?:release|album)\b",
                # Common date formats like "Month DD, YYYY" or "DD Month YYYY"
                r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
                r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+(\d{4})\b",
                r"\b\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
                r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{4})\b"
            ]
            for pattern in patterns:
                match = re.search(pattern, wiki_content, re.IGNORECASE)
                if match:
                    # Find the actual year group (could be group 1 or 2 depending on pattern)
                    potential_year = next(
                        (g for g in match.groups() if g is not None), None
                    )
                    if self._is_valid_year(potential_year):
                        self.console_logger.debug(
                            f"LastFM Year: {potential_year} from wiki content (pattern: '{pattern}')"
                        )
                        return potential_year

        # --- Priority 3: Tags ---
        tags_container = album_data.get("tags")
        tags_data: list[dict[str, Any]] = []

        # Handle different tag container formats
        if isinstance(tags_container, dict):
            # If it's a dictionary, try to get 'tag' key
            tag_value = tags_container.get("tag", [])
            # Convert single dict to list if needed
            tags_data = [tag_value] if isinstance(tag_value, dict) else tag_value
        elif isinstance(tags_container, list):
            tags_data = tags_container


        # Process tags to find year information
        year_tags: dict[str, int] = defaultdict(int)  # Count frequency of year tags

        for tag in tags_data:
            if not isinstance(tag, dict) or "name" not in tag:
                continue

            tag_name = str(tag["name"]).strip()
            # Check if tag is a 4-digit string representing a valid year
            if (
                len(tag_name) == YEAR_LENGTH
                and tag_name.isdigit()
                and self._is_valid_year(tag_name)
            ):
                year_tags[tag_name] += 1

        if year_tags:
            # Get the most frequent year tag, or None if empty
            best_year_tag = max(
                year_tags.items(), key=lambda x: x[1], default=(None, 0)
            )[0]
            if best_year_tag:
                self.console_logger.debug(
                    f"LastFM Year: {best_year_tag} from tags (most frequent)"
                )
                return best_year_tag

        # --- No valid year found ---
        self.console_logger.debug(
            f"No year information extracted from Last.fm data for '{album_data.get('artist', '')} - {album_data.get('name', '')}'"
        )
        return None

    def _normalize_name(self, name: str) -> str:
        """Normalize a name by removing common suffixes and extra characters for better API matching."""
        if not name or not isinstance(name, str):
            return ""

        # Patterns for suffixes/versions to remove (case-insensitive)
        # More specific patterns first
        suffixes_to_strip = [
            # Common Remaster/Edition markers in parens/brackets
            (
                r"\s+[\(\[](?:\d{4}\s+)?(?:Remaster(?:ed)?|Deluxe(?: Edition)?|"
                r"Expanded(?: Edition)?|Legacy Edition|Anniversary Edition|"
                r"Bonus Tracks?|Digital Version)[\)\]]"
            ),
            # Standalone Remaster/Deluxe tags (less common, more risky)
            # r'\s+-\s+(?:Remastered|Deluxe Edition)$',
            # EP/Single suffixes
            r"\s+(?:-\s+)?(?:EP|Single)$",
            r"\s+[\(\[]EP[\)\]]$",
            r"\s+[\(\[]Single[\)\]]$",
        ]
        result = name
        for suffix_pattern in suffixes_to_strip:
            # Use re.IGNORECASE for case-insensitive removal
            result = re.sub(
                suffix_pattern + r"\s*$", "", result, flags=re.IGNORECASE
            ).strip()

        # General cleanup: remove most punctuation (keep hyphens), normalize whitespace
        # Keep basic Latin letters, numbers, whitespace, and hyphens
        result = re.sub(r"[^\w\s\-]", "", result)
        result = re.sub(r"\s+", " ", result).strip()

        # Avoid returning empty string if original name wasn't empty
        return result if result else name

    def _clean_expired_cache(
        self, cache: dict[str, tuple[Any, float]] | Any, ttl_seconds: float, cache_name: str = "Generic"
    ) -> None:
        """Remove expired entries from a timestamped cache dictionary.

        This method is no longer strictly necessary for activity/region cache
        as CacheService.get_async handles TTL, but kept for potential other uses
        or if the cache structure changes.
        """
        if not isinstance(cache, dict):
            self.console_logger.debug(f"{cache_name} cache is not a dictionary. Skipping cleanup.")
            return  # Safety check

        current_time = time.monotonic()
        # Find keys of expired entries
        keys_to_delete = [
            key
            for key, (_, timestamp) in cache.items()
            # Ensure timestamp is a number before comparison
            if isinstance(timestamp, int | float)
            and (current_time - timestamp >= ttl_seconds)
        ]

        if keys_to_delete:
            for key in keys_to_delete:
                try:
                    del cache[key]
                except KeyError:
                    pass  # Ignore if key already removed elsewhere
            self.console_logger.debug(
                f"Cleaned {len(keys_to_delete)} expired entries from {cache_name} cache."
            )

    def _is_valid_year(self, year_str: str | None) -> bool:
        """Check if a string represents a valid release year."""
        if (
            not year_str
            or not isinstance(year_str, str)
            or not year_str.isdigit()
            or len(year_str) != YEAR_LENGTH
        ):
            return False
        try:
            year = int(year_str)
            # Check against configured min year and allow a few years into the future
            # Cast to int to ensure type checker knows we're returning a bool
            min_year = int(self.min_valid_year)
            max_year = int(self.current_year) + 5
            return min_year <= year <= max_year
        except ValueError:
            # Should not happen due to isdigit, but included for safety
            return False
