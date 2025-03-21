#!/usr/bin/env python3
"""
External API Service Module

This module provides an abstraction for interacting with external APIs (MusicBrainz and Discogs)
to retrieve original album release years. It centralizes rate limiting, error handling, and scoring
algorithms to determine the most accurate original release year.

Example:
    >>> import asyncio, logging, yaml
    >>> from external_api_service import ExternalApiService
    >>> # Assume config is loaded from a YAML file:
    >>> with open("my-config.yaml", "r", encoding="utf-8") as f:
    ...     config = yaml.safe_load(f)
    >>> console_logger = logging.getLogger("console_logger")
    >>> error_logger = logging.getLogger("error_logger")
    >>> service = ExternalApiService(config, console_logger, error_logger)
    >>> asyncio.run(service.initialize())
    >>> year = asyncio.run(service.get_album_year("Agalloch", "The White"))
    >>> print("Determined album year:", year)
"""

import asyncio
import logging
import re
import time
import urllib.parse

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

class ExternalApiService:
    """
    ExternalApiService interacts with MusicBrainz and Discogs APIs to retrieve and score
    release information for a given artist and album.

    Attributes:
        config (dict): Configuration dictionary loaded from YAML.
        console_logger (logging.Logger): Logger for console messages.
        error_logger (logging.Logger): Logger for error messages.
        session (aiohttp.ClientSession): HTTP session for API calls.
        rate_limits (dict): Contains API-specific rate limiting information.
        preferred_api (str): Preferred API name ("musicbrainz" or "discogs").
        min_valid_year (int): Minimum valid release year.
        trust_threshold (int): Score threshold below which additional restrictions apply.
        current_year (int): Current year plus one (for future releases).
        activity_cache (dict): Cache mapping artist (lowercase) to a tuple (start_year, end_year).
    """
    def __init__(self, config: Dict[str, Any], console_logger: logging.Logger, error_logger: logging.Logger):
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.session: Optional[aiohttp.ClientSession] = None

        # Get rate limits from config or use defaults
        self.rate_limits = {
            'discogs': {
                'requests_per_minute': config.get("year_retrieval", {}).get("discogs_requests_per_minute", 25),
                'last_request': 0
            },
            'musicbrainz': {
                'requests_per_second': config.get("year_retrieval", {}).get("musicbrainz_requests_per_second", 1),
                'last_request': 0
            }
        }

        self.preferred_api = config.get('year_retrieval', {}).get('preferred_api', 'musicbrainz')
        self.min_valid_year = config.get('year_retrieval', {}).get('min_valid_year', 1900)
        self.trust_threshold = config.get('year_retrieval', {}).get('trust_threshold', 50)
        self.max_year_difference = config.get('year_retrieval', {}).get('max_year_difference', 5)
        self.current_year = datetime.now().year + 1  # +1 allows future releases

        # Cache for artist activity periods to reduce redundant API calls
        self.activity_cache: Dict[str, Tuple[Optional[int], Optional[int]]] = {}
        
        # Artist period context for current request - will be set/cleared for each request
        self.artist_period_context: Optional[Dict[str, Optional[int]]] = None

    async def initialize(self, force: bool = False) -> None:
        """
        Initialize the aiohttp session.

        Args:
            force (bool): If True, forces reinitialization even if a session exists.
        
        Example:
            >>> await service.initialize(force=True)
        """
        if force or not self.session or self.session.closed:
            if force and self.session and not self.session.closed:
                await self.session.close()
            self.session = aiohttp.ClientSession()
            self.console_logger.info("External API session initialized" + (" (forced)" if force else ""))

    async def close(self) -> None:
        """
        Close the aiohttp session.

        Example:
            >>> await service.close()
        """
        if self.session and not self.session.closed:
            await self.session.close()

    async def _respect_rate_limit(self, api_name: str) -> None:
        """
        Waits if necessary to respect the rate limit for the specified API.

        Args:
            api_name (str): API name, either "discogs" or "musicbrainz".
        
        Example:
            >>> await service._respect_rate_limit("musicbrainz")
        """
        rate_limit = self.rate_limits.get(api_name)
        if not rate_limit:
            return

        if api_name == 'discogs':
            min_interval = 60 / rate_limit['requests_per_minute']
        elif api_name == 'musicbrainz':
            min_interval = 1 / rate_limit['requests_per_second']
        else:
            min_interval = 1

        elapsed = time.time() - rate_limit['last_request']
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        rate_limit['last_request'] = time.time()

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

    async def get_artist_activity_period(self, artist: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Retrieve the period of activity for an artist from MusicBrainz.
        The result is cached to avoid redundant API calls.

        The method tries two approaches:
        1. First, it checks if MusicBrainz provides direct lifespan information
        2. If not, it analyzes the artist's releases to determine the active period

        Args:
            artist (str): The artist's name.

        Returns:
            Tuple[Optional[int], Optional[int]]: (start_year, end_year). If end_year is None, the artist is still active.

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
        
        # Get releases for this artist from MusicBrainz
        headers = {'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'}
        artist_encoded = urllib.parse.quote_plus(artist)
        
        try:
            # Search for the artist first
            await self._respect_rate_limit('musicbrainz')
            search_url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{artist_encoded}&fmt=json"
            
            async with self.session.get(search_url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    self.console_logger.warning(f"Cannot determine activity period for {artist}: Artist not found")
                    self.activity_cache[cache_key] = (None, None)
                    return None, None
                    
                data = await response.json()
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
                await self._respect_rate_limit('musicbrainz')
                releases_url = f"https://musicbrainz.org/ws/2/release-group?artist={artist_id}&fmt=json"
                
                async with self.session.get(releases_url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        self.console_logger.warning(f"Failed to get releases for {artist}")
                        self.activity_cache[cache_key] = (None, None)
                        return None, None
                        
                    data = await response.json()
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
            self.error_logger.error(f"Error determining activity period for '{artist}': {e}", exc_info=True)
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

        This method uses the artist_period_context if available to score releases based on
        whether they fall within the artist's active period.

        Args:
            release (dict): Release data from API.
            artist (str): Searched artist name.
            album (str): Searched album name.

        Returns:
            int: Score between 0 and 100.

        Example:
            >>> score = service._score_original_release(release_data, "Agalloch", "The White")
            >>> print(score)
        """
        # Get scoring weights from config or use defaults
        scoring_config = self.config.get('year_retrieval', {}).get('scoring', {})
        
        score = scoring_config.get('default_base_score', 20)
        
        # Album title matching
        release_title = release.get('title', '').lower()
        album_norm = album.lower()
        if release_title == album_norm:
            score += scoring_config.get('exact_match_bonus', 20)
        elif release_title in album_norm or album_norm in release_title:
            score += scoring_config.get('partial_match_bonus', 10)
        
        # Release status
        status = (release.get('status') or '').lower()
        if status == 'official':
            score += scoring_config.get('official_status_bonus', 15)
        elif status == 'bootleg':
            score += scoring_config.get('bootleg_penalty', -40)
        
        # Reissue flag
        if release.get('is_reissue', False):
            score += scoring_config.get('reissue_penalty', -40)
        
        # Country of origin
        country = (release.get('country') or '').lower()
        if country in ['us', 'usa', 'united states']:
            score += scoring_config.get('us_country_bonus', 10)
        
        # Year check (relative to the band's active period)
        year_str = release.get('year', '')
        if year_str:
            try:
                year = int(year_str)
                current_year = datetime.now().year
                
                # Check for future dates (impossible for original release)
                if year > current_year:
                    score += scoring_config.get('future_date_penalty', -50)
                    return max(0, score)  # Immediately return with a low score
                
                # Use the artist period context if available
                if self.artist_period_context:
                    start_year = self.artist_period_context.get('start_year')
                    end_year = self.artist_period_context.get('end_year')
                    
                    if start_year:
                        # If artist is still active (end_year is None)
                        if end_year is None:
                            if start_year <= year <= current_year:
                                score += scoring_config.get('active_period_bonus', 20)  # Release during active period
                            if year >= current_year - 2:
                                score += scoring_config.get('recent_release_penalty', -10)  # Recent releases more likely to be reissues
                        # If artist is no longer active
                        else:
                            if start_year <= year <= end_year:
                                score += scoring_config.get('active_period_bonus', 20)  # Release during confirmed active period
                            elif year > end_year:
                                score += scoring_config.get('post_breakup_penalty', -20)  # Release after breakup (likely reissue)
                    else:
                        # Fallback to general rules if no activity period info
                        if 1950 <= year <= 2010:
                            score += scoring_config.get('valid_year_bonus', 15)
                        elif year > 2018:
                            score += scoring_config.get('very_new_year_penalty', -5)
                else:
                    # Fallback if context not available
                    if 1950 <= year <= 2010:
                        score += scoring_config.get('valid_year_bonus', 15)
                    elif year > 2018:
                        score += scoring_config.get('very_new_year_penalty', -5)
            except ValueError:
                pass
        
        # Packaging analysis
        packaging = (release.get('packaging') or '').lower()
        if packaging in ['digipak', 'digibook']:
            score += scoring_config.get('digipak_penalty', -5)  # Digipak often used for reissues
        elif packaging in ['jewel case']:
            score += scoring_config.get('jewel_case_bonus', 5)  # Jewel case more common for original CD releases
        
        return max(0, min(score, 100))  # Limit to 0-100 range  # Limit to 0-100 range

    async def get_album_year(self, artist: str, album: str) -> Optional[str]:
        """
        Determine the original release year for an album using API data.

        The function queries both MusicBrainz and Discogs, clusters the results by year,
        sums the scores for each year, and selects the year with the highest score. In case
        of similar scores, the earliest valid year is chosen.

        This method first fetches the artist's activity period and sets it as a context
        for scoring releases, then clears the context after use.

        Args:
            artist (str): Artist name.
            album (str): Album name.

        Returns:
            Optional[str]: The determined original release year as a string, or None if not found.

        Example:
            >>> year = await service.get_album_year("Abney Park", "Æther Shanties")
            >>> print("Original release year:", year)
        """
        # Normalize inputs to improve matching
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)
        
        self.console_logger.info(f"Searching for original release year: '{artist_norm} - {album_norm}'")
        
        # Get artist's activity period for better year evaluation
        start_year, end_year = await self.get_artist_activity_period(artist_norm)
        
        # Set artist period context for this request
        self.artist_period_context = {
            'start_year': start_year,
            'end_year': end_year
        }
        
        self.console_logger.info(f"Artist activity period: {start_year or 'unknown'} - {end_year or 'present'}")
        
        try:
            # Get data from both APIs
            results = await asyncio.gather(
                self._get_scored_releases_from_musicbrainz(artist_norm, album_norm),
                self._get_scored_releases_from_discogs(artist_norm, album_norm),
                return_exceptions=True
            )
            
            # Prepare data
            musicbrainz_data = results[0] if isinstance(results[0], list) else []
            discogs_data = results[1] if len(results) > 1 and isinstance(results[1], list) else []
            
            # Logic with clustering releases by years
            all_releases = []
            if musicbrainz_data:
                all_releases.extend(musicbrainz_data)
            if discogs_data:
                all_releases.extend(discogs_data)
            
            # Nothing found
            if not all_releases:
                self.console_logger.info(f"No release data found for '{artist} - {album}'")
                return None
            
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
            
            # Sort years by score
            sorted_years = sorted(year_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Log results
            self.console_logger.info(f"Year scores: " + ", ".join([f"{y}:{s}" for y, s in sorted_years]))
            
            # Take the year with the highest score
            if sorted_years:
                best_year = sorted_years[0][0]
                best_score = sorted_years[0][1]
                
                # Additional check: if there are multiple years with close scores,
                # prefer the earliest year (more likely to be the original release)
                close_years = [y for y, s in sorted_years if s >= best_score * 0.85]
                
                if len(close_years) > 1:
                    # Among years with close scores, take the earliest (but valid)
                    valid_years = [int(y) for y in close_years if self._is_valid_year(y, 75)]
                    if valid_years:
                        earliest_valid_year = str(min(valid_years))
                        self.console_logger.info(f"Multiple high-scoring years found. Using earliest valid: {earliest_valid_year}")
                        return earliest_valid_year
                
                self.console_logger.info(f"Selected best year by score: {best_year} (score: {best_score})")
                return best_year
            
            return None
        finally:
            # Always clear the artist period context after use
            self.artist_period_context = None

    async def _get_scored_releases_from_musicbrainz(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from MusicBrainz for the given artist and album.
        The function first attempts to search for release groups; if none are found,
        it falls back to a general release search.

        Args:
            artist (str): Normalized artist name.
            album (str): Normalized album name.

        Returns:
            List[Dict[str, Any]]: A list of scored release dictionaries.

        Example:
            >>> releases = await service._get_scored_releases_from_musicbrainz("Abney Park", "Æther Shanties")
            >>> for r in releases: print(r)
        """
        await self._respect_rate_limit('musicbrainz')
        scored_releases: List[Dict[str, Any]] = []
        try:
            await self.initialize()
            headers = {'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'}
            artist_encoded = urllib.parse.quote_plus(artist)
            album_encoded = urllib.parse.quote_plus(album)
            search_url = f"https://musicbrainz.org/ws/2/release-group/?query=artist:{artist_encoded} AND releasegroup:{album_encoded}&fmt=json"
            async with self.session.get(search_url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    self.error_logger.error(f"MusicBrainz API error: {response.status} for {artist} - {album}")
                    return []
                data = await response.json()
                release_groups = data.get("release-groups", [])
            if not release_groups:
                search_url = f"https://musicbrainz.org/ws/2/release/?query=artist:{artist_encoded} AND release:{album_encoded}&fmt=json"
                async with self.session.get(search_url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        self.error_logger.error(f"MusicBrainz API error: {response.status} for {artist} - {album}")
                        return []
                    data = await response.json()
                    releases = data.get("releases", [])
            else:
                self.console_logger.info(f"Found {len(release_groups)} release groups for {artist} - {album}")
                top_release_group = release_groups[0]
                release_group_id = top_release_group.get("id")
                releases_url = f"https://musicbrainz.org/ws/2/release?release-group={release_group_id}&fmt=json"
                async with self.session.get(releases_url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        self.error_logger.error(f"MusicBrainz API error: {response.status} for release group {release_group_id}")
                        releases = []
                    else:
                        data = await response.json()
                        releases = data.get("releases", [])
                        primary_type = top_release_group.get("primary-type", "Unknown")
                        first_release_date = top_release_group.get("first-release-date", "Unknown")
                        self.console_logger.info(f"Release group info - Type: {primary_type}, First release date: {first_release_date}")
            if not releases:
                self.console_logger.info(f"No results from MusicBrainz for '{artist} - {album}'")
                return []
            for release in releases:
                year = None
                release_date = release.get("date", "")
                if release_date:
                    year_match = re.search(r'^(\d{4})', release_date)
                    if year_match:
                        year = year_match.group(1)
                country = release.get('country', '')
                status = release.get('status', '')
                packaging = release.get('packaging', '')
                title = release.get('title', '')
                is_reissue = any(kw in title.lower() for kw in ['reissue', 'remaster', 'anniversary', 'edition'])
                release_data = {
                    'source': 'musicbrainz',
                    'title': title,
                    'type': release.get('primary-type', '') or release.get('release-group', {}).get('primary-type', ''),
                    'status': status,
                    'year': year,
                    'country': country,
                    'packaging': packaging,
                    'is_reissue': is_reissue,
                    'score': 0
                }
                if release_data['year']:
                    release_data['score'] = self._score_original_release(release_data, artist, album)
                    scored_releases.append(release_data)
            scored_releases.sort(key=lambda x: x['score'], reverse=True)
            self.console_logger.info(f"Scored {len(scored_releases)} results from MusicBrainz")
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"MB #{i+1}: {r['title']} ({r['year']}) - Status: {r['status']}, Country: {r['country']}, Score: {r['score']}"
                )
            return scored_releases
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from MusicBrainz for '{artist} - {album}': {e}", exc_info=True)
            return []

    async def _get_scored_releases_from_discogs(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Retrieve and score releases from Discogs for the given artist and album.

        Args:
            artist (str): Normalized artist name.
            album (str): Normalized album name.

        Returns:
            List[Dict[str, Any]]: A list of scored release dictionaries.

        Example:
            >>> releases = await service._get_scored_releases_from_discogs("Abney Park", "Æther Shanties")
            >>> print(releases)
        """
        await self._respect_rate_limit('discogs')
        scored_releases: List[Dict[str, Any]] = []
        try:
            await self.initialize()
            token = self.config.get('year_retrieval', {}).get('discogs_token')
            headers = {
                'Authorization': f'Discogs token={token}',
                'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'
            } if token else {'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'}
            artist_encoded = urllib.parse.quote_plus(artist)
            album_encoded = urllib.parse.quote_plus(album)
            search_url = f"https://api.discogs.com/database/search?artist={artist_encoded}&release_title={album_encoded}&type=release"
            self.console_logger.info(f"Discogs query: {search_url}")
            async with self.session.get(search_url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    self.error_logger.error(f"Discogs API error: {response.status} for {artist} - {album}")
                    return []
                data = await response.json()
                results = data.get('results', [])
            if not results:
                self.console_logger.info(f"No results from Discogs for '{artist} - {album}'")
                return []
            for item in results:
                title = item.get('title', '')
                is_reissue = any(kw.lower() in title.lower() for kw in ['reissue', 'remaster', 'anniversary', 'edition'])
                formats = item.get('format', [])
                formats_str = ', '.join(formats) if isinstance(formats, list) else str(formats)
                release_data = {
                    'source': 'discogs',
                    'title': title,
                    'type': item.get('format', [''])[0] if item.get('format') else '',
                    'status': 'official',
                    'year': str(item.get('year', '')),
                    'country': item.get('country', ''),
                    'is_reissue': is_reissue,
                    'format': formats_str,
                    'score': 0
                }
                if release_data['year']:
                    release_data['score'] = self._score_original_release(release_data, artist, album)
                    scored_releases.append(release_data)
            scored_releases.sort(key=lambda x: x['score'], reverse=True)
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"Discogs #{i+1}: {r['title']} ({r['year']}) - Country: {r.get('country', 'Unknown')}, Format: {r.get('format', 'Unknown')}, Score: {r['score']}"
                )
            self.console_logger.info(f"Scored {len(scored_releases)} results from Discogs")
            return scored_releases
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from Discogs for '{artist} - {album}': {e}", exc_info=True)
            return []