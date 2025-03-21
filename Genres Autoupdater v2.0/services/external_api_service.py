#!/usr/bin/env python3

"""
External API Service Module

This module provides an abstraction for interacting with external APIs to retrieve album years.
It centralizes the logic for handling rate limits, applying a preferred API, and error handling.

Example:
    >>> import asyncio
    >>> import logging
    >>> from external_api_service import ExternalApiService
    >>> config = {
    ...     "year_retrieval": {
    ...         "discogs_token": "your_discogs_token",
    ...         "preferred_api": "discogs"
    ...     }
    ... }
    >>> console_logger = logging.getLogger("console_logger")
    >>> error_logger = logging.getLogger("error_logger")
    >>> service = ExternalApiService(config, console_logger, error_logger)
    >>> async def test():
    ...     year = await service.get_album_year("Some Artist", "Some Album")
    ...     print(year)
    >>> asyncio.run(test())
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
    A service to interact with external APIs (MusicBrainz and Discogs) to retrieve original album release years.
    
    This service implements sophisticated algorithms to determine the most accurate original release year 
    by scoring and clustering release data. It handles rate limiting, API preferences, and session management
    while filtering out reissues and identifying the most likely original release.
    
    :param config: Configuration dictionary loaded from my-config.yaml.
    :param console_logger: Logger for console output.
    :param error_logger: Logger for error output.
    """
    def __init__(self, config: Dict[str, Any], console_logger: logging.Logger, error_logger: logging.Logger):
        self.config = config
        self.console_logger = console_logger
        self.error_logger = error_logger
        self.session = None
        self.rate_limits = {
            'discogs': {'requests_per_minute': 25, 'last_request': 0},
            'musicbrainz': {'requests_per_second': 1, 'last_request': 0}
        }
        # Preferred API (discogs or musicbrainz)
        self.preferred_api = config.get('year_retrieval', {}).get('preferred_api', 'musicbrainz')
        
        # Release year validation
        self.min_valid_year = config.get('year_retrieval', {}).get('min_valid_year', 1900)
        self.trust_threshold = config.get('year_retrieval', {}).get('trust_threshold', 50)
        self.max_year_difference = config.get('year_retrieval', {}).get('max_year_difference', 5)
        
        # Current year for maximum limit
        self.current_year = datetime.now().year + 1  # +1 for future releases
        
    async def initialize(self, force=False):
        """Initialize the aiohttp session"""
        if force or not self.session or self.session.closed:
            # Close the current session if it exists and is being forced to refresh
            if force and self.session and not self.session.closed:
                await self.session.close()
            
            self.session = aiohttp.ClientSession()
            self.console_logger.info("External API session initialized" + (" (forced)" if force else ""))
            
    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def _respect_rate_limit(self, api_name: str) -> None:
        """Respect the rate limit for the specified API"""
        rate_limit = self.rate_limits.get(api_name)
        if not rate_limit:
            return
            
        if api_name == 'discogs':
            # Maximum 25 requests per minute (60/25 = 2.4 seconds between requests)
            min_interval = 60 / rate_limit['requests_per_minute']
        elif api_name == 'musicbrainz':
            # Maximum 1 request per second
            min_interval = 1 / rate_limit['requests_per_second']
        else:
            min_interval = 1  # Default
            
        elapsed = time.time() - rate_limit['last_request']
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
            
        rate_limit['last_request'] = time.time()
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for better API matching"""
        # Remove common suffixes that might confuse API matching
        suffixes = [' - EP', ' - Single', ' EP', ' (Deluxe)', ' (Remastered)']
        result = name
        
        for suffix in suffixes:
            if result.endswith(suffix):
                result = result[:-len(suffix)]
                
        # Remove special characters that might affect search
        result = re.sub(r'[^\w\s]', ' ', result)
        # Normalize whitespace
        result = re.sub(r'\s+', ' ', result).strip()
        
        return result

    def _is_valid_year(self, year_str: str, score: int) -> bool:
        """
        Validates whether a year is sensible
        
        Args:
            year_str: The year string to validate
            score: The confidence score of the match
            
        Returns:
            bool: True if the year seems valid
        """
        try:
            year = int(year_str)
            # Базова перевірка на діапазон
            if year < self.min_valid_year or year > self.current_year:
                return False
                
            # Якщо результат має низький бал, вимагаємо більше доказів
            if score < self.trust_threshold:
                # Для результатів з низькою довірою обмежуємо ще більше
                current_year = datetime.now().year
                if year < 1950 or year > current_year:
                    return False
                    
            return True
        except (ValueError, TypeError):
            return False
            
    async def _get_scored_releases_from_musicbrainz(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Get and score releases from MusicBrainz API with advanced original release detection.
        
        This method searches for release groups first, then retrieves individual releases.
        It extracts release years, scores each result based on likelihood of being an
        original release (not a reissue), and handles rate limiting for the API.
        
        :param artist: Artist name (normalized)
        :param album: Album name (normalized)
        :return: List of releases with scores and years
        """
        await self._respect_rate_limit('musicbrainz')
        scored_releases = []
        
        try:
            await self.initialize()
            
            # Include your application name in the user agent
            headers = {'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'}
            
            # Properly encode search terms
            artist_encoded = urllib.parse.quote_plus(artist)
            album_encoded = urllib.parse.quote_plus(album)
            
            # Search for the release GROUP first (important change!)
            # This helps identify the main release group, not individual re-releases
            search_url = f"https://musicbrainz.org/ws/2/release-group/?query=artist:{artist_encoded} AND releasegroup:{album_encoded}&fmt=json"
            
            async with self.session.get(search_url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    self.error_logger.error(f"MusicBrainz API error: {response.status} for {artist} - {album}")
                    return []
                    
                data = await response.json()
                release_groups = data.get("release-groups", [])
            
            # If no release groups found, try normal release search
            if not release_groups:
                search_url = f"https://musicbrainz.org/ws/2/release/?query=artist:{artist_encoded} AND release:{album_encoded}&fmt=json"
                async with self.session.get(search_url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        self.error_logger.error(f"MusicBrainz API error: {response.status} for {artist} - {album}")
                        return []
                        
                    data = await response.json()
                    releases = data.get("releases", [])
            else:
                # For each release group, get all releases
                self.console_logger.info(f"Found {len(release_groups)} release groups for {artist} - {album}")
                
                # Take just the top release group
                if release_groups:
                    top_release_group = release_groups[0]
                    release_group_id = top_release_group.get("id")
                    
                    # Get all releases for this release group
                    releases_url = f"https://musicbrainz.org/ws/2/release?release-group={release_group_id}&fmt=json"
                    
                    async with self.session.get(releases_url, headers=headers, timeout=10) as response:
                        if response.status != 200:
                            self.error_logger.error(f"MusicBrainz API error: {response.status} for release group {release_group_id}")
                            releases = []
                        else:
                            data = await response.json()
                            releases = data.get("releases", [])
                            
                            # Log the primary release type and first release date
                            primary_type = top_release_group.get("primary-type", "Unknown")
                            first_release_date = top_release_group.get("first-release-date", "Unknown")
                            self.console_logger.info(f"Release group info - Type: {primary_type}, First release date: {first_release_date}")
                else:
                    releases = []
            
            if not releases:
                self.console_logger.info(f"No results from MusicBrainz for '{artist} - {album}'")
                return []
                
            # Score and filter results
            for release in releases:
                # Get the release date
                year = None
                release_date = release.get("date", "")
                if release_date:
                    year_match = re.search(r'^(\d{4})', release_date)
                    if year_match:
                        year = year_match.group(1)
                
                # Create release data object
                country = release.get('country', '')
                status = release.get('status', '')
                packaging = release.get('packaging', '')
                
                # Detect if this is a reissue/remaster
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
                
                # Only add releases with years
                if release_data['year']:
                    release_data['score'] = self._score_original_release(release_data, artist, album)
                    scored_releases.append(release_data)
                        
            # Sort results by score
            scored_releases.sort(key=lambda x: x['score'], reverse=True)
            
            # Log results
            self.console_logger.info(f"Scored {len(scored_releases)} results from MusicBrainz")
            
            # Show top results
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"MB #{i+1}: {r['title']} ({r['year']}) - "
                    f"Status: {r['status']}, Country: {r['country']}, "
                    f"Score: {r['score']}"
                )
                
            return scored_releases
                
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from MusicBrainz for '{artist} - {album}': {e}", exc_info=True)
            return []
            
    async def _get_scored_releases_from_discogs(self, artist: str, album: str) -> List[Dict[str, Any]]:
        """
        Get and score releases from Discogs API with advanced original release detection.
        
        This method searches the Discogs database for releases matching the artist and album.
        It extracts release years, formats, and other metadata, then scores each result based 
        on likelihood of being an original release versus a reissue. It handles API
        authentication, rate limiting, and Discogs-specific response processing.
        
        :param artist: Artist name (normalized)
        :param album: Album name (normalized)
        :return: List of releases with scores and years
        """
        await self._respect_rate_limit('discogs')
        scored_releases = []
        
        try:
            await self.initialize()
            
            # Get Discogs API token from config
            token = self.config.get('year_retrieval', {}).get('discogs_token')
            headers = {
                'Authorization': f'Discogs token={token}',
                'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'
            } if token else {'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'}
            
            # URL encode parameters properly
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
                
            # Score and filter results
            for item in results:
                # Detect if this is a reissue/remaster
                title = item.get('title', '')
                is_reissue = any(kw.lower() in title.lower() for kw in ['reissue', 'remaster', 'anniversary', 'edition'])
                
                # Get format information
                formats = item.get('format', [])
                formats_str = ', '.join(formats) if isinstance(formats, list) else str(formats)
                                
                release_data = {
                    'source': 'discogs',
                    'title': title,
                    'type': item.get('format', [''])[0] if item.get('format') else '',
                    'status': 'official',  # Discogs mostly shows official releases
                    'year': str(item.get('year', '')),
                    'country': item.get('country', ''),
                    'is_reissue': is_reissue,
                    'format': formats_str,
                    'score': 0  # Will be updated below
                }
                
                # Evaluate and add only results with a year
                if release_data['year']:
                    release_data['score'] = self._score_original_release(release_data, artist, album)
                    scored_releases.append(release_data)
                
            # Sort results by score
            scored_releases.sort(key=lambda x: x['score'], reverse=True)
            
            # Show top results
            for i, r in enumerate(scored_releases[:3]):
                self.console_logger.info(
                    f"Discogs #{i+1}: {r['title']} ({r['year']}) - "
                    f"Country: {r.get('country', 'Unknown')}, "
                    f"Format: {r.get('format', 'Unknown')}, "
                    f"Score: {r['score']}"
                )
            
            self.console_logger.info(f"Scored {len(scored_releases)} results from Discogs")
            return scored_releases
                
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from Discogs for '{artist} - {album}': {e}", exc_info=True)
            return []
    
    def _score_original_release(self, release: Dict[str, Any], artist: str, album: str) -> int:
        """
        Advanced scoring algorithm focused on identifying original releases vs reissues.
        
        Args:
            release: The release data from API
            artist: Searched artist name
            album: Searched album name
            
        Returns:
            int: Score from 0-100, higher is better match
        """
        score = 0
        
        # Basic score - starting from 20
        score += 20
        
        # 1. Check for exact match of album title
        release_title = release.get('title', '').lower()
        album_norm = album.lower()
        
        if release_title == album_norm:
            score += 20  # Exact match
        elif release_title in album_norm or album_norm in release_title:
            score += 10  # Partial match
        
        # 2. Release status (official is best)
        status = release.get('status', '').lower()
        if status == 'official':
            score += 15
        elif status == 'bootleg':
            score -= 40  # Strongly reduces score for bootlegs
        
        # 3. Key factor: reissues!
        is_reissue = release.get('is_reissue', False)
        if is_reissue:
            score -= 40  # Critical reduction for reissues
        
        # 4. Country of origin (favoring the US for most metal)
        country = release.get('country', '').lower()
        if country in ['us', 'usa', 'united states']:
            score += 10
        
        # 5. Year check (relative to the band's active period)
        year_str = release.get('year', '')
        if year_str:
            try:
                year = int(year_str)
                current_year = datetime.now().year
                
                # Check for future dates (impossible for original release)
                if year > current_year:
                    score -= 50
                    return max(0, score)  # Immediately return with a low score
                
                # Considering the specifics of metal (mostly from the mid-80s)
                # Finding a plausible "active period" range
                artist_lower = artist.lower()
                
                # Examples of well-known bands (can be expanded)
                if 'agalloch' in artist_lower:
                    # Agalloch were active from 1995 to 2016
                    if 1995 <= year <= 2016:
                        score += 20  # Bonus for releases during the active period
                    elif year > 2016:
                        score -= 25  # Discount for releases after the breakup
                elif 'alcest' in artist_lower:
                    # Alcest were active from 2000
                    if 2000 <= year <= 2015:
                        score += 20
                else:
                    # General case - favor older releases,
                    # but not too old (unlikely for a metal album to be from the 70s)
                    if 1980 <= year <= 2010:
                        score += 15
                    elif year > 2018:
                        score -= 5  # Small discount for very new releases (often reissues)
            except ValueError:
                pass
        
        # 6. Додатковий аналіз упаковки (часто вказує на перевидання)
        packaging = release.get('packaging', '').lower()
        if packaging in ['digipak', 'digibook']:
            score -= 5  # Digipak часто використовують для перевидань
        elif packaging in ['jewel case']:
            score += 5  # Jewel case частіше для оригінальних CD релізів
        
        return max(0, min(score, 100))  # Обмежуємо в діапазоні 0-100

def _score_original_release(self, release: Dict[str, Any], artist: str, album: str) -> int:
    """
    Advanced scoring algorithm focused on identifying original releases vs reissues.
    
    Args:
        release: The release data from API
        artist: Searched artist name
        album: Searched album name
        
    Returns:
        int: Score from 0-100, higher is better match
    """
    score = 0
    
    # Базовий бал - починаємо з 20
    score += 20
    
    # 1. Перевірка на точний збіг назви альбому
    release_title = release.get('title', '').lower()
    album_norm = album.lower()
    
    if release_title == album_norm:
        score += 20  # Точний збіг
    elif release_title in album_norm or album_norm in release_title:
        score += 10  # Частковий збіг
    
    # 2. Статус релізу (офіційний найкращий)
    status = release.get('status', '').lower()
    if status == 'official':
        score += 15
    elif status == 'bootleg':
        score -= 40  # Сильно знижуємо рейтинг бутлегів
    
    # 3. Ключовий фактор: перевидання!
    is_reissue = release.get('is_reissue', False)
    if is_reissue:
        score -= 40  # Критичне зниження для перевидань
    
    # 4. Країна походження (надаємо перевагу США для більшості металу)
    country = release.get('country', '').lower()
    if country in ['us', 'usa', 'united states']:
        score += 10
    
    # 5. Перевірка року (відносно початку активності гурту)
    year_str = release.get('year', '')
    if year_str:
        try:
            year = int(year_str)
            current_year = datetime.now().year
            
            # Перевірка на майбутні дати (неможливо для оригінального релізу)
            if year > current_year:
                score -= 50
                return max(0, score)  # Одразу повертаємо з низьким балом
            
            # Use the artist period context if available
            if hasattr(self, 'artist_period_context'):
                start_year = self.artist_period_context.get('start_year')
                end_year = self.artist_period_context.get('end_year')
                
                if start_year:
                    # If artist is still active (end_year is None)
                    if end_year is None:
                        if start_year <= year <= current_year:
                            score += 15  # Release during active period
                        if year >= current_year - 2:
                            score -= 10  # Recent releases more likely to be reissues
                    # If artist is no longer active
                    else:
                        if start_year <= year <= end_year:
                            score += 20  # Release during confirmed active period
                        elif year > end_year:
                            score -= 20  # Release after breakup (likely reissue)
                else:
                    # Fallback to general rules if no activity period info
                    if 1950 <= year <= 2010:
                        score += 10
                    elif year > 2018:
                        score -= 5
            else:
                # Fallback if context not available
                if 1950 <= year <= 2010:
                    score += 10
                elif year > 2018:
                    score -= 5
                
        except ValueError:
            pass
    
    # 6. Додатковий аналіз упаковки (часто вказує на перевидання)
    packaging = release.get('packaging', '').lower()
    if packaging in ['digipak', 'digibook']:
        score -= 5  # Digipak часто використовують для перевидань
    elif packaging in ['jewel case']:
        score += 5  # Jewel case частіше для оригінальних CD релізів
    
    return max(0, min(score, 100))  # Обмежуємо в діапазоні 0-100


async def get_artist_activity_period(self, artist: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Retrieve an artist's period of activity by querying the MusicBrainz API.

    This method attempts to determine when an artist began releasing music and when/if they stopped.
    It first checks an internal cache to avoid redundant API calls. If the artist isn't in the cache,
    it searches MusicBrainz for the artist and checks for direct lifespan data. If that's unavailable,
    it analyzes the artist's release timeline to estimate their active period.

    :param artist: The artist name to query
    :return: A tuple of start year and end year (if available)
    """
    # Check cache first to avoid redundant API calls
    cache_key = f"activity_{artist.lower()}"
    
    # Initialize cache if doesn't exist
    if not hasattr(self, 'activity_cache'):
        self.activity_cache = {}
    elif cache_key in self.activity_cache:
        # Return cached data if available
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
                return None, None
                
            data = await response.json()
            artists = data.get("artists", [])
            
            if not artists:
                self.console_logger.warning(f"No artist found for query: {artist}")
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
                    return None, None
                    
                data = await response.json()
                release_groups = data.get("release-groups", [])
                
                if not release_groups:
                    self.console_logger.warning(f"No releases found for {artist}")
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
        return None, None

    async def get_album_year(self, artist: str, album: str) -> Optional[str]:
        """
        Advanced algorithm to find the original release year, not reissue years.
        
        Args:
            artist: Artist name
            album: Album name
            
        Returns:
            Optional[str]: The original release year if found, None otherwise
        """
        # Normalize inputs to improve matching
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)
        
        self.console_logger.info(f"Searching for original release year: '{artist_norm} - {album_norm}'")
        
        # Отримуємо дані з обох API
        results = await asyncio.gather(
            self._get_scored_releases_from_musicbrainz(artist_norm, album_norm),
            self._get_scored_releases_from_discogs(artist_norm, album_norm),
            return_exceptions=True
        )
        
        # Підготовка даних
        musicbrainz_data = results[0] if isinstance(results[0], list) else []
        discogs_data = results[1] if len(results) > 1 and isinstance(results[1], list) else []
        
        # Логіка з кластеризацією релізів за роками
        all_releases = []
        if musicbrainz_data:
            all_releases.extend(musicbrainz_data)
        if discogs_data:
            all_releases.extend(discogs_data)
        
        # Нічого не знайдено
        if not all_releases:
            self.console_logger.info(f"No release data found for '{artist} - {album}'")
            return None
        
        # Проводимо кластеризацію релізів за роками
        year_clusters = {}
        
        for release in all_releases:
            year = release.get('year')
            if not year:
                continue
                
            # Кластеризуємо релізи по роках
            if year not in year_clusters:
                year_clusters[year] = []
            year_clusters[year].append(release)
        
        # Логуємо кластери за роками
        self.console_logger.info(f"Year clusters: {', '.join(sorted(year_clusters.keys()))}")
        
        # Обчислюємо сумарний бал для кожного року
        year_scores = {}
        for year, releases in year_clusters.items():
            # Беремо топ-3 релізи за балом для кожного року
            top_releases = sorted(releases, key=lambda x: x.get('score', 0), reverse=True)[:3]
            
            # Сумарний бал - сума балів топ-3 релізів
            year_scores[year] = sum(r.get('score', 0) for r in top_releases)
        
        # Сортуємо роки за балом
        sorted_years = sorted(year_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Логуємо результати
        self.console_logger.info(f"Year scores: " + ", ".join([f"{y}:{s}" for y, s in sorted_years]))
        
        # Беремо рік з найвищим балом
        if sorted_years:
            best_year = sorted_years[0][0]
            best_score = sorted_years[0][1]
            
            # Додаткова перевірка: якщо є багато років з близькими балами,
            # віддаємо перевагу найранішому року (ймовірніше оригінальному релізу)
            close_years = [y for y, s in sorted_years if s >= best_score * 0.85]
            
            if len(close_years) > 1:
                # Серед років з близькими балами беремо найраніший (але валідний)
                valid_years = [int(y) for y in close_years if self._is_valid_year(y, 75)]
                if valid_years:
                    earliest_valid_year = str(min(valid_years))
                    self.console_logger.info(f"Multiple high-scoring years found. Using earliest valid: {earliest_valid_year}")
                    return earliest_valid_year
            
            self.console_logger.info(f"Selected best year by score: {best_year} (score: {best_score})")
            return best_year
        
        return None