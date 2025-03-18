import asyncio
import logging
import re
import time
import urllib.parse

from typing import Any, Dict, Optional

import aiohttp

class ExternalApiService:
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
        
    async def initialize(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            self.console_logger.info("External API session initialized")
            
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
    
    async def get_album_year(self, artist: str, album: str) -> Optional[str]:
        """Get album year from external APIs with fallback"""
        # Normalize inputs to improve matching
        artist_norm = self._normalize_name(artist)
        album_norm = self._normalize_name(album)
        
        self.console_logger.info(f"Searching for year: '{artist_norm} - {album_norm}'")
        
        # Try preferred API first
        if self.preferred_api == 'discogs':
            self.console_logger.info("Trying Discogs first...")
            year = await self.get_album_year_from_discogs(artist_norm, album_norm)
            
            if not year:
                self.console_logger.info("Discogs search failed, trying MusicBrainz...")
                year = await self.get_album_year_from_musicbrainz(artist_norm, album_norm)
        else:
            self.console_logger.info("Trying MusicBrainz first...")
            year = await self.get_album_year_from_musicbrainz(artist_norm, album_norm)
            
            if not year:
                self.console_logger.info("MusicBrainz search failed, trying Discogs...")
                year = await self.get_album_year_from_discogs(artist_norm, album_norm)
                
        return year
    
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

    async def get_album_year_from_discogs(self, artist: str, album: str) -> Optional[str]:
        """Get album year from Discogs"""
        await self._respect_rate_limit('discogs')
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
                    return None
                    
                data = await response.json()
                results = data.get('results', [])
                
                if results:
                    # Get the first result's year
                    year = results[0].get('year')
                    if year:
                        self.console_logger.info(f"Found year {year} for '{artist} - {album}' on Discogs")
                        return str(year)
                        
            self.console_logger.info(f"No year found for '{artist} - {album}' on Discogs")
            return None
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from Discogs for '{artist} - {album}': {e}", exc_info=True)
            return None
            
    async def get_album_year_from_musicbrainz(self, artist: str, album: str) -> Optional[str]:
        """Get album year from MusicBrainz"""
        await self._respect_rate_limit('musicbrainz')
        try:
            await self.initialize()
            
            # Include your application name in the user agent
            headers = {'User-Agent': 'MusicGenreUpdater/2.0 (roman.borodavkin@gmail.com)'}
            
            # Search for the release
            search_url = f"https://musicbrainz.org/ws/2/release/?query=artist:{artist} AND release:{album}&fmt=json"
            async with self.session.get(search_url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    self.error_logger.error(f"MusicBrainz API error: {response.status} for {artist} - {album}")
                    return None
                    
                data = await response.json()
                releases = data.get('releases', [])
                if releases:
                    # Get the first release's date
                    release_date = releases[0].get('date', '')
                    if release_date:
                        # Extract the year from the date (YYYY-MM-DD format)
                        year = release_date.split('-')[0]
                        self.console_logger.info(f"Found year {year} for '{artist} - {album}' on MusicBrainz")
                        return year
                        
            self.console_logger.info(f"No year found for '{artist} - {album}' on MusicBrainz")
            return None
        except Exception as e:
            self.error_logger.error(f"Error retrieving year from MusicBrainz for '{artist} - {album}': {e}", exc_info=True)
            return None