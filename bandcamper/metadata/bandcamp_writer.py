"""
Bandcamp Metadata Writer

Extracts metadata from Bandcamp album pages and writes it to audio files
using bandcamper's existing metadata infrastructure.

Adapted from bcmetadatadl functionality.
"""

import json
import re
from typing import Dict, List, Optional
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from bandcamper.metadata.utils import get_track_metadata, suffix_to_metadata


class BandcampMetadataExtractor:
    """Extracts metadata from Bandcamp album pages."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def extract_album_metadata(self, url: str) -> Optional[Dict]:
        """Extract album metadata from a Bandcamp URL.
        
        Returns:
            Dict with keys: artist, album, year, tracks, url
            None if extraction fails
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Warning: Failed to fetch URL {url}: {e}")
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        try:
            # Extract album title
            album_title = self._extract_album_title(soup)
            if not album_title:
                print(f"Warning: Could not find album title for {url}")
                return None
            
            # Extract artist name
            artist_name = self._extract_artist_name(soup)
            if not artist_name:
                print(f"Warning: Could not find artist name for {url}")
                return None
            
            # Extract release year
            release_year = self._extract_release_year(soup, response.text)
            
            # Extract track list
            tracks = self._extract_tracks(soup)
            
            return {
                'artist': artist_name,
                'album': album_title,
                'year': release_year,
                'tracks': tracks,
                'url': url
            }
        except Exception as e:
            print(f"Warning: Error extracting metadata from {url}: {e}")
            return None
    
    def _extract_album_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract album title from the page."""
        # Try multiple selectors for album title
        selectors = [
            ('h2', {'class': 'trackTitle'}),
            ('h1', {'class': 'trackTitle'}),
            ('h2', {}),
        ]
        
        for tag, attrs in selectors:
            elem = soup.find(tag, attrs)
            if elem:
                title = elem.get_text().strip()
                if title:
                    return title
        return None
    
    def _extract_artist_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract artist name from the page."""
        # Try h3 with link first
        artist_elem = soup.find('h3')
        if artist_elem:
            artist_link = artist_elem.find('a')
            if artist_link:
                return artist_link.get_text().strip()
            else:
                # Fallback: try to extract from h3 text
                artist_text = artist_elem.get_text().strip()
                # Remove "by " prefix if present
                return re.sub(r'^by\s+', '', artist_text)
        
        # Try other selectors
        selectors = [
            ('span', {'itemprop': 'byArtist'}),
            ('a', {'class': 'artist'}),
        ]
        
        for tag, attrs in selectors:
            elem = soup.find(tag, attrs)
            if elem:
                artist = elem.get_text().strip()
                if artist:
                    return artist
        
        return None
    
    def _extract_release_year(self, soup: BeautifulSoup, page_text: str) -> Optional[str]:
        """Extract release year from various sources on the page."""
        # Priority 1: Look for "released" followed by a date in tralbum-credits section
        credits_section = soup.find('div', class_='tralbum-credits')
        if credits_section:
            credits_text = credits_section.get_text()
            # Look for "released Month Day, Year" pattern
            released_match = re.search(r'released\s+([A-Za-z]+\s+\d{1,2},?\s+(\d{4}))', credits_text, re.IGNORECASE)
            if released_match:
                year = released_match.group(2)
                if year.isdigit() and len(year) == 4:
                    year_int = int(year)
                    if 1950 <= year_int <= 2030:
                        return year
        
        # Priority 2: Look for year in meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            if meta.get('property') == 'music:release_date':
                content = meta.get('content', '')
                year_match = re.search(r'(\d{4})', content)
                if year_match:
                    year = year_match.group(1)
                    if year.isdigit() and len(year) == 4:
                        year_int = int(year)
                        if 1950 <= year_int <= 2030:
                            return year
        
        # Priority 3: Look for year in pagedata JSON (Bandcamp-specific)
        pagedata_div = soup.find('div', id='pagedata')
        if pagedata_div and pagedata_div.get('data-blob'):
            try:
                pagedata = json.loads(pagedata_div['data-blob'])
                # Look for release date in current item
                if 'current' in pagedata:
                    current = pagedata['current']
                    for date_field in ['release_date', 'publish_date']:
                        if date_field in current and current[date_field]:
                            date_str = str(current[date_field])
                            year_match = re.search(r'(\d{4})', date_str)
                            if year_match:
                                year = year_match.group(1)
                                if year.isdigit() and len(year) == 4:
                                    year_int = int(year)
                                    if 1950 <= year_int <= 2030:
                                        return year
            except (json.JSONDecodeError, KeyError, TypeError):
                pass
        
        # Priority 4: Try to find year in JSON-LD data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                if 'datePublished' in data:
                    year = data['datePublished'][:4]
                    if year.isdigit() and len(year) == 4:
                        year_int = int(year)
                        if 1950 <= year_int <= 2030:
                            return year
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        
        # Priority 5: Look for any 4-digit year in the page text (last resort)
        year_patterns = [
            r'\b(20[0-2][0-9])\b',  # 2000-2029
            r'\b(19[5-9][0-9])\b',  # 1950-1999
        ]
        
        for pattern in year_patterns:
            matches = re.findall(pattern, page_text)
            if matches:
                # Return the most recent valid year found
                valid_years = []
                for year in matches:
                    if year.isdigit() and len(year) == 4:
                        year_int = int(year)
                        if 1950 <= year_int <= 2030:
                            valid_years.append(year_int)
                if valid_years:
                    return str(max(valid_years))  # Return the most recent year
        
        # Priority 6: Look for copyright year
        copyright_patterns = [
            r'\u00a9\s*(\d{4})',
            r'copyright\s+(\d{4})',
        ]
        
        for pattern in copyright_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                for year in matches:
                    if year.isdigit() and len(year) == 4:
                        year_int = int(year)
                        if 1950 <= year_int <= 2030:
                            return year
        
        return None
    
    def _extract_tracks(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract track information from the track table."""
        tracks = []
        
        # Find the track table
        track_table = soup.find('table', id='track_table')
        if not track_table:
            return tracks
        
        # Find all track rows
        track_rows = track_table.find_all('tr', class_='track_row_view')
        
        for row in track_rows:
            track_info = {}
            
            # Extract track number
            track_num_elem = row.find('div', class_='track_number')
            if track_num_elem:
                track_num_text = track_num_elem.get_text().strip()
                track_num = re.sub(r'\D', '', track_num_text)  # Remove non-digits
                if track_num:
                    track_info['number'] = int(track_num)
            
            # Extract track title
            track_title_elem = row.find('span', class_='track-title')
            if track_title_elem:
                track_info['title'] = track_title_elem.get_text().strip()
            
            # Extract track duration
            time_elem = row.find('span', class_='time')
            if time_elem:
                track_info['duration'] = time_elem.get_text().strip()
            
            # Only add track if we have at least number and title
            if 'number' in track_info and 'title' in track_info:
                tracks.append(track_info)
        
        return tracks


class BandcampMetadataWriter:
    """Writes Bandcamp metadata to audio files using bandcamper's metadata classes."""
    
    def __init__(self):
        self.extractor = BandcampMetadataExtractor()
    
    def write_metadata_to_file(self, file_path: Path, bandcamp_url: str) -> bool:
        """Write metadata from Bandcamp URL to an audio file.
        
        Args:
            file_path: Path to the audio file
            bandcamp_url: Original Bandcamp URL for metadata extraction
            
        Returns:
            True if metadata was written successfully, False otherwise
        """
        # Check if file format is supported
        if file_path.suffix not in suffix_to_metadata:
            print(f"Warning: Unsupported file format {file_path.suffix} for {file_path}")
            return False
        
        # Extract metadata from Bandcamp
        album_metadata = self.extractor.extract_album_metadata(bandcamp_url)
        if not album_metadata:
            print(f"Warning: Could not extract metadata from {bandcamp_url}")
            return False
        
        try:
            # Get the appropriate metadata class for this file format
            track_metadata = get_track_metadata(file_path)
            if not track_metadata:
                print(f"Warning: Could not load metadata handler for {file_path}")
                return False
            
            # Find the matching track for this file
            track_info = self._find_matching_track(file_path, album_metadata['tracks'])
            
            # Write basic album metadata
            if hasattr(track_metadata, 'artist') and album_metadata.get('artist'):
                track_metadata.artist = album_metadata['artist']
            
            if hasattr(track_metadata, 'album') and album_metadata.get('album'):
                track_metadata.album = album_metadata['album']
            
            if hasattr(track_metadata, 'year') and album_metadata.get('year'):
                track_metadata.year = album_metadata['year']
            
            # Write track-specific metadata if found
            if track_info:
                if hasattr(track_metadata, 'title') and track_info.get('title'):
                    track_metadata.title = track_info['title']
                
                if hasattr(track_metadata, 'track_number') and track_info.get('number'):
                    track_metadata.track_number = track_info['number']
            
            # Save the metadata to the file
            track_metadata.save()
            
            print(f"Successfully wrote metadata to {file_path}")
            return True
            
        except Exception as e:
            print(f"Warning: Error writing metadata to {file_path}: {e}")
            return False
    
    def _find_matching_track(self, file_path: Path, tracks: List[Dict]) -> Optional[Dict]:
        """Find the track that matches this file.
        
        Uses filename parsing and fuzzy matching to identify the correct track.
        """
        if not tracks:
            return None
        
        filename = file_path.stem.lower()
        
        # Try to extract track number from filename
        track_num_match = re.search(r'(\d+)', filename)
        if track_num_match:
            file_track_num = int(track_num_match.group(1))
            # Find track with matching number
            for track in tracks:
                if track.get('number') == file_track_num:
                    return track
        
        # Try fuzzy matching with track titles
        for track in tracks:
            track_title = track.get('title', '').lower()
            if track_title and self._fuzzy_match(filename, track_title):
                return track
        
        # If no specific match found, return the first track for single-track releases
        if len(tracks) == 1:
            return tracks[0]
        
        return None
    
    def _fuzzy_match(self, str1: str, str2: str, threshold: float = 0.6) -> bool:
        """Simple fuzzy string matching."""
        # Remove common separators and normalize
        clean1 = re.sub(r'[_\-\s]+', ' ', str1).strip()
        clean2 = re.sub(r'[_\-\s]+', ' ', str2).strip()
        
        # Check if one string contains the other
        if clean1 in clean2 or clean2 in clean1:
            return True
        
        # Simple word overlap check
        words1 = set(clean1.split())
        words2 = set(clean2.split())
        
        if not words1 or not words2:
            return False
        
        overlap = len(words1.intersection(words2))
        total_words = len(words1.union(words2))
        
        return (overlap / total_words) >= threshold
