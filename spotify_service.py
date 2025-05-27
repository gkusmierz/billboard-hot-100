#!/usr/bin/env python3
"""
Spotify API Service

This module provides functionality to search for tracks on Spotify
and retrieve their URLs using the Spotify Web API.
"""

import requests
import base64
import json
import time
from typing import Dict, Optional, List
from urllib.parse import quote


class SpotifyService:
    """Service class for interacting with Spotify Web API."""
    
    def __init__(self, client_id: str, client_secret: str):
        """
        Initialize Spotify service with credentials.
        
        Args:
            client_id: Spotify app client ID
            client_secret: Spotify app client secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.token_expires_at = 0
        self.base_url = "https://api.spotify.com/v1"
    
    def _get_access_token(self) -> bool:
        """Get access token from Spotify API."""
        auth_url = "https://accounts.spotify.com/api/token"
        
        # Encode credentials
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "client_credentials"
        }
        
        try:
            response = requests.post(auth_url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data["access_token"]
            # Set expiration time (subtract 60 seconds for safety)
            self.token_expires_at = time.time() + token_data["expires_in"] - 60
            
            print("Successfully obtained Spotify access token")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting access token: {e}")
            return False
    
    def _ensure_valid_token(self) -> bool:
        """Ensure we have a valid access token."""
        if not self.access_token or time.time() >= self.token_expires_at:
            return self._get_access_token()
        return True
    
    def search_track(self, song: str, artist: str) -> Optional[Dict]:
        """
        Search for a track on Spotify.
        
        Args:
            song: Song title
            artist: Artist name
            
        Returns:
            Dictionary with track info or None if not found
        """
        if not self._ensure_valid_token():
            return None
        
        # Clean and format search query
        query = f'track:"{song}" artist:"{artist}"'
        encoded_query = quote(query)
        
        url = f"{self.base_url}/search"
        params = {
            "q": encoded_query,
            "type": "track",
            "limit": 1,
            "market": "US"
        }
        
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            tracks = data.get("tracks", {}).get("items", [])
            
            if tracks:
                track = tracks[0]
                return {
                    "trackId": track["external_urls"]["spotify"],
                    "spotify_id": track["id"],
                    "name": track["name"],
                    "artists": [artist["name"] for artist in track["artists"]],
                    "album": track["album"]["name"],
                    "release_date": track["album"]["release_date"],
                    "popularity": track["popularity"]
                }
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error searching for track '{song}' by '{artist}': {e}")
            return None
    
    def search_tracks_batch(self, songs: List[Dict], delay: float = 0.1) -> List[Dict]:
        """
        Search for multiple tracks with rate limiting.
        
        Args:
            songs: List of dictionaries with 'song' and 'artist' keys
            delay: Delay between requests in seconds
            
        Returns:
            List of songs with trackId added
        """
        results = []
        total = len(songs)
        
        for i, song_data in enumerate(songs, 1):
            song = song_data.get("song", "")
            artist = song_data.get("artist", "")
            
            print(f"Searching {i}/{total}: '{song}' by '{artist}'")
            
            track_info = self.search_track(song, artist)
            
            # Create result with original data
            result = {
                "song": song,
                "artist": artist
            }
            
            if track_info:
                result["trackId"] = track_info["trackId"]
                print(f"  ✓ Found: {track_info['trackId']}")
            else:
                result["trackId"] = None
                print(f"  ✗ Not found")
            
            results.append(result)
            
            # Rate limiting
            if delay > 0 and i < total:
                time.sleep(delay)
        
        return results


def load_spotify_credentials() -> tuple:
    """
    Load Spotify credentials from environment or config file.
    
    Returns:
        Tuple of (client_id, client_secret)
    """
    import os
    
    # Try environment variables first
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    
    if client_id and client_secret:
        return client_id, client_secret
    
    # Try config file
    try:
        with open("spotify_config.json", "r") as f:
            config = json.load(f)
            return config["client_id"], config["client_secret"]
    except FileNotFoundError:
        print("No spotify_config.json found")
    except KeyError as e:
        print(f"Missing key in spotify_config.json: {e}")
    
    return None, None


if __name__ == "__main__":
    # Example usage
    client_id, client_secret = load_spotify_credentials()
    
    if not client_id or not client_secret:
        print("Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables")
        print("Or create a spotify_config.json file with your credentials")
        exit(1)
    
    spotify = SpotifyService(client_id, client_secret)
    
    # Test search
    result = spotify.search_track("Bohemian Rhapsody", "Queen")
    if result:
        print(f"Found: {result['trackId']}")
    else:
        print("Track not found")