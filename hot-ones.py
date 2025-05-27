#!/usr/bin/env python3
"""
Billboard Hot 100 #1 Songs Extractor

This script processes all date files in the date/ directory to extract
the #1 song from each chart date and creates two output files:
- hot-ones.json: All #1 songs with their dates
- distinct-hot-ones.json: Unique #1 songs with Spotify track URLs (removing duplicates)
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Set
from spotify_service import SpotifyService, load_spotify_credentials


def load_date_file(filepath: str) -> Dict:
    """Load and parse a date JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error loading {filepath}: {e}")
        return {}


def extract_number_one_song(date_data: Dict) -> Dict:
    """Extract the #1 song from chart data."""
    if not date_data or 'data' not in date_data:
        return {}
    
    # Find the song with this_week position 1
    for song in date_data['data']:
        if song.get('this_week') == 1:
            return {
                'date': date_data.get('date'),
                'song': song.get('song'),
                'artist': song.get('artist')
            }
    
    return {}


def get_distinct_songs(hot_ones: List[Dict]) -> List[Dict]:
    """Get distinct songs by removing duplicates based on song title and artist."""
    seen_songs: Set[str] = set()
    distinct_songs: List[Dict] = []
    
    for entry in hot_ones:
        # Create a unique key combining song and artist
        song_key = f"{entry.get('song', '').strip().lower()}|{entry.get('artist', '').strip().lower()}"
        
        if song_key not in seen_songs and entry.get('song') and entry.get('artist'):
            seen_songs.add(song_key)
            # Remove date field for distinct songs
            distinct_entry = {
                'song': entry.get('song'),
                'artist': entry.get('artist')
            }
            distinct_songs.append(distinct_entry)
    
    return distinct_songs


def add_spotify_tracks(distinct_songs: List[Dict]) -> List[Dict]:
    """Add Spotify track URLs to distinct songs."""
    print("\n=== Adding Spotify Track URLs ===")
    
    # Load Spotify credentials
    client_id, client_secret = load_spotify_credentials()
    
    if not client_id or not client_secret:
        print("Warning: No Spotify credentials found!")
        print("Please either:")
        print("1. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables")
        print("2. Create spotify_config.json with your credentials (see spotify_config.json.example)")
        print("\nProceeding without Spotify integration...")
        
        # Add null trackId to all songs
        for song in distinct_songs:
            song['trackId'] = None
        return distinct_songs
    
    # Initialize Spotify service
    spotify = SpotifyService(client_id, client_secret)
    
    # Search for tracks with rate limiting
    print(f"Searching for {len(distinct_songs)} distinct songs on Spotify...")
    return spotify.search_tracks_batch(distinct_songs, delay=0.1)


def main():
    """Main function to process all date files and create output files."""
    date_dir = Path('date')
    
    if not date_dir.exists():
        print("Error: 'date' directory not found!")
        return
    
    hot_ones: List[Dict] = []
    processed_files = 0
    
    # Process all JSON files in the date directory
    for date_file in sorted(date_dir.glob('*.json')):
        print(f"Processing {date_file.name}...")
        
        date_data = load_date_file(date_file)
        if date_data:
            number_one = extract_number_one_song(date_data)
            if number_one:
                hot_ones.append(number_one)
                processed_files += 1
    
    print(f"\nProcessed {processed_files} files")
    print(f"Found {len(hot_ones)} #1 songs")
    
    # Save all #1 songs to hot-ones.json
    with open('hot-ones.json', 'w', encoding='utf-8') as f:
        json.dump({
            'total_entries': len(hot_ones),
            'description': 'All #1 songs from Billboard Hot 100 charts',
            'data': hot_ones
        }, f, indent=2, ensure_ascii=False)
    
    print(f"Saved all #1 songs to hot-ones.json")
    
    # Get distinct songs and add Spotify track URLs
    distinct_songs = get_distinct_songs(hot_ones)
    distinct_songs_with_spotify = add_spotify_tracks(distinct_songs)
    
    # Count successful Spotify matches
    spotify_matches = sum(1 for song in distinct_songs_with_spotify if song.get('trackId'))
    
    with open('distinct-hot-ones.json', 'w', encoding='utf-8') as f:
        json.dump({
            'total_unique_songs': len(distinct_songs_with_spotify),
            'total_chart_weeks': len(hot_ones),
            'spotify_matches': spotify_matches,
            'description': 'Distinct #1 songs from Billboard Hot 100 charts with Spotify track URLs',
            'data': distinct_songs_with_spotify
        }, f, indent=2, ensure_ascii=False)
    
    print(f"Saved {len(distinct_songs_with_spotify)} distinct songs to distinct-hot-ones.json")
    print(f"Found Spotify matches for {spotify_matches}/{len(distinct_songs_with_spotify)} songs ({spotify_matches/len(distinct_songs_with_spotify)*100:.1f}%)")
    
    # Print some statistics
    print(f"\nStatistics:")
    print(f"- Total chart weeks with #1 songs: {len(hot_ones)}")
    print(f"- Unique #1 songs: {len(distinct_songs_with_spotify)}")
    print(f"- Average weeks per unique song: {len(hot_ones) / len(distinct_songs_with_spotify):.2f}")
    print(f"- Spotify matches: {spotify_matches}/{len(distinct_songs_with_spotify)} ({spotify_matches/len(distinct_songs_with_spotify)*100:.1f}%)")


if __name__ == '__main__':
    main()