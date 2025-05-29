import json

def load_artist_songs():
    results = {}
    with open('distinct-hot-ones.json', 'r', encoding='utf-8') as hot_ones:
        data = json.load(hot_ones)
        sorted_data = sorted(data['data'], key=lambda x: (x['artist'], x['song']))
        counter = 1
        for item in sorted_data:
            idx = counter
            artist = item['artist']
            song = item['song']
            trackId = item['trackId']
            key = artist + ' - ' + song
            value = {
                'idx': idx,
                'artist': artist,
                'song': song,
                'trackId': trackId,
            }
            results[key] = value
            counter += 1
    return results

def load_hot_ones(artist_songs):
    results = []
    with open('hot-ones.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        counter = 1
        for item in data['data']:
            date = item['date']
            artist = item['artist']
            song = item['song']
            key = artist + ' - ' + song
            if key in artist_songs:
                value = {
                    'id': counter,
                    'date': item['date'],
                    'songId': artist_songs[key]['idx']
                }
                counter += 1
                results.append(value)
    return results

def save_to_db(artist_songs, hot_ones):
    import sqlite3
    conn = sqlite3.connect('hot-ones.sqlite3')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS artist_songs (
            id INTEGER PRIMARY KEY,
            artist TEXT,
            song TEXT,
            trackId TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hot_ones (
            id INTEGER PRIMARY KEY,
            date TEXT,
            songId INTEGER,
            FOREIGN KEY (songId) REFERENCES artist_songs(idx)
        )
    ''')

    # Insert artist songs
    for key, value in artist_songs.items():
        cursor.execute('''
            INSERT OR IGNORE INTO artist_songs (id, artist, song, trackId)
            VALUES (?, ?, ?, ?)
        ''', (value['idx'], value['artist'], value['song'], value['trackId']))

    # Insert hot ones
    for item in hot_ones:
        cursor.execute('''
            INSERT INTO hot_ones (id, date, songId)
            VALUES (?, ?, ?)
        ''', (item['id'], item['date'], item['songId']))

    conn.commit()
    conn.close()

# Example usage
if __name__ == "__main__":
    artist_songs = load_artist_songs()
    hot_ones = load_hot_ones(artist_songs)
    save_to_db(artist_songs, hot_ones)
    pass