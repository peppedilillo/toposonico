#!/usr/bin/env python3
"""Merge spotify_clean and audio_features into a single database.

This creates spotify_merged.sqlite3 which contains all tables from
spotify_clean.sqlite3 plus an audio_features table with the audio
features data. Having everything in one database enables fast joins.

This is a one-time operation that takes ~30 minutes.
"""
import shutil
import sqlite3
import time
from pathlib import Path


DATA_DIR = Path.home() / "HDD/Datasets"
MERGED_PATH = DATA_DIR / "annas_archive_spotify_2025_07_merged/spotify_merged.sqlite3"
SPOTIFY_PATH = DATA_DIR / "annas_archive_spotify_2025_07/spotify_clean.sqlite3"
AUDIO_PATH = DATA_DIR / "annas_archive_spotify_2025_07/spotify_clean_audio_features.sqlite3"

def main():
    if MERGED_PATH.exists():
        print(f"Merged database already exists: {MERGED_PATH}")
        response = input("Delete and recreate? [y/N] ")
        if response.lower() != "y":
            print("Aborted.")
            return
        MERGED_PATH.unlink()

    # Step 1: Copy spotify_clean.sqlite3 (contains the 'tracks' table)
    print(f"Copying {SPOTIFY_PATH.name} to {MERGED_PATH.name}...")
    start = time.time()
    shutil.copy(SPOTIFY_PATH, MERGED_PATH)
    print(f"  Done in {time.time() - start:.1f}s")

    # Step 2: Open and attach audio database
    conn = sqlite3.connect(MERGED_PATH)
    conn.execute(f"ATTACH DATABASE '{AUDIO_PATH}' AS audio")

    # Step 3: Create audio_features table with track_rowid mapping
    # We join on tracks.id (string) to pull the internal tracks.rowid (integer)
    print("Creating audio_features table with rowid mapping...")
    start = time.time()
    conn.execute("""
        CREATE TABLE audio_features AS
        SELECT 
            t.rowid AS track_rowid,
            af.track_id, 
            af.time_signature, af.tempo, af.key, af.mode,
            af.danceability, af.energy, af.loudness, af.speechiness,
            af.acousticness, af.instrumentalness, af.liveness, af.valence
        FROM audio.track_audio_features af
        JOIN tracks t ON af.track_id = t.id
        WHERE af.null_response = 0
    """)
    conn.commit()
    print(f"  Done in {time.time() - start:.1f}s")

    # Step 4: Create indices for performance
    print("Creating indices...")
    start = time.time()
    # Unique index on the string ID (original behavior)
    conn.execute("CREATE UNIQUE INDEX idx_audio_features_track_id ON audio_features(track_id)")
    # New index on the integer rowid for faster joins with the local tracks table
    conn.execute("CREATE UNIQUE INDEX idx_audio_features_track_rowid ON audio_features(track_rowid)")
    # Improves performances of genre aggregation
    conn.execute("CREATE INDEX idx_artist_genres_covering ON artist_genres(artist_rowid, genre);")
    conn.commit()
    print(f"  Done in {time.time() - start:.1f}s")

    # Report
    cursor = conn.execute("SELECT COUNT(*) FROM audio_features")
    count = cursor.fetchone()[0]
    print(f"\nMerged database created: {MERGED_PATH}")
    print(f"Audio features rows: {count:,}")

    conn.close()

if __name__ == "__main__":
    main()