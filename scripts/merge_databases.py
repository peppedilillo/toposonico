#!/usr/bin/env python3
"""Merge spotify_clean and audio_features into a single database.

Usage:
    python scripts/merge_databases.py --spotify <spotify_db> --audio <audio_db> <output>

Example:
    python scripts/merge_databases.py \\
        --spotify ~/data/spotify_clean.sqlite3 \\
        --audio ~/data/spotify_clean_audio_features.sqlite3 \\
        ~/data/spotify_merged.sqlite3

This creates a merged database containing all tables from spotify_clean
plus an audio_features table. Having everything in one database enables fast joins.
Takes ~30 minutes for large datasets.
"""

import argparse
import shutil
import sqlite3
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Merge spotify_clean and audio_features databases"
    )
    parser.add_argument(
        "--spotify", required=True, type=Path, help="Path to spotify_clean.sqlite3"
    )
    parser.add_argument(
        "--audio", required=True, type=Path, help="Path to audio_features.sqlite3"
    )
    parser.add_argument("output", type=Path, help="Path for merged output database")
    args = parser.parse_args()

    if not args.spotify.exists():
        raise FileNotFoundError(f"Spotify database not found: {args.spotify}")
    if not args.audio.exists():
        raise FileNotFoundError(f"Audio database not found: {args.audio}")

    if args.output.exists():
        print(f"Merged database already exists: {args.output}")
        response = input("Delete and recreate? [y/N] ")
        if response.lower() != "y":
            print("Aborted.")
            return
        args.output.unlink()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(f"Copying {args.spotify.name} to {args.output.name}...")
    start = time.time()
    shutil.copy(args.spotify, args.output)
    print(f"  Done in {time.time() - start:.1f}s")

    conn = sqlite3.connect(args.output)
    conn.execute(f"ATTACH DATABASE '{args.audio}' AS audio")

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

    print("Creating indices...")
    start = time.time()
    conn.execute(
        "CREATE UNIQUE INDEX idx_audio_features_track_id ON audio_features(track_id)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX idx_audio_features_track_rowid ON audio_features(track_rowid)"
    )
    conn.execute(
        "CREATE INDEX idx_artist_genres_covering ON artist_genres(artist_rowid, genre);"
    )
    conn.commit()
    print(f"  Done in {time.time() - start:.1f}s")

    cursor = conn.execute("SELECT COUNT(*) FROM audio_features")
    count = cursor.fetchone()[0]
    print(f"\nMerged database created: {args.output}")
    print(f"Audio features rows: {count:,}")

    conn.close()


if __name__ == "__main__":
    main()
