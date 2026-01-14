#!/usr/bin/env python3
"""Extract training data for autoencoder from Spotify databases.

This script extracts tracks with popularity >= threshold and joins:
- Track metadata (tracks table)
- Album metadata (albums table)
- First artist per track (track_artists + artists tables)
- Artist genres concatenated with ";" (artist_genres table)
- Audio features (track_audio_features from separate database)

Output: data/popularity{POP}/tracks.parquet
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.db import DATABASES


def get_combined_connection() -> sqlite3.Connection:
    """Get a connection with both databases attached."""
    spotify_path = DATABASES["spotify"]
    audio_path = DATABASES["audio"]

    # Connect to spotify as main, attach audio
    conn = sqlite3.connect(f"file:{spotify_path}?mode=ro", uri=True)
    conn.execute(f"ATTACH DATABASE 'file:{audio_path}?mode=ro' AS audio")
    return conn


def fetch_tracks(conn, popularity: int) -> pd.DataFrame:
    """Fetch tracks with popularity >= threshold."""
    query = """
        SELECT rowid as track_rowid, id as track_id, album_rowid,
               external_id_isrc, popularity as track_popularity,
               track_number, disc_number, duration_ms, explicit
        FROM tracks
        WHERE popularity >= ?
    """
    return pd.read_sql_query(query, conn, params=(popularity,))


def fetch_first_artists(conn, track_rowids: list[int]) -> pd.DataFrame:
    """Fetch first artist for each track."""
    if not track_rowids:
        return pd.DataFrame(columns=["track_rowid", "artist_rowid"])
    placeholders = ",".join(["?"] * len(track_rowids))
    query = f"""
        SELECT track_rowid, MIN(artist_rowid) as artist_rowid
        FROM track_artists
        WHERE track_rowid IN ({placeholders})
        GROUP BY track_rowid
    """
    return pd.read_sql_query(query, conn, params=track_rowids)


def fetch_artists(conn, artist_rowids: list[int]) -> pd.DataFrame:
    """Fetch artist metadata."""
    if not artist_rowids:
        return pd.DataFrame(
            columns=["artist_rowid", "artist_name", "artist_popularity", "artist_followers"]
        )
    placeholders = ",".join(["?"] * len(artist_rowids))
    query = f"""
        SELECT rowid as artist_rowid, name as artist_name,
               popularity as artist_popularity, followers_total as artist_followers
        FROM artists
        WHERE rowid IN ({placeholders})
    """
    return pd.read_sql_query(query, conn, params=artist_rowids)


def fetch_genres(conn, artist_rowids: list[int]) -> pd.DataFrame:
    """Fetch genres for artists, concatenated with ';'."""
    if not artist_rowids:
        return pd.DataFrame(columns=["artist_rowid", "genres"])
    placeholders = ",".join(["?"] * len(artist_rowids))
    query = f"""
        SELECT artist_rowid, GROUP_CONCAT(genre, ';') as genres
        FROM artist_genres
        WHERE artist_rowid IN ({placeholders})
        GROUP BY artist_rowid
    """
    return pd.read_sql_query(query, conn, params=artist_rowids)


def fetch_albums(conn, album_rowids: list[int]) -> pd.DataFrame:
    """Fetch album metadata."""
    if not album_rowids:
        return pd.DataFrame(
            columns=[
                "album_rowid", "release_date", "release_date_precision",
                "album_popularity", "album_type", "external_id_upc", "label"
            ]
        )
    placeholders = ",".join(["?"] * len(album_rowids))
    query = f"""
        SELECT rowid as album_rowid, release_date, release_date_precision,
               popularity as album_popularity, album_type, external_id_upc, label
        FROM albums
        WHERE rowid IN ({placeholders})
    """
    return pd.read_sql_query(query, conn, params=album_rowids)


def fetch_audio_features_join(conn, popularity: int) -> pd.DataFrame:
    """Fetch audio features using JOIN with tracks table (much faster)."""
    query = """
        SELECT t.id as track_id, af.time_signature, af.tempo, af.key, af.mode,
               af.danceability, af.energy, af.loudness, af.speechiness,
               af.acousticness, af.instrumentalness, af.liveness, af.valence
        FROM tracks t
        JOIN audio.track_audio_features af ON t.id = af.track_id
        WHERE t.popularity >= ?
        AND af.null_response = 0
    """
    return pd.read_sql_query(query, conn, params=(popularity,))


def main():
    parser = argparse.ArgumentParser(
        description="Extract training data for autoencoder"
    )
    parser.add_argument(
        "-p",
        "--popularity",
        type=int,
        default=80,
        help="Minimum track popularity threshold (default: 80)",
    )
    args = parser.parse_args()

    total_start = time.time()
    conn = get_combined_connection()

    # 1. Fetch tracks
    print(f"Fetching tracks with popularity >= {args.popularity}...")
    start = time.time()
    tracks_df = fetch_tracks(conn, args.popularity)
    print(f"  {len(tracks_df):,} tracks in {time.time() - start:.1f}s")

    if tracks_df.empty:
        print("No tracks found.")
        return

    # 2. Fetch first artist per track
    print("Fetching first artist per track...")
    start = time.time()
    track_rowids = tracks_df["track_rowid"].tolist()
    first_artists_df = fetch_first_artists(conn, track_rowids)
    print(f"  {len(first_artists_df):,} artist mappings in {time.time() - start:.1f}s")

    # 3. Fetch artist metadata
    print("Fetching artist metadata...")
    start = time.time()
    artist_rowids = first_artists_df["artist_rowid"].dropna().astype(int).unique().tolist()
    artists_df = fetch_artists(conn, artist_rowids)
    print(f"  {len(artists_df):,} artists in {time.time() - start:.1f}s")

    # 4. Fetch genres
    print("Fetching genres...")
    start = time.time()
    genres_df = fetch_genres(conn, artist_rowids)
    print(f"  {len(genres_df):,} genre mappings in {time.time() - start:.1f}s")

    # 5. Fetch albums
    print("Fetching albums...")
    start = time.time()
    album_rowids = tracks_df["album_rowid"].unique().tolist()
    albums_df = fetch_albums(conn, album_rowids)
    print(f"  {len(albums_df):,} albums in {time.time() - start:.1f}s")

    # 6. Fetch audio features (using JOIN - much faster than IN clause)
    print("Fetching audio features...")
    start = time.time()
    audio_df = fetch_audio_features_join(conn, args.popularity)
    print(f"  {len(audio_df):,} audio features in {time.time() - start:.1f}s")

    conn.close()

    # Merge all dataframes
    print("Merging data...")
    start = time.time()

    # tracks + first_artists
    df = tracks_df.merge(first_artists_df, on="track_rowid", how="left")

    # + artists
    df = df.merge(artists_df, on="artist_rowid", how="left")

    # + genres
    df = df.merge(genres_df, on="artist_rowid", how="left")
    df["genres"] = df["genres"].fillna("")

    # + albums
    df = df.merge(albums_df, on="album_rowid", how="left")

    # + audio features
    df = df.merge(audio_df, on="track_id", how="inner")

    print(f"  Merged in {time.time() - start:.1f}s")

    # Write output
    output_dir = Path(__file__).parent.parent / "data" / f"popularity{args.popularity}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "tracks.parquet"

    print(f"Writing to {output_path}...")
    df.to_parquet(output_path, index=False)

    total_elapsed = time.time() - total_start
    print(f"\nDone in {total_elapsed:.1f}s total.")
    print(f"Output: {output_path}")
    print(f"Shape: {df.shape}")


if __name__ == "__main__":
    main()
