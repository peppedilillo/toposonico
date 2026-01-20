#!/usr/bin/env python3
"""Extract training data for autoencoder from merged Spotify database.

Usage:
    python scripts/extract_training_data.py <merged_db> [-p POP] [-g]

Example:
    python scripts/extract_training_data.py ~/data/spotify_merged.sqlite3 -p 50 -g

Requires: Run scripts/merge_databases.py first to create the merged database.
Output: data/raw/training_pop{POP}[_genres].parquet
"""

import argparse
import sqlite3
import time
from pathlib import Path

import pandas as pd


OUTPUT_DIR = Path(__file__).parent.parent / "data/raw"


QUERY = """
SELECT
    t.rowid as track_rowid, t.name as track_name,
    a.name as artist_name, a.rowid as artist_rowid,
    t.album_rowid, al.name as album_name, al.album_type, al.label,
    al.release_date, al.release_date_precision,
    t.external_id_isrc as id_isrc, al.external_id_upc as id_upc,
    af.time_signature, af.tempo, af.key, af.mode,
    af.danceability, af.energy, af.loudness, af.speechiness,
    af.acousticness, af.instrumentalness, af.liveness, af.valence,
    t.explicit, t.popularity as track_popularity,
    a.popularity as artist_popularity, a.followers_total as artist_followers,
    al.popularity as album_popularity, t.duration_ms, t.track_number,
    t.disc_number, al.total_tracks
FROM tracks AS t
INNER JOIN audio_features AS af ON t.rowid = af.track_rowid
INNER JOIN albums AS al ON t.album_rowid = al.rowid
INNER JOIN track_artists AS ta ON t.rowid = ta.track_rowid
INNER JOIN artists AS a ON ta.artist_rowid = a.rowid
WHERE t.popularity > ?
  AND ta.artist_rowid = (
      SELECT artist_rowid
      FROM track_artists
      WHERE track_rowid = t.rowid
      LIMIT 1 -- for the moment we limit to the track's first artist
  );
"""


QUERY_GENRES = """
WITH GenreAgg AS (
    SELECT
        artist_rowid,
        GROUP_CONCAT(genre, ' | ') AS genre_list
    FROM artist_genres
    GROUP BY artist_rowid
)  -- concatenate genres per artist into a new table
SELECT
    t.rowid as track_rowid, t.name as track_name,
    a.name as artist_name, a.rowid as artist_rowid,
    t.album_rowid, al.name as album_name, al.album_type, al.label,
    al.release_date, al.release_date_precision,
    t.external_id_isrc as id_isrc, al.external_id_upc as id_upc,
    af.time_signature, af.tempo, af.key, af.mode,
    af.danceability, af.energy, af.loudness, af.speechiness,
    af.acousticness, af.instrumentalness, af.liveness, af.valence,
    t.explicit, t.popularity as track_popularity,
    a.popularity as artist_popularity, a.followers_total as artist_followers,
    al.popularity as album_popularity, t.duration_ms, t.track_number,
    t.disc_number, al.total_tracks, ga.genre_list as artist_genres
FROM tracks AS t
INNER JOIN audio_features AS af ON t.rowid = af.track_rowid
INNER JOIN albums AS al ON t.album_rowid = al.rowid
INNER JOIN track_artists AS ta ON t.rowid = ta.track_rowid
INNER JOIN artists AS a ON ta.artist_rowid = a.rowid
LEFT JOIN GenreAgg AS ga ON a.rowid = ga.artist_rowid
WHERE t.popularity > ?
  AND ta.artist_rowid = (
      SELECT artist_rowid
      FROM track_artists
      WHERE track_rowid = t.rowid
      LIMIT 1 -- for the moment we limit to the track's first artist
  );
"""


def main():
    parser = argparse.ArgumentParser(
        description="Extract training data for autoencoder"
    )
    parser.add_argument("database", type=Path, help="Path to merged database")
    parser.add_argument(
        "-p", "--popularity",
        type=int,
        default=80,
        help="Minimum track popularity threshold (default: 80)",
    )
    parser.add_argument(
        "-g", "--genres",
        action="store_true",
        default=False,
        help="Include artist genres",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(
            f"Database not found: {args.database}\n"
            "Run 'python scripts/merge_databases.py' first."
        )

    start = time.time()
    conn = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True)

    print(f"Extracting tracks with popularity > {args.popularity}...")
    df = pd.read_sql_query(
        QUERY_GENRES if args.genres else QUERY, conn, params=[args.popularity]
    )
    conn.close()

    print(f"  {len(df):,} tracks in {time.time() - start:.1f}s")

    if df.empty:
        print("No tracks found.")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filename = (
        f"training_pop{args.popularity}_genres.parquet"
        if args.genres else f"training_pop{args.popularity}.parquet"
    )
    output_path = OUTPUT_DIR / filename

    print(f"Writing to {output_path}...")
    df.to_parquet(output_path, index=False)

    print(f"\nDone in {time.time() - start:.1f}s total.")
    print(f"Output: {output_path}")
    print(f"Shape: {df.shape}")


if __name__ == "__main__":
    main()
