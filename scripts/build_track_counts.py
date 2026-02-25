#!/usr/bin/env python3
"""Build a per-track playlist count table from the playlist SQLite database.

For each track_rowid, counts the number of distinct playlists it appears in
(applying the same filters and DISTINCT semantics used in training). The result
is a two-column parquet file used for offline k-core filtering and frequency
subsampling in full-dataset training.

This is a one-time, slow operation (full scan of ~1.7B rows on disk). Run it
once and cache the result. At training time load the parquet and derive:

    freq[i]      = playlist_count[i] / playlist_count.sum()   # for subsampling
    keep[i]      = playlist_count[i] >= k                     # for k-core

Usage:
    python scripts/build_track_counts.py <playlist_db> [-o OUTPUT]

Example:
    python scripts/build_track_counts.py \\
        ~/HDD/Datasets/annas_archive_spotify_2025_07/spotify_clean_playlists.sqlite3
"""

import argparse
import time
from pathlib import Path

import pandas as pd

from src.db import get_connection


OUTPUT_DIR = Path(__file__).parent.parent / "data" / "playlist"

QUERY = """
    SELECT track_rowid, COUNT(*) AS playlist_count
    FROM (
        SELECT DISTINCT playlist_rowid, track_rowid
        FROM playlist_tracks
        WHERE is_episode = 0
          AND is_local = 0
          AND track_rowid IS NOT NULL
    )
    GROUP BY track_rowid
    ORDER BY track_rowid
"""


def main():
    parser = argparse.ArgumentParser(
        description="Build per-track playlist count table from playlist database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("database", type=Path, help="Path to playlist SQLite database")
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output parquet path (default: data/playlist/track_playlist_counts.parquet)",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Database not found: {args.database}")

    output_path = args.output or OUTPUT_DIR / "track_playlist_counts.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database : {args.database}")
    print(f"Output   : {output_path}")
    print()
    print("Counting distinct playlists per track...")
    print("(Full table scan of ~1.7B rows — this will take a while on HDD.)")

    conn = get_connection(args.database)
    t0 = time.time()
    chunks = []
    for chunk in pd.read_sql_query(QUERY, conn, chunksize=500_000):
        chunks.append(chunk)
        n_so_far = sum(len(c) for c in chunks)
        print(f"  {n_so_far:,} tracks processed...", end="\r")
    conn.close()
    elapsed = time.time() - t0

    df = pd.concat(chunks, ignore_index=True)
    df["track_rowid"] = df["track_rowid"].astype("int64")
    df["playlist_count"] = df["playlist_count"].astype("int32")

    n = len(df)
    print(f"  {n:,} unique track_rowids found in {elapsed:.1f}s")

    print(f"\nWriting counts to {output_path}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  File size: {size_mb:.1f} MB")

    counts = df["playlist_count"]
    print("\nPlaylist count distribution:")
    print(f"  min    : {counts.min():,}")
    print(f"  median : {counts.median():.0f}")
    print(f"  mean   : {counts.mean():.1f}")
    print(f"  p95    : {counts.quantile(0.95):.0f}")
    print(f"  p99    : {counts.quantile(0.99):.0f}")
    print(f"  max    : {counts.max():,}")

    print(f"\nDone in {time.time() - t0:.1f}s total.")
    print(f"Tracks: {n:,}  |  total playlist-track pairs: {counts.sum():,}")


if __name__ == "__main__":
    main()
