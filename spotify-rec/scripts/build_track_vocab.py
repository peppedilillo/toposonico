#!/usr/bin/env python3
"""Build a global track vocabulary from the playlist SQLite database.

Scans all valid track_rowids in playlist_tracks (filtering out episodes, local
files, and null rowids) and assigns a stable sequential integer index to each.
The result is a two-column parquet file used as the embedding table vocabulary
for playlist2vec training.

This is a one-time, slow operation (full scan of ~1.7B rows on disk). Run it
once and cache the result. Subsequent training runs load the parquet directly.

Usage:
    python scripts/build_track_vocab.py <playlist_db> [-o OUTPUT]

Example:
    python scripts/build_track_vocab.py \\
        ~/HDD/Datasets/annas_archive_spotify_2025_07/spotify_clean_playlists.sqlite3
"""

import argparse
import time
from pathlib import Path

import pandas as pd

from src.db import get_connection


OUTPUT_DIR = Path(__file__).parent.parent / "data" / "playlist"

QUERY = """
    SELECT DISTINCT track_rowid
    FROM playlist_tracks
    WHERE is_episode = 0
      AND is_local = 0
      AND track_rowid IS NOT NULL
"""


def main():
    parser = argparse.ArgumentParser(
        description="Build global track vocabulary from playlist database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("database", type=Path, help="Path to playlist SQLite database")
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output parquet path (default: data/playlist/global_track_vocab.parquet)",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Database not found: {args.database}")

    output_path = args.output or OUTPUT_DIR / "global_track_vocab.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database : {args.database}")
    print(f"Output   : {output_path}")
    print()
    print("Scanning playlist_tracks for distinct track_rowids...")
    print("(Full table scan of ~1.7B rows — this will take a while on HDD.)")

    conn = get_connection(args.database)
    t0 = time.time()
    df = pd.read_sql_query(QUERY, conn)
    conn.close()
    elapsed = time.time() - t0

    n = len(df)
    print(f"  {n:,} unique track_rowids found in {elapsed:.1f}s")

    # Assign stable sequential indices sorted by track_rowid.
    # Sorting ensures the mapping is reproducible and allows binary search later.
    df = df.sort_values("track_rowid").reset_index(drop=True)
    df.index.name = "index"
    df = df.reset_index()[["track_rowid", "index"]]
    df["track_rowid"] = df["track_rowid"].astype("int64")
    df["index"] = df["index"].astype("int32")

    print(f"\nWriting vocabulary to {output_path}...")
    df.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  File size: {size_mb:.1f} MB")

    print("\nMemory estimates for embedding table (float32):")
    for dim in [128, 256]:
        mb = n * dim * 4 / 1_048_576
        gb = mb / 1024
        print(f"  {dim}-dim : {mb:,.0f} MB  ({gb:.2f} GB)")

    print(f"\nDone in {time.time() - t0:.1f}s total.")
    print(f"Vocab size: {n:,} tracks  |  index range: 0 – {n - 1:,}")


if __name__ == "__main__":
    main()
