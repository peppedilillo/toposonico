"""Build the base training vocabulary from the playlist SQLite database.

Counts distinct playlist appearances per track (full table scan of ~1.7B rows),
filters by --min-count, then assigns contiguous track_ids (0..vocab_size-1)
sorted by track_rowid. Output parquet contains track_rowid, track_id, and
playlist_count only. Metadata enrichment happens in a second stage via
`build_vocab_t1.py`.
"""

import argparse
import os
from pathlib import Path
import sqlite3
import time

import numpy as np
import pandas as pd


MIN_COUNT = 5

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
        description="Build training vocabulary from playlist database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("SICK_PLAYLIST_DB"),
        help="Path to playlist SQLite database. $SICK_PLAYLIST_DB",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("SICK_T0_VOCAB"),
        help="Output parquet path. $SICK_T0_VOCAB",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=MIN_COUNT,
        help=f"Minimum playlist count to include a track (default: {MIN_COUNT}).",
    )
    args = parser.parse_args()

    if args.min_count < 1:
        raise ValueError("Argument --min-count must be >= 1.")

    if args.database is None:
        raise ValueError("--database / $SICK_PLAYLIST_DB not set.")
    db_path = Path(args.database)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}.")

    if args.output is None:
        raise ValueError("--output / $SICK_T0_VOCAB not set.")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database  : {db_path}")
    print(f"Output    : {output_path}")
    print(f"Min count : {args.min_count}")
    print()
    print("Counting distinct playlists per track...")
    print("(Full table scan of ~1.7B rows — this will take a while on HDD)")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    t0 = time.time()
    chunks = []
    for chunk in pd.read_sql_query(QUERY, conn, chunksize=500_000):
        chunks.append(chunk)
        n_so_far = sum(len(c) for c in chunks)
        print(f"  {n_so_far:,} tracks processed...", end="\r")
    conn.close()

    df = pd.concat(chunks, ignore_index=True)
    df["track_rowid"] = df["track_rowid"].astype("int64")
    df["playlist_count"] = df["playlist_count"].astype("int32")
    n_total = len(df)
    print(f"  {n_total:,} unique track_rowids found in {time.time() - t0:.1f}s")

    if args.min_count > 1:
        df = df[df["playlist_count"] >= args.min_count].reset_index(drop=True)
        n_dropped = n_total - len(df)
        print(f"  Dropped {n_dropped:,} tracks below min_count={args.min_count} ({100 * n_dropped / n_total:.1f}%)")

    df = df.sort_values("track_rowid").reset_index(drop=True)
    df["track_id"] = np.arange(len(df), dtype=np.int32)
    df = df[["track_rowid", "track_id", "playlist_count"]]

    vocab_size = len(df)
    counts = df["playlist_count"]
    print(f"\nVocab size: {vocab_size:,} tracks")
    print("\nPlaylist count distribution:")
    print(f"  min    : {counts.min():,}")
    print(f"  median : {counts.median():.0f}")
    print(f"  mean   : {counts.mean():.1f}")
    print(f"  p95    : {counts.quantile(0.95):.0f}")
    print(f"  p99    : {counts.quantile(0.99):.0f}")
    print(f"  max    : {counts.max():,}")

    print(f"\nWriting vocabulary to {output_path}...")
    df.to_parquet(output_path, index=False)
    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  File size: {size_mb:.1f} MB")

    print("\nMemory estimates for embedding table (float32):")
    for dim in [128, 256]:
        gb = vocab_size * dim * 4 * 2 / 1_073_741_824
        print(f"  {dim}-dim : {gb:.1f} GB  (both tables)")

    print(f"\nDone in {time.time() - t0:.1f}s total.")
    print(f"Vocab size: {vocab_size:,}  |  track_id range: 0 – {vocab_size - 1:,}")


if __name__ == "__main__":
    main()
