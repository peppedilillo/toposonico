#!/usr/bin/env python3
"""Build a training vocabulary from pre-computed track playlist counts.

Filters by --min-count, then assigns contiguous track_ids (0..vocab_size-1)
sorted by track_rowid. Output parquet contains track_rowid, track_id, and
playlist_count — everything the training loop needs for OOV filtering,
subsampling, and negative-sampling weights.

Usage:
    python scripts/build_training_vocab.py [--counts COUNTS] [--output OUTPUT] [--min-count N]

Example:
    python scripts/build_training_vocab.py --counts track_playlist_counts.parquet --output training_vocab.parquet --min-count 5
"""

import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Build training vocabulary from track playlist counts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--counts",
        default=os.environ.get("T2M_TRACK_COUNT"),
        help="Path to input track_playlist_counts.parquet",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("T2M_TRAINING_VOCAB"),
        help="Output parquet path",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum playlist count to include a track (default: 1, i.e. no filtering)",
    )
    args = parser.parse_args()

    if args.min_count < 1:
        raise ValueError("Argument --min_count must be >= 1.")

    if args.counts is None:
        raise ValueError(
            "No `T2M_TRACK_COUNT` environment variable set. "
            "Either run with --counts argument or define the environment variable."
        )
    counts_path = Path(args.counts)
    if not counts_path.exists():
        raise FileNotFoundError(f"Counts parquet not found: {counts_path}")

    if args.output is None:
        raise ValueError(
            "No `T2M_TRAINING_VOCAB` environment variable set. "
            "Either run with --output argument or define the environment variable."
        )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Counts   : {counts_path}")
    print(f"Output   : {output_path}")
    print(f"Min count: {args.min_count}")
    print()

    df = pd.read_parquet(counts_path)
    n_total = len(df)
    print(f"Loaded {n_total:,} tracks from counts parquet")

    if args.min_count > 1:
        df = df[df["playlist_count"] >= args.min_count].reset_index(drop=True)
        n_dropped = n_total - len(df)
        print(
            f"Dropped  {n_dropped:,} tracks below min_count={args.min_count} ({100 * n_dropped / n_total:.1f}%)"
        )

    df = df.sort_values("track_rowid").reset_index(drop=True)
    df["track_id"] = np.arange(len(df), dtype=np.int32)
    df = df[["track_rowid", "track_id", "playlist_count"]]

    vocab_size = len(df)
    print(f"Vocab size: {vocab_size:,} tracks")

    counts = df["playlist_count"]
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
        gb = vocab_size * dim * 4 * 2 / 1_073_741_824  # × 2 for both embedding tables
        print(f"  {dim}-dim : {gb:.1f} GB  (both tables)")

    print(
        f"\nDone.  Vocab size: {vocab_size:,}  |  track_id range: 0 – {vocab_size - 1:,}"
    )


if __name__ == "__main__":
    main()
