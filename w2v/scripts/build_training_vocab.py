#!/usr/bin/env python3
"""Build a training vocabulary from pre-computed track playlist counts.

Filters tracks by a minimum playlist-count threshold, then assigns contiguous
integer IDs (0..vocab_size-1) sorted by track_rowid for reproducibility.

The output parquet is self-contained — it holds everything the training loop needs:
  - track_rowid → track_id mapping  (for remapping playlist chunk data)
  - playlist_count                   (for subsampling and negative-sampling weights)

At training time derive what you need in two vectorised lines:
    freq      = df["playlist_count"] / df["playlist_count"].sum()
    keep_prob = np.minimum(1.0, np.sqrt(t / freq))          # subsampling
    weights   = counts ** 0.75 / (counts ** 0.75).sum()     # negative sampling

Usage:
    python scripts/build_training_vocab.py <counts> [--min-count N] [-o OUTPUT]

Example:
    python scripts/build_training_vocab.py data/playlist/track_playlist_counts.parquet --min-count 5
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

OUTPUT_DIR = Path(__file__).parent.parent / "data"


def main():
    parser = argparse.ArgumentParser(
        description="Build training vocabulary from track playlist counts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "counts",
        type=Path,
        help="Path to input track_playlist_counts.parquet",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum playlist count to include a track (default: 1, i.e. no filtering)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output parquet path (default: data/playlist/training_vocab.parquet)",
    )
    args = parser.parse_args()

    if not args.counts.exists():
        raise FileNotFoundError(f"Counts parquet not found: {args.counts}")

    output_path = args.output or OUTPUT_DIR / "training_vocab.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Counts   : {args.counts}")
    print(f"Output   : {output_path}")
    print(f"Min count: {args.min_count}")
    print()

    df = pd.read_parquet(args.counts)
    n_total = len(df)
    print(f"Loaded {n_total:,} tracks from counts parquet")

    if args.min_count > 1:
        df = df[df["playlist_count"] >= args.min_count].reset_index(drop=True)
        n_dropped = n_total - len(df)
        print(f"Dropped  {n_dropped:,} tracks below min_count={args.min_count} ({100 * n_dropped / n_total:.1f}%)")

    # Sort by track_rowid for a stable, reproducible mapping.
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

    print(f"\nDone.  Vocab size: {vocab_size:,}  |  track_id range: 0 – {vocab_size - 1:,}")


if __name__ == "__main__":
    main()
