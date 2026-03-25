#!/usr/bin/env python3
"""Build artist, album and label lookup tables from track_lookup.parquet.

Loads the track lookup, aggregates to entity level using Artists/Albums/Labels
from src.entities (thresholds from T2M_*_MINTRACK env vars), and writes one
parquet per entity.

Usage:
    python scripts/build_entity_lookups.py [--track-lookup PATH]
        [--artist-output PATH] [--album-output PATH] [--label-output PATH]

Example:
    python scripts/build_entity_lookups.py
"""

import argparse
import os
import time
from pathlib import Path

import pandas as pd

from src.entities import Albums, Artists, Labels


def main():
    parser = argparse.ArgumentParser(
        description="Build entity lookup tables from track_lookup.parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--track-lookup",
        default=os.environ.get("T2M_LOOKUP_TRACK"),
        help="Path to track_lookup.parquet. Set to `T2M_LOOKUP_TRACK` by default.",
    )
    parser.add_argument(
        "--artist-output",
        default=os.environ.get("T2M_LOOKUP_ARTIST"),
        help="Output path for artist_lookup.parquet. Set to `T2M_LOOKUP_ARTIST` by default.",
    )
    parser.add_argument(
        "--album-output",
        default=os.environ.get("T2M_LOOKUP_ALBUM"),
        help="Output path for album_lookup.parquet. Set to `T2M_LOOKUP_ALBUM` by default.",
    )
    parser.add_argument(
        "--label-output",
        default=os.environ.get("T2M_LOOKUP_LABEL"),
        help="Output path for label_lookup.parquet. Set to `T2M_LOOKUP_LABEL` by default.",
    )
    args = parser.parse_args()

    if args.track_lookup is None:
        raise ValueError(
            "No `T2M_LOOKUP_TRACK` environment variable set. "
            "Either run with --track-lookup or define the environment variable."
        )
    track_lookup_path = Path(args.track_lookup)
    if not track_lookup_path.exists():
        raise FileNotFoundError(f"Track lookup not found: {track_lookup_path}")

    if args.artist_output is None:
        raise ValueError(
            "No `T2M_LOOKUP_ARTIST` environment variable set. "
            "Either run with --artist-output or define the environment variable."
        )
    if args.album_output is None:
        raise ValueError(
            "No `T2M_LOOKUP_ALBUM` environment variable set. "
            "Either run with --album-output or define the environment variable."
        )
    if args.label_output is None:
        raise ValueError(
            "No `T2M_LOOKUP_LABEL` environment variable set. "
            "Either run with --label-output or define the environment variable."
        )

    artist_path = Path(args.artist_output)
    album_path = Path(args.album_output)
    label_path = Path(args.label_output)

    for p in (artist_path, album_path, label_path):
        p.parent.mkdir(parents=True, exist_ok=True)

    print(f"Track lookup : {track_lookup_path}")
    print(f"Artist output: {artist_path}")
    print(f"Album output : {album_path}")
    print(f"Label output : {label_path}")
    print()

    t0 = time.time()
    print("Loading track lookup...")
    df = pd.read_parquet(track_lookup_path)
    print(f"  {len(df):,} rows  ({time.time() - t0:.1f}s)")
    print()

    for name, cls, output_path in [
        ("artists", Artists, artist_path),
        ("albums", Albums, album_path),
        ("labels", Labels, label_path),
    ]:
        t1 = time.time()
        result = cls.lookup(df)
        result.to_parquet(output_path, index=False)
        size_mb = output_path.stat().st_size / 1_048_576
        print(f"{name:<8}  {len(result):>8,} rows  →  {output_path}  ({size_mb:.1f} MB, {time.time() - t1:.1f}s)")

    print(f"\nDone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
