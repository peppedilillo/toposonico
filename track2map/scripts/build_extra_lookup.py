#!/usr/bin/env python3
"""Build entity-level lookup tables from track_lookup.parquet.

Produces three small aggregated parquets — artist, album, and label — from a
single pass over track_lookup.parquet. Useful for downstream tile generation
and UI panels that need entity metadata without scanning the full track table.

Usage:
    python scripts/build_extra_lookup.py [--lookup PATH] [--output-dir DIR]

Example:
    source config.env && python scripts/build_extra_lookup.py
"""

import argparse
import os
from pathlib import Path

import pandas as pd

from src.lookup import album_lookup, artist_lookup, label_lookup


def main():
    parser = argparse.ArgumentParser(
        description="Build artist/album/label lookup tables from track_lookup.parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--lookup",
        default=os.environ.get("T2M_TRACK_LOOKUP"),
        help="Path to track_lookup.parquet. Defaults to T2M_TRACK_LOOKUP env var.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("T2M_LOOKUP_DIR"),
        help="Directory for output parquets. Defaults to the parent dir of --lookup.",
    )
    args = parser.parse_args()

    if args.lookup is None:
        raise ValueError(
            "No `T2M_TRACK_LOOKUP` environment variable set. "
            "Either run with --lookup argument or define the environment variable."
        )
    lookup_path = Path(args.lookup)
    if not lookup_path.exists():
        raise FileNotFoundError(f"Lookup parquet not found: {lookup_path}")

    if args.output_dir is None:
        raise ValueError(
            "No `T2M_LOOKUP_DIR` environment variable set. "
            "Either run with --output_dir argument or define the environment variable."
        )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Lookup    : {lookup_path}")
    print(f"Output dir: {output_dir}")
    print()

    cols = ["track_rowid", "artist_rowid", "artist_name", "album_rowid", "album_name",
            "label", "logcounts"]
    print("Loading track_lookup.parquet...")
    df = pd.read_parquet(lookup_path, columns=cols)
    print(f"  {len(df):,} rows loaded")
    print()

    artist = artist_lookup(df)
    out = output_dir / "artist_lookup.parquet"
    artist.to_parquet(out, index=False)
    print(f"artist_lookup.parquet: {len(artist):,} artists → {out}")

    album = album_lookup(df)
    out = output_dir / "album_lookup.parquet"
    album.to_parquet(out, index=False)
    print(f"album_lookup.parquet : {len(album):,} albums  → {out}")

    label = label_lookup(df)
    out = output_dir / "label_lookup.parquet"
    label.to_parquet(out, index=False)
    print(f"label_lookup.parquet : {len(label):,} labels  → {out}")


if __name__ == "__main__":
    main()
