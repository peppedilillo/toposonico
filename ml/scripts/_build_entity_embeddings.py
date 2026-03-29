#!/usr/bin/env python3
"""Mean-pool track embeddings to artist, album and label level.

Loads the track embedding parquet and the track lookup, aggregates to entity
level using Artists/Albums/Labels from src.entities (thresholds from
SICK_*_MINTRACK env vars), and writes one parquet per entity.

Output format matches track embeddings: {entity_key} (int64 or str) + e0…e{D-1}
(float32), one row per entity.

Usage:
    python scripts/build_entity_embeddings.py [--embedding PATH]
        [--track-lookup PATH] [--artist-output PATH] [--album-output PATH]
        [--label-output PATH]

Example:
    python scripts/build_entity_embeddings.py
"""

import argparse
import os
from pathlib import Path
import time

import pandas as pd

from src.entities import Albums
from src.entities import Artists
from src.entities import Labels


def main():
    parser = argparse.ArgumentParser(
        description="Mean-pool track embeddings to entity level",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--embedding",
        default=os.environ.get("SICK_EMBEDDING_TRACK"),
        help="Path to track embedding parquet (track_rowid + e0..e{D-1}). Set to `SICK_EMBEDDING_TRACK` by default.",
    )
    parser.add_argument(
        "--lookup-track",
        default=os.environ.get("SICK_LOOKUP_TRACK"),
        help="Path to track_lookup.parquet. Set to `SICK_LOOKUP_TRACK` by default.",
    )
    parser.add_argument(
        "--artist-output",
        default=os.environ.get("SICK_EMBEDDING_ARTIST"),
        help="Output path for artist embeddings. Set to `SICK_EMBEDDING_ARTIST` by default.",
    )
    parser.add_argument(
        "--album-output",
        default=os.environ.get("SICK_EMBEDDING_ALBUM"),
        help="Output path for album embeddings. Set to `SICK_EMBEDDING_ALBUM` by default.",
    )
    parser.add_argument(
        "--label-output",
        default=os.environ.get("SICK_EMBEDDING_LABEL"),
        help="Output path for label embeddings. Set to `SICK_EMBEDDING_LABEL` by default.",
    )
    args = parser.parse_args()

    if args.embedding is None:
        raise ValueError(
            "No `SICK_EMBEDDING_TRACK` environment variable set. "
            "Either run with --embedding or define the environment variable."
        )
    if args.lookup_track is None:
        raise ValueError(
            "No `SICK_LOOKUP_TRACK` environment variable set. "
            "Either run with --lookup-track or define the environment variable."
        )
    if args.artist_output is None:
        raise ValueError(
            "No `SICK_EMBEDDING_ARTIST` environment variable set. "
            "Either run with --artist-output or define the environment variable."
        )
    if args.album_output is None:
        raise ValueError(
            "No `SICK_EMBEDDING_ALBUM` environment variable set. "
            "Either run with --album-output or define the environment variable."
        )
    if args.label_output is None:
        raise ValueError(
            "No `SICK_EMBEDDING_LABEL` environment variable set. "
            "Either run with --label-output or define the environment variable."
        )

    embedding_path = Path(args.embedding)
    if not embedding_path.exists():
        raise FileNotFoundError(f"Track embedding not found: {embedding_path}")
    lookup_track_path = Path(args.lookup_track)
    if not lookup_track_path.exists():
        raise FileNotFoundError(f"Track lookup not found: {lookup_track_path}")

    artist_path = Path(args.artist_output)
    album_path = Path(args.album_output)
    label_path = Path(args.label_output)

    for p in (artist_path, album_path, label_path):
        p.parent.mkdir(parents=True, exist_ok=True)

    print(f"Embedding    : {embedding_path}")
    print(f"Track lookup : {lookup_track_path}")
    print(f"Artist output: {artist_path}")
    print(f"Album output : {album_path}")
    print(f"Label output : {label_path}")
    print()

    t0 = time.time()
    print("Loading track embeddings...")
    emb_df = pd.read_parquet(embedding_path)
    ndim = emb_df.filter(like="e").shape[1]
    print(f"  {len(emb_df):,} tracks, {ndim}d  ({time.time() - t0:.1f}s)")

    print("Loading track lookup...")
    t1 = time.time()
    lookup_df = pd.read_parquet(
        lookup_track_path,
        columns=["track_rowid", "artist_rowid", "album_rowid", "label"],
    )
    print(f"  {len(lookup_df):,} rows  ({time.time() - t1:.1f}s)")
    print()

    for name, cls, output_path in [
        ("artists", Artists, artist_path),
        ("albums",  Albums,  album_path),
        ("labels",  Labels,  label_path),
    ]:
        t1 = time.time()
        result = cls.embeddings(emb_df, lookup_df)
        result.to_parquet(output_path, index=False)
        size_mb = output_path.stat().st_size / 1_048_576
        print(
            f"{name:<8}  {len(result):>8,} rows  {ndim}d  →  {output_path}  ({size_mb:.1f} MB, {time.time() - t1:.1f}s)"
        )

    print(f"\nDone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
