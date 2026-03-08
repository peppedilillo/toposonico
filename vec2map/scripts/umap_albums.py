"""Compute album centroids in UMAP space as simple track mean positions.

Joins a track UMAP projection with the track lookup table and computes
per-album arithmetic mean of all coord columns (umap_x, umap_y, [umap_z]).

Usage:
    python umap_albums.py <umap> <lookup> <output> [--min-tracks N]

Examples:
    # output is a directory — produces album_umap_2d_pure_bolt_nn100_md0d01_cosine.parquet
    python umap_albums.py \
        vec2map/outs/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \
        data/playlist/track_lookup.parquet \
        vec2map/outs/

    # explicit output file
    python umap_albums.py \
        vec2map/outs/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \
        data/playlist/track_lookup.parquet \
        vec2map/outs/album_centroids.parquet \
        --min-tracks 5
"""

import argparse
from pathlib import Path
import time

import numpy as np
import pandas as pd


MIN_TRACKS_DEFAULT = 6


def main():
    parser = argparse.ArgumentParser(
        description="Compute album centroids in UMAP space",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("umap", type=Path, help="Track UMAP parquet (track_rowid, umap_x, umap_y[, umap_z])")
    parser.add_argument("lookup", type=Path, help="Track lookup parquet")
    parser.add_argument("output", type=Path, help="Output parquet path or directory")
    parser.add_argument(
        "--min-tracks",
        type=int,
        default=MIN_TRACKS_DEFAULT,
        help=f"Minimum tracks per album (default: {MIN_TRACKS_DEFAULT})",
    )
    args = parser.parse_args()

    if not args.umap.exists():
        raise FileNotFoundError(f"UMAP parquet not found: {args.umap}")
    if not args.lookup.exists():
        raise FileNotFoundError(f"Lookup parquet not found: {args.lookup}")

    if args.output.is_dir():
        out_path = args.output / ("album_" + args.umap.name)
    else:
        out_path = args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"UMAP       : {args.umap}")
    print(f"Lookup     : {args.lookup}")
    print(f"Min tracks : {args.min_tracks}")
    print(f"Output     : {out_path}")
    print()

    t0 = time.time()

    print("Loading inputs...", end="\r")
    umap_df = pd.read_parquet(args.umap)
    lookup_df = pd.read_parquet(args.lookup, columns=["track_rowid", "album_rowid"])
    print(f"Loaded: {len(umap_df):,} UMAP rows, {len(lookup_df):,} lookup rows  ({time.time() - t0:.1f}s)")

    coord_cols = [c for c in umap_df.columns if c != "track_rowid"]

    df = umap_df.merge(lookup_df, on="track_rowid", how="inner")
    df = df.dropna(subset=["album_rowid"])
    print(f"Joined: {len(df):,} tracks with album_rowid")

    print("Computing album centroids...", end="\r")
    agg = df.groupby("album_rowid", sort=False).agg(
        **{c: (c, "mean") for c in coord_cols},
        track_count=("track_rowid", "count"),
    )

    before = len(agg)
    agg = agg[agg["track_count"] >= args.min_tracks]
    dropped = before - len(agg)

    result = agg[coord_cols + ["track_count"]].reset_index()
    result["album_rowid"] = result["album_rowid"].astype(np.int64)
    for c in coord_cols:
        result[c] = result[c].astype(np.float32)
    result["track_count"] = result["track_count"].astype(np.int32)

    result.to_parquet(out_path, index=False)

    elapsed = time.time() - t0
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"Done in {elapsed:.1f}s")
    print(f"Output : {out_path}  ({size_mb:.1f} MB, {len(result):,} albums)")
    print(f"Dropped: {dropped:,} albums below --min-tracks {args.min_tracks}")


if __name__ == "__main__":
    main()
