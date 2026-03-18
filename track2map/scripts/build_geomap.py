"""Compute joint lon/lat coordinates for all entity types from UMAP projections.

Reads UMAP parquets for any combination of tracks, albums, artists, and labels,
computes a single global bounding box across all of them (with padding), then
writes per-entity geo-parquets containing only the key column + lon + lat.

Running with a subset of entities shifts the bbox — always run with all 4 together
to keep coordinate alignment stable across entity types.

Usage:
    python scripts/build_geomap.py \\
        [--track-umap PATH] [--album-umap PATH] \\
        [--artist-umap PATH] [--label-umap PATH] \\
        [--output-dir DIR] [--extent DEG] [--padding FRAC]

Examples:
    python scripts/build_geomap.py \\
        --track-umap  outs/umap/umap_track_2d_pure_bolt_nn150_md0d01_cosine.parquet \\
        --album-umap  outs/umap/umap_album_2d_pure_bolt_nn150_md0d01_cosine.parquet \\
        --artist-umap outs/umap/umap_artist_2d_pure_bolt_nn150_md0d01_cosine.parquet \\
        --label-umap  outs/umap/umap_label_2d_pure_bolt_nn150_md0d01_cosine.parquet
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

from src.topo import umap2geo

ENTITIES = {
    "track": ("track_umap", "track_rowid"),
    "album": ("album_umap", "album_rowid"),
    "artist": ("artist_umap", "artist_rowid"),
    "label": ("label_umap", "label"),
}

EXTENT_DEFAULT = 45.0
PADDING_DEFAULT = 0.02


def main():
    parser = argparse.ArgumentParser(
        description="Joint-normalize UMAP coords to lon/lat for all entity types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--track-umap", type=Path, metavar="PATH")
    parser.add_argument("--album-umap", type=Path, metavar="PATH")
    parser.add_argument("--artist-umap", type=Path, metavar="PATH")
    parser.add_argument("--label-umap", type=Path, metavar="PATH")
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("T2M_GEO_DIR"),
        metavar="DIR",
        help="Directory for output geo-parquets (default: $T2M_GEO_DIR)",
    )
    parser.add_argument(
        "--extent",
        type=float,
        default=EXTENT_DEFAULT,
        metavar="DEG",
        help=f"Half-width in degrees of the lon/lat square (default: {EXTENT_DEFAULT})",
    )
    parser.add_argument(
        "--padding",
        type=float,
        default=PADDING_DEFAULT,
        metavar="FRAC",
        help=f"Fractional padding added to each side of the bbox (default: {PADDING_DEFAULT})",
    )
    args = parser.parse_args()

    if args.output_dir is None:
        raise ValueError(
            "No `T2M_GEO_DIR` environment variable set. "
            "Either run with --output-dir argument or define the environment variable."
        )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    active = {
        entity: getattr(args, attr)
        for entity, (attr, _) in ENTITIES.items()
        if getattr(args, attr) is not None
    }

    if not active:
        print("Error: at least one --*-umap argument is required", file=sys.stderr)
        sys.exit(1)

    for entity, path in active.items():
        if not path.exists():
            print(f"Error: {entity} UMAP parquet not found: {path}", file=sys.stderr)
            sys.exit(1)

    umap_frames = []
    entity_names = []
    for entity, path in active.items():
        key_col = ENTITIES[entity][1]
        df = pd.read_parquet(path, columns=[key_col, "umap_x", "umap_y"])
        umap_frames.append((df, key_col))
        entity_names.append(entity)

    geo_frames = umap2geo(
        umap_frames, max_lon=args.extent, max_lat=args.extent, padding=args.padding
    )

    for entity, geo in zip(entity_names, geo_frames):
        out_path = output_dir / f"{entity}_geo.parquet"
        geo.to_parquet(out_path, index=False)
        print(f"{entity:8s}  {len(geo):>9,} rows  →  {out_path}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
