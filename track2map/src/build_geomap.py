"""Compute joint lon/lat coordinates for all entity types from UMAP projections.

Reads UMAP parquets for any combination of tracks, albums, artists, and labels,
computes a single global bounding box across all of them (with padding), then
writes per-entity geo-parquets containing only the key column + lon + lat.

Running with a subset of entities shifts the bbox — always run with all 4 together
to keep coordinate alignment stable across entity types.

All four parquets must come from the same UMAP fit — mixing parquets from different
fits produces incompatible coordinates.

Usage:
    uv run python scripts/build_geomap.py [options]

Examples:
    # all four entities via env vars (recommended)
    source config.env && uv run python scripts/build_geomap.py

    # explicit paths (env-var-free)
    uv run python scripts/build_geomap.py \\
        --track-umap  outs/umap/umap_track_2d_nn150_md0d01_cosine.parquet \\
        --album-umap  outs/umap/umap_album_2d_nn150_md0d01_cosine.parquet \\
        --artist-umap outs/umap/umap_artist_2d_nn150_md0d01_cosine.parquet \\
        --label-umap  outs/umap/umap_label_2d_nn150_md0d01_cosine.parquet \\
        --output-dir  outs/geo/
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

from src.topo import umap2geo


ENTITIES = {
    "track":  ("track_umap",  "track_rowid",  "T2M_TRACK_UMAP"),
    "album":  ("album_umap",  "album_rowid",  "T2M_ALBUM_UMAP"),
    "artist": ("artist_umap", "artist_rowid", "T2M_ARTIST_UMAP"),
    "label":  ("label_umap",  "label",        "T2M_LABEL_UMAP"),
}


def main():
    parser = argparse.ArgumentParser(
        description="Joint-normalize UMAP coords to lon/lat for all entity types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--track-umap", default=os.environ.get("T2M_TRACK_UMAP"), metavar="PATH",
        help="UMAP parquet for tracks. $T2M_TRACK_UMAP",
    )
    parser.add_argument(
        "--album-umap", default=os.environ.get("T2M_ALBUM_UMAP"), metavar="PATH",
        help="UMAP parquet for albums. $T2M_ALBUM_UMAP",
    )
    parser.add_argument(
        "--artist-umap", default=os.environ.get("T2M_ARTIST_UMAP"), metavar="PATH",
        help="UMAP parquet for artists. $T2M_ARTIST_UMAP",
    )
    parser.add_argument(
        "--label-umap", default=os.environ.get("T2M_LABEL_UMAP"), metavar="PATH",
        help="UMAP parquet for labels. $T2M_LABEL_UMAP",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("T2M_GEO_DIR"),
        metavar="DIR",
        help="Directory for output geo-parquets (default: $T2M_GEO_DIR)",
    )
    parser.add_argument(
        "--width",
        type=float,
        default=os.environ.get("T2M_GEO_WIDTH"),
        metavar="DEG",
        help=f"Width in degrees of the lon/lat square (default: $T2M_GEO_WIDTH).",
    )
    parser.add_argument(
        "--padding",
        type=float,
        default=os.environ.get("T2M_GEO_PADDING"),
        metavar="DEG",
        help=f"Fractional padding added to each side of the bbox (default: $T2M_GEO_PADDING).",
    )
    args = parser.parse_args()

    if args.width is None:
        raise ValueError(
            "No `T2M_GEO_WIDTH` environment variable set. "
            "Either run with --width argument or define the environment variable."
        )
    hwidth = args.width / 2.
    if args.padding is None:
        raise ValueError(
            "No `T2M_GEO_PADDING` environment variable set. "
            "Either run with --padding argument or define the environment variable."
        )
    if args.output_dir is None:
        raise ValueError(
            "No `T2M_GEO_DIR` environment variable set. "
            "Either run with --output-dir argument or define the environment variable."
        )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # subset of entities whose UMAP path was provided; allows running on fewer than 4
    active = {
        entity: Path(getattr(args, attr))
        for entity, (attr, *_) in ENTITIES.items()
        if getattr(args, attr) is not None
    }

    if not active:
        print(
            "Error: no UMAP paths provided. Pass --track-umap / --album-umap / "
            "--artist-umap / --label-umap or set $T2M_TRACK_UMAP etc. in config.env",
            file=sys.stderr,
        )
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
        umap_frames, max_lon=hwidth, max_lat=hwidth, padding=args.padding
    )

    for entity, geo in zip(entity_names, geo_frames):
        out_path = output_dir / f"{entity}_geo.parquet"
        geo.to_parquet(out_path, index=False)
        print(f"{entity:8s}  {len(geo):>9,} rows  →  {out_path}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
