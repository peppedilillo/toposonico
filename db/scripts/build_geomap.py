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
from typing import Sequence

import pandas as pd


ENTITIES = {
    "track":  ("umap_track",  "track_rowid",  "SICK_UMAP_TRACK"),
    "album":  ("umap_album",  "album_rowid",  "SICK_UMAP_ALBUM"),
    "artist": ("umap_artist", "artist_rowid", "SICK_UMAP_ARTIST"),
    "label":  ("umap_label",  "label",        "SICK_UMAP_LABEL"),
}


def umap2geo(
    umap_frames: Sequence[tuple[pd.DataFrame, str]],
    max_lon: float = 22.5,
    max_lat: float = 22.5,
    padding: float = 1.,
) -> list[pd.DataFrame]:
    """Map UMAP coordinates to fake lon/lat using a shared bounding box.

    Computes a single global bbox across all input frames (so entity types stay
    spatially aligned), applies fractional padding, then normalises each frame to
    [-max_lon, +max_lon] × [-max_lat, +max_lat].

    Args:
        umap_frames: sequence of (df, key_col) pairs. Each df must have columns
            ``umap_x`` and ``umap_y`` plus the key column.
        max_lon: half-width in degrees for the x axis (default 22.5).
        max_lat: half-width in degrees for the y axis (default 22.5).
        padding: padding added to each side of the global bbox (default 1.).

    Returns:
        List of DataFrames, one per input, with columns [key_col, lon, lat] (float32).
    """
    import numpy as np

    x_min = min(df.umap_x.min() for df, _ in umap_frames)
    x_max = max(df.umap_x.max() for df, _ in umap_frames)
    y_min = min(df.umap_y.min() for df, _ in umap_frames)
    y_max = max(df.umap_y.max() for df, _ in umap_frames)

    x_min -= padding
    x_max += padding
    y_min -= padding
    y_max += padding

    results = []
    for df, key_col in umap_frames:
        x_norm = (df.umap_x - x_min) / (x_max - x_min)
        y_norm = (df.umap_y - y_min) / (y_max - y_min)
        results.append(
            pd.DataFrame(
                {
                    key_col: df[key_col],
                    "lon": (x_norm * 2 * max_lon - max_lon).round(6).astype(np.float32),
                    "lat": (y_norm * 2 * max_lat - max_lat).round(6).astype(np.float32),
                }
            )
        )
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Joint-normalize UMAP coords to lon/lat for all entity types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--umap-track", default=os.environ.get("SICK_UMAP_TRACK"), metavar="PATH",
        help="UMAP parquet for tracks.Set to `SICK_UMAP_TRACK` by default.",
    )
    parser.add_argument(
        "--umap-album", default=os.environ.get("SICK_UMAP_ALBUM"), metavar="PATH",
        help="UMAP parquet for albums. Set to `SICK_UMAP_ALBUM` by default.",
    )
    parser.add_argument(
        "--umap-artist", default=os.environ.get("SICK_UMAP_ARTIST"), metavar="PATH",
        help="UMAP parquet for artists. Set to `SICK_UMAP_ARTIST` by default.",
    )
    parser.add_argument(
        "--umap-label", default=os.environ.get("SICK_UMAP_LABEL"), metavar="PATH",
        help="UMAP parquet for labels. Set to `SICK_UMAP_LABEL` by default.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("SICK_GEO_DIR"),
        metavar="DIR",
        help="Directory for output geo-parquets. Set to `SICK_GEO_DIR` by default.",
    )
    parser.add_argument(
        "--width",
        type=float,
        default=os.environ.get("SICK_GEO_WIDTH"),
        metavar="DEG",
        help=f"Width in degrees of the lon/lat square. Set to `SICK_GEO_WIDTH` by default.",
    )
    parser.add_argument(
        "--padding",
        type=float,
        default=os.environ.get("SICK_GEO_PADDING"),
        metavar="DEG",
        help=f"Fractional padding added to each side of the bbox. Set to `SICK_GEO_PADDING` by default.",
    )
    args = parser.parse_args()

    if args.width is None:
        raise ValueError(
            "No `SICK_GEO_WIDTH` environment variable set. "
            "Either run with --width argument or define the environment variable."
        )
    hwidth = args.width / 2.
    if args.padding is None:
        raise ValueError(
            "No `SICK_GEO_PADDING` environment variable set. "
            "Either run with --padding argument or define the environment variable."
        )
    if args.output_dir is None:
        raise ValueError(
            "No `SICK_GEO_DIR` environment variable set. "
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
            "Error: no UMAP paths provided. Pass --umap-track / --umap-album / "
            "--umap-artist / --umap-label or set $SICK_UMAP_TRACK etc. in config.env",
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
        out_path = output_dir / f"geo_{entity}.parquet"
        geo.to_parquet(out_path, index=False)
        print(f"{entity:8s}  {len(geo):>9,} rows  →  {out_path}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
