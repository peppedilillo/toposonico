"""Compute joint lon/lat coordinates for all entity types from UMAP projections.

Reads UMAP parquets for all entity types from the ml manifest, computes a single
global bounding box across all of them (with padding), then writes per-entity
geo-parquets containing only the key column + lon + lat.

Always run with all 4 entities together to keep coordinate alignment stable.
All four parquets must come from the same UMAP fit — mixing parquets from different
fits produces incompatible coordinates.

Usage:
    uv run python scripts/build_geomap.py [options]

Examples:
    source config.env && uv run python scripts/build_geomap.py
    uv run python scripts/build_geomap.py --manifest ml/outs/manifest.toml --output-dir outs/geo/
"""

import argparse
import os
from pathlib import Path
from typing import Sequence

import pandas as pd

from src.utils import get_auxpaths, read_manifest


ENTITIES = {
    "track":  "track_rowid",
    "album":  "album_rowid",
    "artist": "artist_rowid",
    "label":  "label",
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
        "--manifest",
        default=os.environ.get("SICK_MANIFEST"),
        metavar="PATH",
        help="Path to ml manifest TOML. $SICK_MANIFEST",
    )
    parser.add_argument(
        "--width",
        type=float,
        default=os.environ.get("SICK_GEO_WIDTH"),
        metavar="DEG",
        help="Width in degrees of the lon/lat square. $SICK_GEO_WIDTH",
    )
    parser.add_argument(
        "--padding",
        type=float,
        default=os.environ.get("SICK_GEO_PADDING"),
        metavar="DEG",
        help="Fractional padding added to each side of the bbox. $SICK_GEO_PADDING",
    )
    args = parser.parse_args()

    if args.manifest is None:
        raise ValueError("--manifest / $SICK_MANIFEST not set")
    if args.width is None:
        raise ValueError("--width / $SICK_GEO_WIDTH not set")
    if args.padding is None:
        raise ValueError("--padding / $SICK_GEO_PADDING not set")

    manifest = read_manifest(args.manifest)
    umap_paths = {entity: Path(p) for entity, p in manifest["umap"].items()}

    geo_paths = get_auxpaths()["geo"]
    next(iter(geo_paths.values())).parent.mkdir(parents=True, exist_ok=True)

    hwidth = args.width / 2.

    umap_frames = []
    entity_names = []
    for entity, path in umap_paths.items():
        key_col = ENTITIES[entity]
        df = pd.read_parquet(path, columns=[key_col, "umap_x", "umap_y"])
        umap_frames.append((df, key_col))
        entity_names.append(entity)

    geo_frames = umap2geo(
        umap_frames, max_lon=hwidth, max_lat=hwidth, padding=args.padding
    )

    for entity, geo in zip(entity_names, geo_frames):
        out_path = geo_paths[entity]
        geo.to_parquet(out_path, index=False)
        print(f"{entity:8s}  {len(geo):>9,} rows  →  {out_path}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
