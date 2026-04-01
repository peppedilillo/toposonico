"""Compute joint lon/lat coordinates for all entity types from UMAP projections.

Reads UMAP parquets from the ml manifest, applies per-entity filter indices (built by
build_index.py) to restrict to the DB-visible subset, then computes a single global
bounding box across all four entity types (with padding) and writes per-entity
geo-parquets containing only the key column + lon + lat.

Always run with all 4 entities together to keep coordinates spatially aligned.
All four UMAP parquets must come from the same UMAP fit — mixing parquets from
different fits produces incompatible coordinates.

Output directory is set via $SICK_OUT_DIR; parquets land in $SICK_OUT_DIR/geo/.

Usage:
    uv run python scripts/build_geomap.py [options]

Examples:
    source config.env && uv run python scripts/build_geomap.py
    uv run python scripts/build_geomap.py --manifest ml/outs/manifest.toml
"""

import argparse
import os

import numpy as np
import pandas as pd

from src.utils import ENTITY_KEYS as EKEYS
from src.utils import get_geo_paths
from src.utils import read_manifest
from src.utils import get_index_filter_db_paths
from src.utils import EntityPaths
from src.utils import EntityTable


def umap2geo(
    umap: EntityTable,
    max_lon: float = 22.5,
    max_lat: float = 22.5,
    padding: float = 1.0,
) -> EntityTable:
    """Map UMAP coordinates to lon/lat using a joint bounding box across all entity types.

    Computes a single global bbox over all four entity DataFrames (so entity types stay
    spatially aligned), applies coordinate-space padding, then normalises each frame into
    [-|max_lon|, +|max_lon|] × [-|max_lat|, +|max_lat|].

    Args:
        umap: EntityTable of four DataFrames. Each must have ``umap_x``, ``umap_y``,
            and the entity key column (e.g. ``track_rowid``).
        max_lon: half-width in degrees for the x axis (default 22.5). Passing a
            negative value flips the x-axis (east↔west mirror) — used in main() to
            correct UMAP's default x-axis orientation.
        max_lat: half-width in degrees for the y axis (default 22.5).
        padding: margin added to each side of the global UMAP bbox before
            normalisation, in UMAP coordinate space (default 1.0).

    Returns:
        EntityTable of four DataFrames, each with columns [key_col, lon, lat] (float32).
    """
    x_min = min(df.umap_x.min() for df in umap)
    x_max = max(df.umap_x.max() for df in umap)
    y_min = min(df.umap_y.min() for df in umap)
    y_max = max(df.umap_y.max() for df in umap)

    x_min -= padding
    x_max += padding
    y_min -= padding
    y_max += padding

    results = []
    for key_col, df in (
        (EKEYS.track, umap.track),
        (EKEYS.artist, umap.artist),
        (EKEYS.album, umap.album),
        (EKEYS.label, umap.label),
    ):
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
    return EntityTable(*results)


def read_and_filter(umaps: EntityPaths, filters: EntityPaths) -> EntityTable:
    """Load UMAP parquets and restrict each entity to the rows listed in its filter index.

    Args:
        umaps: per-entity paths to UMAP parquets (must contain umap_x, umap_y, key column).
        filters: per-entity paths to .npy arrays of key values to keep (the DB-visible subset).

    Returns:
        EntityTable of filtered DataFrames.
    """
    return EntityTable(
        track=pd.read_parquet(umaps.track).set_index(EKEYS.track).loc[np.load(filters.track)].reset_index(),
        artist=pd.read_parquet(umaps.artist).set_index(EKEYS.artist).loc[np.load(filters.artist)].reset_index(),
        album=pd.read_parquet(umaps.album).set_index(EKEYS.album).loc[np.load(filters.album)].reset_index(),
        label=pd.read_parquet(umaps.label).set_index(EKEYS.label).loc[np.load(filters.label)].reset_index(),
    )


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

    hwidth = args.width / 2.0
    geo_paths = get_geo_paths()
    manifest = read_manifest(args.manifest)
    umap = manifest["umap"]
    filters = get_index_filter_db_paths()

    geo = umap2geo(read_and_filter(umap, filters), -hwidth, +hwidth, args.padding)

    geo.track.to_parquet(geo_paths.track, index=False)
    print(f"{'track':8s}  {len(geo.track):>9,} rows  →  {geo_paths.track}")
    geo.artist.to_parquet(geo_paths.artist, index=False)
    print(f"{'artist':8s}  {len(geo.artist):>9,} rows  →  {geo_paths.artist}")
    geo.album.to_parquet(geo_paths.album, index=False)
    print(f"{'album':8s}  {len(geo.album):>9,} rows  →  {geo_paths.album}")
    geo.label.to_parquet(geo_paths.label, index=False)
    print(f"{'label':8s}  {len(geo.label):>9,} rows  →  {geo_paths.label}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
