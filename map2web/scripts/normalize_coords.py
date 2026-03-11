"""Compute joint lon/lat coordinates for all entity types from UMAP projections.

Reads UMAP parquets for any combination of tracks, albums, artists, and labels,
computes a single global bounding box across all of them (with padding), then
writes per-entity geo-parquets containing only the key column + lon + lat.

Running with a subset of entities shifts the bbox — always run with all 4 together
to keep coordinate alignment stable across entity types.

Usage:
    python scripts/normalize_coords.py \\
        [--track-umap PATH] [--album-umap PATH] \\
        [--artist-umap PATH] [--label-umap PATH] \\
        [--output-dir DIR] [--extent DEG] [--padding FRAC]

Examples:
    UMAP=../track2map/outs/umap
    python scripts/normalize_coords.py \\
        --track-umap  $UMAP/umap_track_2d_pure_bolt_nn150_md0d01_cosine.parquet \\
        --album-umap  $UMAP/umap_album_2d_pure_bolt_nn150_md0d01_cosine.parquet \\
        --artist-umap $UMAP/umap_artist_2d_pure_bolt_nn150_md0d01_cosine.parquet \\
        --label-umap  $UMAP/umap_label_2d_pure_bolt_nn150_md0d01_cosine.parquet
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ENTITIES = {
    "track": ("track_umap", "track_rowid"),
    "album": ("album_umap", "album_rowid"),
    "artist": ("artist_umap", "artist_rowid"),
    "label": ("label_umap", "label"),
}

OUTPUT_DIR_DEFAULT = Path(__file__).parent.parent / "assets" / "geo"
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
        type=Path,
        default=OUTPUT_DIR_DEFAULT,
        metavar="DIR",
        help=f"Directory for output geo-parquets (default: {OUTPUT_DIR_DEFAULT})",
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

    inputs = {
        entity: getattr(args, attr.replace("-", "_"))
        for entity, (attr, _) in ENTITIES.items()
    }
    active = {e: p for e, p in inputs.items() if p is not None}

    if not active:
        print("Error: at least one --*-umap argument is required", file=sys.stderr)
        sys.exit(1)

    for entity, path in active.items():
        if not path.exists():
            print(f"Error: {entity} UMAP parquet not found: {path}", file=sys.stderr)
            sys.exit(1)

    # Load all and compute global bbox
    frames = {}
    x_min_global = np.inf
    x_max_global = -np.inf
    y_min_global = np.inf
    y_max_global = -np.inf

    for entity, path in active.items():
        key_col = ENTITIES[entity][1]
        df = pd.read_parquet(path, columns=[key_col, "umap_x", "umap_y"])
        frames[entity] = (df, key_col)
        x_min_global = min(x_min_global, df.umap_x.min())
        x_max_global = max(x_max_global, df.umap_x.max())
        y_min_global = min(y_min_global, df.umap_y.min())
        y_max_global = max(y_max_global, df.umap_y.max())

    x_range = x_max_global - x_min_global
    y_range = y_max_global - y_min_global
    pad = args.padding

    x_min = x_min_global - x_range * pad
    x_max = x_max_global + x_range * pad
    y_min = y_min_global - y_range * pad
    y_max = y_max_global + y_range * pad

    print(f"Global bbox (pre-padding):  x=[{x_min_global:.4f}, {x_max_global:.4f}]  y=[{y_min_global:.4f}, {y_max_global:.4f}]")
    print(f"Global bbox (post-padding): x=[{x_min:.4f}, {x_max:.4f}]  y=[{y_min:.4f}, {y_max:.4f}]")
    print(f"Extent: ±{args.extent}°  Padding: {pad*100:.1f}%")
    print()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    e = args.extent

    for entity, (df, key_col) in frames.items():
        x_norm = (df.umap_x - x_min) / (x_max - x_min)
        y_norm = (df.umap_y - y_min) / (y_max - y_min)

        out = pd.DataFrame({
            key_col: df[key_col],
            "lon": (x_norm * 2 * e - e).round(6).astype(np.float32),
            "lat": (y_norm * 2 * e - e).round(6).astype(np.float32),
        })

        out_path = args.output_dir / f"{entity}_geo.parquet"
        out.to_parquet(out_path, index=False)
        print(f"{entity:8s}  {len(out):>9,} rows  →  {out_path}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
