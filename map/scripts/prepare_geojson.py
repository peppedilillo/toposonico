"""Build line-delimited GeoJSON for tippecanoe from a UMAP projection + track lookup.

Maps UMAP coordinates to a fake geographic square centred on the origin so that
Mercator distortion is negligible. Tracks with missing name or artist are dropped.
Each feature carries a ``tippecanoe-minzoom`` property derived from track_popularity
so that popular tracks appear at low zoom and the long tail only at max zoom.

Output is streamed to stdout; progress and stats go to stderr.

Usage:
    python scripts/prepare_geojson.py <umap> <lookup> [options]

Examples:
    python scripts/prepare_geojson.py \\
        ../umap/outs/umap/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \\
        ../w2v/outs/track_lookup.parquet \\
        | tippecanoe -e web/public/tiles -z7 -Z0 -l tracks --force

    # write ndjson to file first (easier to re-run tippecanoe without re-generating):
    python scripts/prepare_geojson.py \\
        ../umap/outs/umap/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \\
        ../w2v/outs/track_lookup.parquet \\
        > /tmp/tracks.ndjson
    tippecanoe -e web/public/tiles -z7 -Z0 -l tracks --force /tmp/tracks.ndjson
"""

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

LOG_EVERY_DEFAULT = 500_000
EXTENT_DEFAULT = 45.0  # degrees; UMAP space maps to [-EXTENT, +EXTENT] on both axes
MAX_ZOOM_DEFAULT = 7


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main():
    parser = argparse.ArgumentParser(
        description="Stream ndjson for tippecanoe from UMAP projection + track lookup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage:")[1],
    )
    parser.add_argument("umap", type=Path, help="Path to UMAP projection parquet")
    parser.add_argument("lookup", type=Path, help="Path to track lookup parquet")
    parser.add_argument(
        "--extent",
        type=float,
        default=EXTENT_DEFAULT,
        metavar="DEG",
        help=f"Half-width in degrees of the fake lon/lat square (default: {EXTENT_DEFAULT})",
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=MAX_ZOOM_DEFAULT,
        metavar="Z",
        help=f"Maximum zoom level passed to tippecanoe (default: {MAX_ZOOM_DEFAULT})",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=LOG_EVERY_DEFAULT,
        metavar="N",
        help=f"Print progress every N features (default: {LOG_EVERY_DEFAULT:,})",
    )
    args = parser.parse_args()

    if not args.umap.exists():
        raise FileNotFoundError(f"UMAP parquet not found: {args.umap}")
    if not args.lookup.exists():
        raise FileNotFoundError(f"Lookup parquet not found: {args.lookup}")

    eprint(f"UMAP    : {args.umap}")
    eprint(f"Lookup  : {args.lookup}")
    eprint(f"Extent  : ±{args.extent}°  (lon/lat)")
    eprint(f"Max zoom: {args.max_zoom}")
    eprint()


    t0 = time.time()
    eprint("Loading UMAP projection …")
    umap = pd.read_parquet(args.umap, columns=["track_rowid", "umap_x", "umap_y"])
    eprint(f"  {len(umap):,} tracks")

    eprint("Loading track lookup …")
    lookup = pd.read_parquet(
        args.lookup,
        columns=["track_rowid", "track_name", "artist_name", "track_popularity"],
    )
    eprint(f"  {len(lookup):,} rows")

    eprint("Joining …")
    df = umap.merge(lookup, on="track_rowid", how="inner")
    eprint(f"  {len(df):,} tracks after join")

    before = len(df)
    df = df.dropna(subset=["track_name", "artist_name"])
    dropped = before - len(df)
    eprint(f"  {dropped:,} dropped (missing name or artist)  →  {len(df):,} remaining")

    df["track_popularity"] = df["track_popularity"].fillna(0).astype(np.uint8)
    df["track_name"] = df["track_name"].astype(str)
    df["artist_name"] = df["artist_name"].astype(str)

    e = args.extent
    x_min, x_max = df.umap_x.min(), df.umap_x.max()
    y_min, y_max = df.umap_y.min(), df.umap_y.max()
    x_norm = (df.umap_x - x_min) / (x_max - x_min)  # [0, 1]
    y_norm = (df.umap_y - y_min) / (y_max - y_min)  # [0, 1]
    lons = (x_norm * 2 * e - e).round(6).values  # [-e, +e]
    lats = (y_norm * 2 * e - e).round(6).values  # [-e, +e]

    # ---------------------------------- tippecanoe-minzoom from popularity
    # popularity 100 → minzoom 0 (always visible)
    # popularity   0 → minzoom max_zoom (only at full detail)
    pop = df.track_popularity.values.astype(np.int32)
    minzooms = np.clip(args.max_zoom - (pop * args.max_zoom // 100), 0, args.max_zoom)

    rowids = df.track_rowid.values
    names = df.track_name.values
    artists = df.artist_name.values

    eprint("Streaming ndjson to stdout …")
    n = len(df)
    t1 = time.time()
    out = sys.stdout

    for i in range(n):
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lons[i]), float(lats[i])],
            },
            "properties": {
                "track_rowid": int(rowids[i]),
                #"track_name": names[i],
                #"artist_name": artists[i],
                "track_popularity": int(pop[i]),
                "tippecanoe-minzoom": int(minzooms[i]),
            },
        }
        out.write(json.dumps(feature, ensure_ascii=False))
        out.write("\n")

        if i > 0 and i % args.log_every == 0:
            elapsed = time.time() - t1
            rate = i / elapsed
            eprint(f"  {i:>9,} / {n:,}  ({rate:,.0f} features/s)", end="\r")

    elapsed = time.time() - t0
    rate = n / (time.time() - t1)
    eprint(f"  {n:>9,} / {n:,}  ({rate:,.0f} features/s)     ")
    eprint()
    eprint(f"Done in {elapsed:.1f}s  —  {n:,} features written")


if __name__ == "__main__":
    main()
