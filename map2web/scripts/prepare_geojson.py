"""Join a geo-parquet + lookup and stream ndjson to stdout for tippecanoe.

Each entity type (track, album, artist, label) has its own join key and property
set. The geo-parquet is produced by normalize_coords.py; the lookup parquet comes
from track2map/outs/.

Per-feature tippecanoe-minzoom encodes LOD: popular entities appear at low zoom,
the long tail only at max zoom. Labels get a fixed minzoom of 3.

Progress is printed to stderr every --log-every features.

Usage:
    python scripts/prepare_geojson.py <entity> <geo_parquet> <lookup_parquet> \\
        [--max-zoom Z] [--log-every N]

    entity ∈ {track, album, artist, label}

Examples:
    python scripts/prepare_geojson.py track \\
        assets/geo/track_geo.parquet ../track2map/outs/track_lookup.parquet \\
        > assets/tracks.ndjson

    python scripts/prepare_geojson.py label \\
        assets/geo/label_geo.parquet ../track2map/outs/label_lookup.parquet \\
        > assets/labels.ndjson
"""

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

MAX_ZOOM_DEFAULT = 7
LOG_EVERY_DEFAULT = 500_000
LABEL_MINZOOM = 3

ENTITY_CONFIGS = {
    "track": {
        "key": "track_rowid",
        "lookup_cols": ["track_rowid", "track_name", "artist_name", "track_popularity"],
        "name_cols": ["track_name", "artist_name"],
    },
    "album": {
        "key": "album_rowid",
        "lookup_cols": ["album_rowid", "album_name", "track_count", "mean_popularity"],
        "name_cols": ["album_name"],
    },
    "artist": {
        "key": "artist_rowid",
        "lookup_cols": ["artist_rowid", "artist_name", "track_count", "mean_popularity"],
        "name_cols": ["artist_name"],
    },
    "label": {
        "key": "label",
        "lookup_cols": ["label", "track_count", "mean_popularity"],
        "name_cols": ["label"],
    },
}


def minzoom_from_pop(pop: np.ndarray, max_zoom: int) -> np.ndarray:
    return np.clip(max_zoom - (pop.astype(np.int32) * max_zoom // 100), 0, max_zoom)


def main():
    parser = argparse.ArgumentParser(
        description="Stream ndjson for tippecanoe from a geo-parquet + lookup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("entity", choices=list(ENTITY_CONFIGS), help="Entity type")
    parser.add_argument("geo", type=Path, help="Path to geo-parquet (from normalize_coords.py)")
    parser.add_argument("lookup", type=Path, help="Path to lookup parquet")
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=MAX_ZOOM_DEFAULT,
        metavar="Z",
        help=f"Maximum zoom level (default: {MAX_ZOOM_DEFAULT})",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=LOG_EVERY_DEFAULT,
        metavar="N",
        help=f"Print progress every N features (default: {LOG_EVERY_DEFAULT:,})",
    )
    args = parser.parse_args()

    for path, label in [(args.geo, "geo"), (args.lookup, "lookup")]:
        if not path.exists():
            print(f"Error: {label} parquet not found: {path}", file=sys.stderr)
            sys.exit(1)

    cfg = ENTITY_CONFIGS[args.entity]
    key = cfg["key"]
    Z = args.max_zoom

    print(f"Entity  : {args.entity}", file=sys.stderr)
    print(f"Geo     : {args.geo}", file=sys.stderr)
    print(f"Lookup  : {args.lookup}", file=sys.stderr)
    print(f"Max zoom: {Z}", file=sys.stderr)
    print(file=sys.stderr)

    t0 = time.time()

    print("Loading geo …", file=sys.stderr)
    geo = pd.read_parquet(args.geo, columns=[key, "lon", "lat"])
    print(f"  {len(geo):,} rows", file=sys.stderr)

    print("Loading lookup …", file=sys.stderr)
    lookup = pd.read_parquet(args.lookup, columns=cfg["lookup_cols"])
    print(f"  {len(lookup):,} rows", file=sys.stderr)

    print("Joining …", file=sys.stderr)
    df = geo.merge(lookup, on=key, how="inner")
    print(f"  {len(df):,} rows after join", file=sys.stderr)

    before = len(df)
    df = df.dropna(subset=cfg["name_cols"])
    df = df[~df[cfg["name_cols"]].apply(lambda col: col.str.strip() == "").any(axis=1)]
    dropped = before - len(df)
    print(f"  {dropped:,} dropped (null/empty name)  →  {len(df):,} remaining", file=sys.stderr)

    # Build minzoom array
    if args.entity == "label":
        minzooms = np.full(len(df), LABEL_MINZOOM, dtype=np.int32)
    else:
        pop = df["mean_popularity"].fillna(0).values if "mean_popularity" in df.columns else df["track_popularity"].fillna(0).values
        minzooms = minzoom_from_pop(pop, Z)

    lons = df["lon"].values
    lats = df["lat"].values
    keys = df[key].values

    # Pre-extract property arrays per entity
    artists: np.ndarray = np.empty(0)
    counts: np.ndarray = np.empty(0)
    if args.entity == "track":
        names = df["track_name"].astype(str).values
        artists = df["artist_name"].astype(str).values
        pops = df["track_popularity"].fillna(0).astype(np.int32).values
    elif args.entity == "album":
        names = df["album_name"].astype(str).values
        counts = df["track_count"].fillna(0).astype(np.int32).values
        pops = df["mean_popularity"].fillna(0).astype(np.float32).values
    elif args.entity == "artist":
        names = df["artist_name"].astype(str).values
        counts = df["track_count"].fillna(0).astype(np.int32).values
        pops = df["mean_popularity"].fillna(0).astype(np.float32).values
    else:  # label
        names = df["label"].astype(str).values
        counts = df["track_count"].fillna(0).astype(np.int32).values
        pops = df["mean_popularity"].fillna(0).astype(np.float32).values

    print("Streaming ndjson to stdout …", file=sys.stderr)
    n = len(df)
    t1 = time.time()
    out = sys.stdout

    for i in range(n):
        if args.entity == "track":
            props = {
                "track_rowid": int(keys[i]),
                "track_name": names[i],
                "artist_name": artists[i],
                "track_popularity": int(pops[i]),
                "tippecanoe-minzoom": int(minzooms[i]),
            }
        elif args.entity == "album":
            props = {
                "album_rowid": int(keys[i]),
                "album_name": names[i],
                "track_count": int(counts[i]),
                "mean_popularity": round(float(pops[i]), 2),
                "tippecanoe-minzoom": int(minzooms[i]),
            }
        elif args.entity == "artist":
            props = {
                "artist_rowid": int(keys[i]),
                "artist_name": names[i],
                "track_count": int(counts[i]),
                "mean_popularity": round(float(pops[i]), 2),
                "tippecanoe-minzoom": int(minzooms[i]),
            }
        else:  # label
            props = {
                "label": names[i],
                "track_count": int(counts[i]),
                "mean_popularity": round(float(pops[i]), 2),
                "tippecanoe-minzoom": int(minzooms[i]),
            }

        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lons[i]), float(lats[i])]},
            "properties": props,
        }
        out.write(json.dumps(feature, ensure_ascii=False))
        out.write("\n")

        if i > 0 and i % args.log_every == 0:
            elapsed = time.time() - t1
            rate = i / elapsed
            print(f"  {i:>9,} / {n:,}  ({rate:,.0f} features/s)", end="\r", file=sys.stderr)

    elapsed = time.time() - t0
    rate = n / (time.time() - t1)
    print(f"  {n:>9,} / {n:,}  ({rate:,.0f} features/s)     ", file=sys.stderr)
    print(file=sys.stderr)
    print(f"Done in {elapsed:.1f}s  —  {n:,} features written", file=sys.stderr)


if __name__ == "__main__":
    main()
