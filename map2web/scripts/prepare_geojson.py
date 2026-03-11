"""Join a geo-parquet + lookup and write ndjson for tippecanoe.

Each entity type (track, album, artist, label) has its own join key and property
set. The geo-parquet is produced by normalize_coords.py; the lookup parquet comes
from track2map/outs/.

Input paths are resolved from CLI flags or M2W_* env vars (set via config.env).
Output path defaults to $M2W_GEOJSON_DIR/{entity}.ndjson.

Usage:
    source config.env && python scripts/prepare_geojson.py <entity> [options]

    entity ∈ {track, album, artist, label}

Examples:
    source config.env && python scripts/prepare_geojson.py track
    source config.env && python scripts/prepare_geojson.py album

    python scripts/prepare_geojson.py track \\
        --geo /path/to/track_geo.parquet \\
        --lookup /path/to/track_lookup.parquet \\
        --output /path/to/track.ndjson
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

LOG_EVERY_DEFAULT = 500_000

ENTITY_CONFIGS = {
    "track": {
        "key": "track_rowid",
        "lookup_cols": ["track_rowid", "track_name", "artist_name", "logcounts"],
        "name_cols": ["track_name", "artist_name"],
    },
    "album": {
        "key": "album_rowid",
        "lookup_cols": ["album_rowid", "album_name", "artist_name", "track_count", "logcounts"],
        "name_cols": ["album_name"],
    },
    "artist": {
        "key": "artist_rowid",
        "lookup_cols": ["artist_rowid", "artist_name", "track_count", "logcounts"],
        "name_cols": ["artist_name"],
    },
    "label": {
        "key": "label",
        "lookup_cols": ["label", "track_count", "logcounts"],
        "name_cols": ["label"],
    },
}


def main():
    parser = argparse.ArgumentParser(
        description="Write ndjson for tippecanoe from a geo-parquet + lookup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("entity", choices=list(ENTITY_CONFIGS), help="Entity type")
    parser.add_argument(
        "--geo", type=Path, default=None,
        help="Path to geo-parquet. Defaults to M2W_{ENTITY}_GEO env var.",
    )
    parser.add_argument(
        "--lookup", type=Path, default=None,
        help="Path to lookup parquet. Defaults to M2W_{ENTITY}_LOOKUP env var.",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output ndjson path. Defaults to M2W_GEOJSON_DIR/{entity}.ndjson.",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=LOG_EVERY_DEFAULT,
        metavar="N",
        help=f"Print progress every N features (default: {LOG_EVERY_DEFAULT:,})",
    )
    args = parser.parse_args()

    entity_upper = args.entity.upper()

    if args.geo is None:
        val = os.environ.get(f"M2W_{entity_upper}_GEO")
        if val is None:
            raise ValueError(
                f"M2W_{entity_upper}_GEO is not set. "
                f"Run with --geo or source config.env."
            )
        args.geo = Path(val)

    if args.lookup is None:
        val = os.environ.get(f"M2W_{entity_upper}_LOOKUP")
        if val is None:
            raise ValueError(
                f"M2W_{entity_upper}_LOOKUP is not set. "
                f"Run with --lookup or source config.env."
            )
        args.lookup = Path(val)

    if args.output is None:
        geojson_dir = os.environ.get("M2W_GEOJSON_DIR")
        if geojson_dir is None:
            raise ValueError(
                "M2W_GEOJSON_DIR is not set. "
                "Run with --output or source config.env."
            )
        args.output = Path(geojson_dir) / f"{args.entity}.ndjson"

    for path, label in [(args.geo, "geo"), (args.lookup, "lookup")]:
        if not path.exists():
            print(f"Error: {label} parquet not found: {path}")
            sys.exit(1)

    cfg = ENTITY_CONFIGS[args.entity]
    key = cfg["key"]

    print(f"Entity  : {args.entity}")
    print(f"Geo     : {args.geo}")
    print(f"Lookup  : {args.lookup}")
    print(f"Output  : {args.output}")
    print()

    t0 = time.time()

    print("Loading geo …")
    geo = pd.read_parquet(args.geo, columns=[key, "lon", "lat"])
    print(f"  {len(geo):,} rows")

    print("Loading lookup …")
    lookup = pd.read_parquet(args.lookup, columns=cfg["lookup_cols"])
    print(f"  {len(lookup):,} rows")

    print("Joining …")
    df = geo.merge(lookup, on=key, how="inner")
    print(f"  {len(df):,} rows after join")

    before = len(df)
    df = df.dropna(subset=cfg["name_cols"])
    df = df[~df[cfg["name_cols"]].apply(lambda col: col.str.strip() == "").any(axis=1)]
    dropped = before - len(df)
    print(f"  {dropped:,} dropped (null/empty name)  →  {len(df):,} remaining")

    lons = df["lon"].values
    lats = df["lat"].values
    keys = df[key].values

    artists: np.ndarray = np.empty(0)
    counts: np.ndarray = np.empty(0)
    if args.entity == "track":
        names = df["track_name"].astype(str).values
        artists = df["artist_name"].astype(str).values
        pops = df["logcounts"].fillna(0).astype(np.int32).values
    elif args.entity == "album":
        names = df["album_name"].astype(str).values
        artists = df["artist_name"].astype(str).values
        counts = df["track_count"].fillna(0).astype(np.int32).values
        pops = df["logcounts"].fillna(0).astype(np.float32).values
    elif args.entity == "artist":
        names = df["artist_name"].astype(str).values
        counts = df["track_count"].fillna(0).astype(np.int32).values
        pops = df["logcounts"].fillna(0).astype(np.float32).values
    else:  # label
        names = df["label"].astype(str).values
        counts = df["track_count"].fillna(0).astype(np.int32).values
        pops = df["logcounts"].fillna(0).astype(np.float32).values

    args.output.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing ndjson …")
    n = len(df)
    t1 = time.time()

    with open(args.output, "w", encoding="utf-8") as out:
        for i in range(n):
            if args.entity == "track":
                props = {
                    "track_rowid": int(keys[i]),
                    "track_name": names[i],
                    "artist_name": artists[i],
                    "logcounts": int(pops[i]),
                }
            elif args.entity == "album":
                props = {
                    "album_rowid": int(keys[i]),
                    "album_name": names[i],
                    "artist_name": artists[i],
                    "track_count": int(counts[i]),
                    "logcounts": round(float(pops[i]), 2),
                }
            elif args.entity == "artist":
                props = {
                    "artist_rowid": int(keys[i]),
                    "artist_name": names[i],
                    "track_count": int(counts[i]),
                    "logcounts": round(float(pops[i]), 2),
                }
            else:  # label
                props = {
                    "label": names[i],
                    "track_count": int(counts[i]),
                    "logcounts": round(float(pops[i]), 2),
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
                print(f"  {i:>9,} / {n:,}  ({rate:,.0f} features/s)", end="\r")

    elapsed = time.time() - t0
    rate = n / (time.time() - t1)
    print(f"  {n:>9,} / {n:,}  ({rate:,.0f} features/s)     ")
    print()
    print(f"Done in {elapsed:.1f}s  —  {n:,} features written to {args.output}")


if __name__ == "__main__":
    main()
