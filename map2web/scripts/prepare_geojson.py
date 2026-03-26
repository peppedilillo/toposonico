"""Query the track2map SQLite DB and write lean ndjson for tippecanoe.

Each entity type (track, album, artist, label) is written to its own ndjson file.
Features carry only the entity rowid and LOD signals (logcounts, track_count); names
and metadata are served at query time by the backend. The tippecanoe build step uses
--exclude to strip logcounts and track_count from tile features, leaving only the rowid.

Usage:
    source config.env && python scripts/prepare_geojson.py <entity> [options]

    entity ∈ {track, album, artist, label}

Examples:
    source config.env && python scripts/prepare_geojson.py track
    source config.env && python scripts/prepare_geojson.py label --output /tmp/label.ndjson
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

LOG_EVERY_DEFAULT = 500_000

ENTITY_CONFIGS = {
    "track": {
        "key": "track_rowid",
        "sql": "SELECT track_rowid, lon, lat, logcounts FROM tracks",
    },
    "album": {
        "key": "album_rowid",
        "sql": "SELECT album_rowid, lon, lat, logcounts FROM albums",
    },
    "artist": {
        "key": "artist_rowid",
        "sql": "SELECT artist_rowid, lon, lat, logcounts FROM artists",
    },
    "label": {
        "key": "label_rowid",
        "sql": "SELECT label_id AS label_rowid, lon, lat, logcounts FROM labels",
    },
}


def main():
    parser = argparse.ArgumentParser(
        description="Write ndjson for tippecanoe from the track2map SQLite DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("entity", choices=list(ENTITY_CONFIGS), help="Entity type")
    parser.add_argument(
        "--db", type=Path, default=None,
        help="Path to track2map.db. Defaults to $T2M_DB.",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output ndjson path. Defaults to $M2W_GEOJSON_DIR/{entity}.ndjson.",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=LOG_EVERY_DEFAULT,
        metavar="N",
        help=f"Print progress every N features (default: {LOG_EVERY_DEFAULT:,})",
    )
    args = parser.parse_args()

    if args.db is None:
        val = os.environ.get("T2M_DB")
        if val is None:
            print("Error: T2M_DB is not set. Pass --db or source config.env.")
            sys.exit(1)
        args.db = Path(val)

    if not args.db.exists():
        print(f"Error: DB not found: {args.db}")
        sys.exit(1)

    if args.output is None:
        geojson_dir = os.environ.get("M2W_GEOJSON_DIR")
        if geojson_dir is None:
            print("Error: M2W_GEOJSON_DIR is not set. Pass --output or source config.env.")
            sys.exit(1)
        args.output = Path(geojson_dir) / f"{args.entity}.ndjson"

    cfg = ENTITY_CONFIGS[args.entity]
    key = cfg["key"]

    print(f"Entity : {args.entity}")
    print(f"DB     : {args.db}")
    print(f"Output : {args.output}")
    print()

    t0   = time.time()
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    print("Querying DB …")
    df = pd.read_sql_query(cfg["sql"], conn)
    conn.close()
    print(f"  {len(df):,} rows")

    lons   = df["lon"].values
    lats   = df["lat"].values
    keys   = df[key].values
    pops   = df["logcounts"].fillna(0).astype(np.float32).values

    args.output.parent.mkdir(parents=True, exist_ok=True)
    print("Writing ndjson …")
    n  = len(df)
    t1 = time.time()

    with open(args.output, "w", encoding="utf-8") as out:
        for i in range(n):
            props = {key: int(keys[i]), "logcounts": round(float(pops[i]), 2)}
            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lons[i]), float(lats[i])]},
                "properties": props,
            }
            out.write(json.dumps(feature, ensure_ascii=False))
            out.write("\n")

            if i > 0 and i % args.log_every == 0:
                elapsed = time.time() - t1
                print(f"  {i:>9,} / {n:,}  ({i / elapsed:,.0f} features/s)", end="\r")

    elapsed = time.time() - t0
    rate    = n / max(time.time() - t1, 1e-6)
    print(f"  {n:>9,} / {n:,}  ({rate:,.0f} features/s)     ")
    print()
    print(f"Done in {elapsed:.1f}s  —  {n:,} features written to {args.output}")


if __name__ == "__main__":
    main()
