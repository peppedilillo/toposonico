"""Build NDJSON GeoJSON point exports for all entity types from the DB.

Reads lon/lat + logcount from sick.db and writes one newline-delimited GeoJSON
Feature file per entity for downstream tippecanoe ingestion.

Usage:
    source config.env && uv run python scripts/build_geojson.py
    uv run python scripts/build_geojson.py --db path/to/sick.db
"""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import time

from src.utils import ENTITY_KEYS as EKEYS
from src.utils import get_geojson_paths

ENTITY_CONFIGS = (
    ("track", "tracks", EKEYS.track),
    ("album", "albums", EKEYS.album),
    ("artist", "artists", EKEYS.artist),
    ("label", "labels", EKEYS.label),
)


def build_entity(
    conn: sqlite3.Connection,
    table: str,
    key_col: str,
    out_path: Path,
) -> None:
    rows = conn.execute(f"SELECT {key_col}, lon, lat, logcount FROM {table} ORDER BY {key_col}")
    total = 0
    started_at = time.time()

    with open(out_path, "w", encoding="utf-8") as out:
        for rowid, lon, lat, logcount in rows:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)],
                },
                "properties": {
                    key_col: int(rowid),
                    "logcount": float(logcount),
                },
            }
            out.write(json.dumps(feature, ensure_ascii=False))
            out.write("\n")
            total += 1

    elapsed = time.time() - started_at
    print(f"  {total:,} features written in {elapsed:.1f}s")
    print(f"  Saved to {out_path}")


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Build NDJSON GeoJSON point exports for all entity types.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("SICK_DB"),
        metavar="PATH",
        help="Path to sick.db. $SICK_DB",
    )
    args = parser.parse_args(argv)

    if args.db is None:
        raise ValueError("--db / $SICK_DB not set")

    geojson_paths = get_geojson_paths()
    conn = sqlite3.connect(args.db)

    print("track")
    build_entity(conn, "tracks", EKEYS.track, geojson_paths.track)

    print("album")
    build_entity(conn, "albums", EKEYS.album, geojson_paths.album)

    print("artist")
    build_entity(conn, "artists", EKEYS.artist, geojson_paths.artist)

    print("label")
    build_entity(conn, "labels", EKEYS.label, geojson_paths.label)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
