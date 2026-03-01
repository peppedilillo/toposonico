#!/usr/bin/env python3
"""Build a playlist lookup table from the playlist SQLite database.

Extracts display metadata and curation-quality signals for all playlists and
writes a parquet file keyed by playlist_rowid. Useful during chunk-based
training to filter or weight playlists by follower count / track count without
touching the database again.

Columns: playlist_rowid, name, description, public, owner_id,
         owner_display_name, followers_total, tracks_total.

Usage:
    python scripts/build_playlist_lookup.py <db> [-o OUTPUT]

Example:
    python scripts/build_playlist_lookup.py ~/HDD/Datasets/.../spotify_clean_playlists.sqlite3
"""

import argparse
from pathlib import Path
import sqlite3
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR = Path(__file__).parent.parent / "data"

QUERY = """
    SELECT
        rowid              AS playlist_rowid,
        name               AS name,
        description        AS description,
        public             AS public,
        owner_id           AS owner_id,
        owner_display_name AS owner_display_name,
        followers_total    AS followers_total,
        tracks_total       AS tracks_total
    FROM playlists
"""

SCHEMA = pa.schema(
    [
        pa.field("playlist_rowid", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("description", pa.string()),
        pa.field("public", pa.bool_()),
        pa.field("owner_id", pa.string()),
        pa.field("owner_display_name", pa.string()),
        pa.field("followers_total", pa.int32()),
        pa.field("tracks_total", pa.int32()),
    ]
)


def get_connection(database_path: Path) -> sqlite3.Connection:
    """
    Get a read-only connection to the database.

    Returns:
        sqlite3.Connection configured for read-only access
    """
    uri = f"file:{database_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    parser = argparse.ArgumentParser(
        description="Build playlist lookup table from playlist database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("database", type=Path, help="Path to playlist SQLite database")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output parquet path (default: data/playlist/playlist_lookup.parquet)",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Database not found: {args.database}")

    output_path = args.output or OUTPUT_DIR / "playlist_lookup.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database : {args.database}")
    print(f"Output   : {output_path}")
    print()
    print("Querying playlists...")

    t0 = time.time()
    conn = get_connection(args.database)
    df = pd.read_sql_query(QUERY, conn)
    conn.close()

    elapsed_query = time.time() - t0
    print(f"  {len(df):,} rows fetched in {elapsed_query:.1f}s")

    df["playlist_rowid"] = df["playlist_rowid"].astype("int32")
    df["public"] = df["public"].astype("boolean")
    df["followers_total"] = df["followers_total"].astype("Int32")
    df["tracks_total"] = df["tracks_total"].astype("int32")

    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    pq.write_table(table, output_path)

    elapsed = time.time() - t0
    size_mb = output_path.stat().st_size / 1_048_576

    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output : {output_path}  ({size_mb:.1f} MB)")
    print(f"Rows   : {len(df):,}")
    print(f"Bytes/row (compressed): {size_mb * 1_048_576 / len(df):.0f}")


if __name__ == "__main__":
    main()
