"""Enrich the base training vocabulary with stable entity ids from metadata.

The base vocabulary defines trainable track membership via playlist evidence.
This script joins that membership set to the metadata DB and emits the enriched
training dimension used by downstream ML stages.
"""

import argparse
import os
from pathlib import Path
import sqlite3
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_SIZE_DEFAULT = 50_000
TEMP_TABLE_NAME = "training_vocab_tmp"

QUERY = """
    SELECT
        v.track_rowid      AS track_rowid,
        v.track_id         AS track_id,
        v.playlist_count   AS playlist_count,
        t.external_id_isrc AS id_isrc,
        ta.artist_rowid    AS artist_rowid,
        t.album_rowid      AS album_rowid,
        al.label           AS label
    FROM {temp_table} AS v
    INNER JOIN tracks        AS t  ON t.rowid         = v.track_rowid
    INNER JOIN albums        AS al ON t.album_rowid   = al.rowid
    INNER JOIN track_artists AS ta ON t.rowid         = ta.track_rowid
    WHERE ta.artist_rowid = (
        SELECT artist_rowid FROM track_artists WHERE track_rowid = t.rowid LIMIT 1
    )
"""

SCHEMA = pa.schema(
    [
        pa.field("track_rowid", pa.int64()),
        pa.field("track_id", pa.int32()),
        pa.field("playlist_count", pa.int32()),
        pa.field("artist_rowid", pa.int64()),
        pa.field("album_rowid", pa.int64()),
        pa.field("label_rowid", pa.int32()),
        pa.field("id_isrc", pa.string()),
    ]
)


def get_connection(database_path: Path) -> sqlite3.Connection:
    """Open a read-only SQLite connection to the given database path."""
    uri = f"file:{database_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def assign_label_rowids(labels: pd.Series) -> dict[str, int]:
    """Return a deterministic {label_string: label_rowid} mapping.

    Labels are sorted lexicographically so the mapping is stable across runs.
    Ids start at 1; 0 is reserved as the null/unknown sentinel (NULL in parquet,
    stored as pd.NA via Int32 nullable dtype).
    """
    valid = sorted({label for label in labels.dropna().unique() if label != ""})
    return {label: idx for idx, label in enumerate(valid, start=1)}


def create_temp_vocab_table(conn: sqlite3.Connection, table_name: str = TEMP_TABLE_NAME) -> None:
    """Create (or replace) a temporary vocab table for the join query."""
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"""
        CREATE TEMP TABLE {table_name} (
            track_rowid     INTEGER PRIMARY KEY,
            track_id        INTEGER NOT NULL,
            playlist_count  INTEGER NOT NULL
        )
        """)
    conn.execute(f"CREATE INDEX {table_name}_track_rowid_idx ON {table_name}(track_rowid)")


def load_temp_vocab_table(
    conn: sqlite3.Connection,
    vocab: pd.DataFrame,
    chunk_size: int,
    table_name: str = TEMP_TABLE_NAME,
) -> None:
    """Insert vocab rows into the temp table in chunks, printing progress."""
    rows = list(vocab[["track_rowid", "track_id", "playlist_count"]].itertuples(index=False, name=None))
    total = len(rows)
    started_at = time.time()

    for start in range(0, total, chunk_size):
        batch = rows[start : start + chunk_size]
        conn.executemany(
            f"""
            INSERT INTO {table_name} (track_rowid, track_id, playlist_count)
            VALUES (?, ?, ?)
            """,
            batch,
        )
        done = min(start + chunk_size, total)
        elapsed = time.time() - started_at
        rate = done / elapsed if elapsed > 0 else 0.0
        print(f"  {done:>10,} / {total:,} staged  ({rate:,.0f} rows/s)", end="\r")

    print()


def fetch_joined_metadata(conn: sqlite3.Connection, table_name: str = TEMP_TABLE_NAME) -> pd.DataFrame:
    """Execute the enrichment join and return one row per track with entity ids."""
    query = QUERY.format(temp_table=table_name)
    metadata = pd.read_sql_query(query, conn)
    if metadata.empty:
        return metadata
    metadata["track_rowid"] = metadata["track_rowid"].astype("int64")
    metadata["track_id"] = metadata["track_id"].astype("int32")
    metadata["playlist_count"] = metadata["playlist_count"].astype("int32")
    metadata["artist_rowid"] = metadata["artist_rowid"].astype("int64")
    metadata["album_rowid"] = metadata["album_rowid"].astype("int64")
    return metadata


def validate_metadata_coverage(vocab: pd.DataFrame, metadata: pd.DataFrame) -> None:
    """Raise if the join result does not cover exactly the expected track_rowids.

    Checks for duplicates, missing rows, and unexpected extra rows.
    """
    expected = pd.Index(vocab["track_rowid"])
    actual = pd.Index(metadata["track_rowid"])

    duplicates = metadata["track_rowid"][metadata["track_rowid"].duplicated()]
    if not duplicates.empty:
        preview = ", ".join(map(str, duplicates.head(5).tolist()))
        raise RuntimeError(f"Duplicate metadata rows for track_rowid(s): {preview}")

    missing = expected.difference(actual)
    if not missing.empty:
        preview = ", ".join(map(str, missing[:5].tolist()))
        raise RuntimeError(f"Missing metadata rows for track_rowid(s): {preview}")

    extra = actual.difference(expected)
    if not extra.empty:
        preview = ", ".join(map(str, extra[:5].tolist()))
        raise RuntimeError(f"Unexpected metadata rows for track_rowid(s): {preview}")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich the base training vocabulary with metadata ids.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("SICK_TRACKS_DB"),
        help="Path to track SQLite database. $SICK_TRACKS_DB",
    )
    parser.add_argument(
        "--input",
        default=os.environ.get("SICK_T0_VOCAB"),
        help="Base training vocab path. $SICK_T0_VOCAB",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("SICK_T1_VOCAB"),
        help="Output path for enriched training vocab. $SICK_T1_VOCAB",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per temp-table insert batch (default: {CHUNK_SIZE_DEFAULT:,}).",
    )
    args = parser.parse_args()

    if args.database is None:
        raise ValueError("--database / $SICK_TRACKS_DB not set.")
    if args.input is None:
        raise ValueError("--input / $SICK_T0_VOCAB not set.")
    if args.output is None:
        raise ValueError("--output / $SICK_T1_VOCAB not set.")

    db_path = Path(args.database)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Base training vocab not found: {input_path}")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database  : {db_path}")
    print(f"Input     : {input_path}")
    print(f"Output    : {output_path}")
    print(f"Chunk size: {args.chunk_size:,} rows")
    print()

    t0 = time.time()
    print("Loading base training vocab...")
    vocab = pd.read_parquet(input_path, columns=["track_rowid", "track_id", "playlist_count"])
    vocab["track_rowid"] = vocab["track_rowid"].astype("int64")
    vocab["track_id"] = vocab["track_id"].astype("int32")
    vocab["playlist_count"] = vocab["playlist_count"].astype("int32")
    print(f"  {len(vocab):,} rows loaded  ({time.time() - t0:.1f}s)")

    print("Staging base vocab in SQLite temp table...")
    conn = get_connection(db_path)
    create_temp_vocab_table(conn)
    load_temp_vocab_table(conn, vocab, args.chunk_size)

    print("Fetching metadata through temp-table join...")
    metadata = fetch_joined_metadata(conn)
    conn.close()
    validate_metadata_coverage(vocab, metadata)
    print(f"  {len(metadata):,} metadata rows validated")

    print("Assigning deterministic label ids...")
    label_rowids = assign_label_rowids(metadata["label"])
    print(f"  {len(label_rowids):,} non-empty labels")
    print()

    metadata["label_rowid"] = metadata["label"].map(label_rowids).astype("Int32")
    enriched = metadata[
        [
            "track_rowid",
            "track_id",
            "playlist_count",
            "artist_rowid",
            "album_rowid",
            "label_rowid",
            "id_isrc",
        ]
    ].copy()
    enriched["artist_rowid"] = enriched["artist_rowid"].astype("int64")
    enriched["album_rowid"] = enriched["album_rowid"].astype("int64")

    print("Writing enriched training vocab...")
    table = pa.Table.from_pandas(enriched, schema=SCHEMA, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  {len(enriched):,} written")
    print(f"\nDone in {time.time() - t0:.1f}s")
    print(f"Output : {output_path}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
