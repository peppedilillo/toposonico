#!/usr/bin/env python3
"""Build a track lookup table from the metadata SQLite database.

Writes a parquet keyed by track_rowid with display columns: track_name,
artist_rowid, artist_name, album_rowid, album_name, track_popularity,
release_date, id_isrc, label.
Used for inspecting nearest-neighbour results. Pass --vocab to restrict output
to tracks that appear in playlists (~47M); omit to write everything.

Usage:
    python scripts/build_track_lookup.py [--database DB] [--output OUTPUT]
                                          [--vocab VOCAB] [--chunk-size N]

Example:
    python scripts/build_track_lookup.py --vocab training_vocab.parquet
"""

import argparse
from pathlib import Path
import sqlite3
import time
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_SIZE_DEFAULT = 500_000

QUERY = """
    SELECT
        t.rowid            AS track_rowid,
        t.name             AS track_name,
        t.popularity       AS track_popularity,
        t.external_id_isrc AS id_isrc,
        ta.artist_rowid    AS artist_rowid,
        a.name             AS artist_name,
        t.album_rowid      AS album_rowid,
        al.name            AS album_name,
        al.label           AS label,
        al.release_date    AS release_date
    FROM tracks AS t
    INNER JOIN albums        AS al ON t.album_rowid   = al.rowid
    INNER JOIN track_artists AS ta ON t.rowid         = ta.track_rowid
    INNER JOIN artists       AS a  ON ta.artist_rowid = a.rowid
    WHERE ta.artist_rowid = (
        SELECT artist_rowid FROM track_artists WHERE track_rowid = t.rowid LIMIT 1
    )
"""

SCHEMA = pa.schema(
    [
        pa.field("track_rowid", pa.int64()),
        pa.field("track_name", pa.string()),
        pa.field("track_popularity", pa.uint8()),
        pa.field("id_isrc", pa.string()),
        pa.field("artist_rowid", pa.int64()),
        pa.field("artist_name", pa.string()),
        pa.field("album_rowid", pa.int64()),
        pa.field("album_name", pa.string()),
        pa.field("label", pa.string()),
        pa.field("release_date", pa.string()),
        pa.field("logcounts", pa.float32()),
    ]
)


def get_connection(database_path: Path) -> sqlite3.Connection:
    uri = f"file:{database_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def main():
    parser = argparse.ArgumentParser(
        description="Build track lookup table from metadata database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("T2M_TRACKS_DB"),
        help="Path to track SQLite database. Set to `T2M_TRACKS_DB` by default.",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("T2M_TRACK_LOOKUP"),
        help="Output parquet path. Set to `T2M_TRACK_LOOKUP` by default.",
    )
    parser.add_argument(
        "--vocab",
        default=os.environ.get("T2M_TRAINING_VOCAB"),
        help="Global track vocab parquet — if given, only tracks whose track_rowid appears "
        "in the vocab are written; all others are skipped. Omit to write all rows. "
        "Set to `T2M_TRAINING_VOCAB` by default.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per chunk for streaming write (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    if args.database is None:
        raise ValueError(
            "No `T2M_TRACKS_DB` environment variable set. "
            "Either run with --database argument or define the environment variable."
        )
    db_path = Path(args.database)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    if args.output is None:
        raise ValueError(
            "No `T2M_TRACK_LOOKUP` environment variable set. "
            "Either run with --output argument or define the environment variable."
        )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.vocab is None:
        raise FileNotFoundError(f"Vocab parquet not found: {args.vocab}\n")
    vocab_path = Path(args.vocab)
    if not vocab_path.exists():
        raise FileNotFoundError(f"Track vocab not found: {vocab_path}")

    print(f"Database  : {db_path}")
    print(f"Vocab     : {vocab_path or '(none — no filtering)'}")
    print(f"Output    : {output_path}")
    print(f"Chunk size: {args.chunk_size:,} rows")
    print()

    print("Loading vocab for filtering...")
    vocab = pd.read_parquet(vocab_path, columns=["track_rowid", "playlist_count"])
    vocab_ids = pd.Index(vocab["track_rowid"])
    vocab_counts = vocab.set_index("track_rowid")["playlist_count"]
    print(f"  {len(vocab_ids):,} track_rowids loaded")

    print()

    print("Querying tracks (streaming in chunks)...")
    print("(This may take a while on a large database.)")

    conn = get_connection(db_path)
    cursor = conn.execute(QUERY)
    col_names: list[str] = [desc[0] for desc in cursor.description]

    t0 = time.time()
    total_rows = 0
    total_skipped = 0
    rate = 0.0

    with pq.ParquetWriter(output_path, SCHEMA) as writer:
        while True:
            rows = cursor.fetchmany(args.chunk_size)
            if not rows:
                break

            chunk = pd.DataFrame.from_records(rows, columns=col_names)
            chunk["track_rowid"] = chunk["track_rowid"].astype("int64")

            mask = chunk["track_rowid"].isin(
                vocab_ids
            )  # pyright: ignore[reportArgumentType]
            total_skipped += int((~mask).sum())
            chunk = chunk.loc[mask]

            if not chunk.empty:
                chunk["track_popularity"] = (
                    chunk["track_popularity"].fillna(0).astype("uint8")
                )
                chunk["logcounts"] = np.log10(
                    chunk["track_rowid"].map(vocab_counts)
                ).astype("float32")
                writer.write_table(
                    pa.Table.from_pandas(chunk, schema=SCHEMA, preserve_index=False)
                )

            total_rows += len(chunk)
            elapsed = time.time() - t0
            rate = total_rows / elapsed if elapsed > 0 else 0.0
            print(
                f"  {total_rows:>10,} written  {total_skipped:>10,} skipped  ({rate:,.0f} rows/s)",
                end="\r",
            )

    conn.close()
    elapsed = time.time() - t0

    size_mb = output_path.stat().st_size / 1_048_576
    print(
        f"\n  {total_rows:,} written  {total_skipped:,} skipped  ({rate:,.0f} rows/s)"
    )
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output : {output_path}  ({size_mb:.1f} MB)")
    if vocab_ids is not None:
        print(
            f"Rows   : {total_rows:,}  ({100 * total_rows / len(vocab_ids):.1f}% of vocab covered)"
        )
    else:
        print(f"Rows   : {total_rows:,}")
    if total_rows:
        print(f"Bytes/row (compressed): {size_mb * 1_048_576 / total_rows:.0f}")


if __name__ == "__main__":
    main()
