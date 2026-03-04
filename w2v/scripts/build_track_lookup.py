#!/usr/bin/env python3
"""Build a track lookup table from the metadata SQLite database.

Writes a parquet keyed by track_rowid with display columns: track_name,
artist_rowid, artist_name, album_rowid, album_name, track_popularity,
release_date, id_isrc, label.
Used for inspecting nearest-neighbour results. Pass --vocab to restrict output
to tracks that appear in playlists (~47M); omit to write everything.

Usage:
    python scripts/build_track_lookup.py <db> <output> [--vocab VOCAB] [--chunk-size N]

Example:
    python scripts/build_track_lookup.py spotify_clean.sqlite3 track_lookup.parquet
"""

import argparse
from pathlib import Path
import sqlite3
import time

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
        description="Build track lookup table from metadata database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("database", type=Path, help="Path to metadata SQLite database")
    parser.add_argument("output", type=Path, help="Output parquet path")
    parser.add_argument(
        "--vocab",
        type=Path,
        default=None,
        help="Global track vocab parquet — if given, only tracks whose track_rowid appears "
        "in the vocab are written; all others are skipped. Omit to write all rows.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per chunk for streaming write (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Database not found: {args.database}")
    if args.vocab is not None and not args.vocab.exists():
        raise FileNotFoundError(
            f"Vocab parquet not found: {args.vocab}\n"
            "Run 'python scripts/build_track_vocab.py' first."
        )

    output_path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database  : {args.database}")
    print(f"Vocab     : {args.vocab or '(none — no filtering)'}")
    print(f"Output    : {output_path}")
    print(f"Chunk size: {args.chunk_size:,} rows")
    print()

    vocab_ids: pd.Index | None = None
    if args.vocab is not None:
        print("Loading vocab track_rowids for filtering...")
        vocab_ids = pd.Index(
            pd.read_parquet(args.vocab, columns=["track_rowid"])["track_rowid"]
        )
        print(f"  {len(vocab_ids):,} track_rowids loaded")
        print()

    print("Querying tracks (streaming in chunks)...")
    print("(This may take a while on a large database.)")

    conn = get_connection(args.database)
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

            if vocab_ids is not None:
                mask = chunk["track_rowid"].isin(
                    vocab_ids
                )  # pyright: ignore[reportArgumentType]
                total_skipped += int((~mask).sum())
                chunk = chunk.loc[mask]

            if not chunk.empty:
                chunk["track_popularity"] = (
                    chunk["track_popularity"].fillna(0).astype("uint8")
                )
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
