#!/usr/bin/env python3
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

CHUNK_SIZE_DEFAULT = 100_000

QUERY = """
    SELECT
        t.rowid            AS track_rowid,
        t.external_id_isrc AS id_isrc,
        ta.artist_rowid    AS artist_rowid,
        t.album_rowid      AS album_rowid,
        al.label           AS label
    FROM tracks AS t
    INNER JOIN albums        AS al ON t.album_rowid   = al.rowid
    INNER JOIN track_artists AS ta ON t.rowid         = ta.track_rowid
    WHERE t.rowid IN ({placeholders})
      AND ta.artist_rowid = (
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
    uri = f"file:{database_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def assign_label_rowids(labels: pd.Series) -> dict[str, int]:
    valid = sorted({label for label in labels.dropna().unique() if label != ""})
    return {label: idx for idx, label in enumerate(valid, start=1)}


def chunked_track_rowids(track_rowids: pd.Series, chunk_size: int) -> list[list[int]]:
    values = track_rowids.astype("int64").tolist()
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def fetch_metadata_batch(conn: sqlite3.Connection, track_rowids: list[int]) -> pd.DataFrame:
    placeholders = ",".join("?" * len(track_rowids))
    query = QUERY.format(placeholders=placeholders)
    batch = pd.read_sql_query(query, conn, params=track_rowids)
    if batch.empty:
        return batch
    batch["track_rowid"] = batch["track_rowid"].astype("int64")
    batch["artist_rowid"] = batch["artist_rowid"].astype("int64")
    batch["album_rowid"] = batch["album_rowid"].astype("int64")
    return batch


def validate_metadata_coverage(vocab: pd.DataFrame, metadata: pd.DataFrame) -> None:
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


def fetch_vocab_metadata(
    conn: sqlite3.Connection, vocab: pd.DataFrame, chunk_size: int
) -> pd.DataFrame:
    chunks = []
    total_rows = 0
    started_at = time.time()
    track_chunks = chunked_track_rowids(vocab["track_rowid"], chunk_size)

    for idx, batch_ids in enumerate(track_chunks, start=1):
        chunk = fetch_metadata_batch(conn, batch_ids)
        chunks.append(chunk)
        total_rows += len(chunk)
        elapsed = time.time() - started_at
        rate = total_rows / elapsed if elapsed > 0 else 0.0
        print(
            f"  [{idx:>5}/{len(track_chunks)}] {total_rows:>10,} rows fetched  ({rate:,.0f} rows/s)",
            end="\r",
        )

    print()
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description="Enrich the base training vocabulary with metadata ids",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("SICK_TRACKS_DB"),
        help="Path to track SQLite database. Set to `SICK_TRACKS_DB` by default.",
    )
    parser.add_argument(
        "--input",
        default=os.environ.get("SICK_TRAINING_VOCAB_BASE")
        or os.environ.get("SICK_TRAINING_VOCAB"),
        help="Base training vocab path. Defaults to `SICK_TRAINING_VOCAB_BASE`, "
        "falling back to `SICK_TRAINING_VOCAB`.",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("SICK_TRAINING_VOCAB"),
        help="Output path for enriched training vocab. Set to `SICK_TRAINING_VOCAB` by default.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Track ids per SQL batch query (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    if args.database is None:
        raise ValueError(
            "No `SICK_TRACKS_DB` environment variable set. "
            "Either run with --database argument or define the environment variable."
        )
    if args.input is None:
        raise ValueError(
            "No base training vocab path set. Use --input or set "
            "`SICK_TRAINING_VOCAB_BASE` / `SICK_TRAINING_VOCAB`."
        )
    if args.output is None:
        raise ValueError(
            "No output path set. Use --output or set `SICK_TRAINING_VOCAB`."
        )

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

    print("Fetching metadata for vocab track ids...")
    conn = get_connection(db_path)
    metadata = fetch_vocab_metadata(conn, vocab, args.chunk_size)
    conn.close()
    validate_metadata_coverage(vocab, metadata)
    print(f"  {len(metadata):,} metadata rows validated")

    print("Assigning deterministic label ids...")
    label_rowids = assign_label_rowids(metadata["label"])
    print(f"  {len(label_rowids):,} non-empty labels")
    print()

    metadata["label_rowid"] = metadata["label"].map(label_rowids).astype("Int32")
    enriched = vocab.merge(metadata, on="track_rowid", how="left", validate="one_to_one")
    enriched = enriched[
        [
            "track_rowid",
            "track_id",
            "playlist_count",
            "artist_rowid",
            "album_rowid",
            "label_rowid",
            "id_isrc",
        ]
    ]
    enriched["track_rowid"] = enriched["track_rowid"].astype("int64")
    enriched["track_id"] = enriched["track_id"].astype("int32")
    enriched["playlist_count"] = enriched["playlist_count"].astype("int32")
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
