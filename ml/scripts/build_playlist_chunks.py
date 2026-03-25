#!/usr/bin/env python3
"""Export playlist_tracks to parquet chunks for full-scale training.

Streams playlist_tracks in batches of N playlists (filtered: no episodes, no
local files, no null track_rowids) and writes one parquet chunk per batch.
Chunks store raw track_rowids — remapping to track_id happens at training time
via training_vocab.parquet, keeping chunks valid across vocab choices.

--offset slices the playlist list with Python semantics before chunking:
0 = all, K > 0 = skip first K, -K = last K only.

Usage:
    python scripts/build_playlist_chunks.py output_dir [--database DB]
                                             [--chunk-size N] [--offset K]
                                             [--overwrite]

Examples:
    python scripts/build_playlist_chunks.py ./chunks --chunk-size 100000

    python scripts/build_playlist_chunks.py ./mini_chunks \\
        --offset -40000 --chunk-size 15000
"""

import argparse
from datetime import datetime
from datetime import timezone
import json
import os
from pathlib import Path
import sqlite3
import time

import pandas as pd

PLAYLIST_ROWIDS_QUERY = "SELECT rowid FROM playlists ORDER BY rowid"


CHUNK_QUERY = """
    SELECT
        playlist_rowid,
        track_rowid,
        position,
        added_at,
        added_by_id
    FROM playlist_tracks
    WHERE playlist_rowid BETWEEN ? AND ?
      AND is_episode = 0
      AND is_local = 0
      AND track_rowid IS NOT NULL
"""


def get_connection(database_path: Path) -> sqlite3.Connection:
    uri = f"file:{database_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def main():
    parser = argparse.ArgumentParser(
        description="Export playlist_tracks to parquet chunks for training",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("output_dir", type=Path, help="Output directory for chunks")
    parser.add_argument(
        "--database",
        default=os.environ.get("SICK_PLAYLIST_DB"),
        help="Path to playlist SQLite database. Set to `SICK_PLAYLIST_DB` by default.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Number of playlists per chunk (default: 100,000)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help=(
            "Slice the playlist list before chunking using Python semantics. "
            "0 (default) = all playlists; K > 0 = skip first K; -K = last K only. "
            "Valid range: -(n-1) to (n-1)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing chunk files (default: skip existing)",
    )
    args = parser.parse_args()

    if args.database is None:
        raise ValueError(
            "No `SICK_PLAYLIST_DB` environment variable set. "
            "Either run with --database argument or define the environment variable."
        )
    db_path = Path(args.database)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection(db_path)

    print(f"Database   : {db_path}")
    print(f"Output dir : {output_dir}")
    print(f"Chunk size : {args.chunk_size:,} playlists")
    print(f"Offset     : {args.offset}")
    print()
    print("Fetching playlist rowids...")
    all_playlist_rowids = pd.read_sql_query(PLAYLIST_ROWIDS_QUERY, conn)[
        "rowid"
    ].tolist()
    n = len(all_playlist_rowids)

    if args.offset != 0 and not (-(n - 1) <= args.offset <= n - 1):
        raise ValueError(
            f"--offset {args.offset} out of range; valid range is [{-(n-1)}, {n-1}] "
            f"for {n:,} playlists."
        )

    playlist_rowids = (
        all_playlist_rowids[args.offset :] if args.offset != 0 else all_playlist_rowids
    )
    total_playlists = len(playlist_rowids)

    batches = [
        playlist_rowids[i : i + args.chunk_size]
        for i in range(0, total_playlists, args.chunk_size)
    ]
    total_chunks = len(batches)

    if args.offset != 0:
        print(
            f"  {n:,} playlists in DB, {total_playlists:,} selected (offset={args.offset}) → {total_chunks:,} chunks"
        )
    else:
        print(f"  {total_playlists:,} playlists → {total_chunks:,} chunks")
    print()

    w = len(str(total_chunks))
    t0 = time.time()
    total_rows_written = 0
    skipped = 0

    for i, batch in enumerate(batches):
        out_path = output_dir / f"chunk_{i:06d}.parquet"

        if out_path.exists() and not args.overwrite:
            skipped += 1
            continue

        lo, hi = batch[0], batch[-1]
        df = pd.read_sql_query(CHUNK_QUERY, conn, params=[lo, hi])

        df["playlist_rowid"] = df["playlist_rowid"].astype("int32")
        df["track_rowid"] = df["track_rowid"].astype("int64")
        df["position"] = df["position"].astype("Int16")
        df["added_at"] = df["added_at"].astype("Int64")
        # added_by_id: keep as string (may be null)

        df.to_parquet(out_path, index=False)
        total_rows_written += len(df)

        elapsed = time.time() - t0
        print(
            f"  [{i+1:{w}}/{total_chunks}] {out_path.name}"
            f"  {len(df):>10,} rows"
            f"  {elapsed:>6.0f}s elapsed"
        )

    conn.close()

    if skipped:
        print(
            f"\n  {skipped} chunk(s) skipped (already exist). Use --overwrite to regenerate."
        )

    manifest = {
        "total_chunks": total_chunks,
        "playlists_per_chunk": args.chunk_size,
        "total_playlists": total_playlists,
        "offset": args.offset,
        "columns": [
            "playlist_rowid",
            "track_rowid",
            "position",
            "added_at",
            "added_by_id",
        ],
        "filters": "is_episode=0, is_local=0, track_rowid IS NOT NULL",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"\nManifest written to {manifest_path}")
    written = total_chunks - skipped
    print(
        f"Done in {time.time() - t0:.0f}s  —  {written} chunk(s) written, {skipped} skipped  |  {total_rows_written:,} rows written"
    )


if __name__ == "__main__":
    main()
