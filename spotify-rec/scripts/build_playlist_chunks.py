#!/usr/bin/env python3
"""Export playlist_tracks to parquet chunks for full-scale training.

Streams the full playlist_tracks table in batches of N playlists, applying the
standard base filters (no episodes, no local files, no null track_rowids). Each
chunk is a self-contained parquet file holding all (playlist, track) pairs for
that batch of playlists, with extra columns retained for future data-quality work.

No track_rowid → track_id remapping is done here. Chunks store raw track_rowids
so they remain valid across different training vocab choices (min_count thresholds).
Remapping and vocab filtering happen at training time using training_vocab.parquet.

No deduplication is applied. Duplicate (playlist_rowid, track_rowid) pairs from
the same track being added twice to a playlist are rare and produce negligible
extra training pairs.

The number of distinct playlists per chunk may be slightly lower than --chunk-size:
playlists whose tracks are all episodes, local files, or have null track_rowids are
included in the rowid batch but contribute no rows to the output.


Usage:
    python scripts/build_playlist_chunks.py <database> [-o OUTPUT_DIR]
                                             [--chunk-size N] [--offset K]
                                             [--overwrite]

The --offset argument slices the playlist list with Python semantics before
chunking: offset=0 (default) includes all playlists; offset=K > 0 skips the
first K playlists; offset=-K includes only the last K playlists. Valid range:
-(n-1) to (n-1), where n is the total number of playlists.

Example — build a small two-chunk mini-library from the last 32,000 playlists:
    python scripts/build_playlist_chunks.py \\
        ~/HDD/Datasets/annas_archive_spotify_2025_07/spotify_clean_playlists.sqlite3 \\
        -o data/playlist/mini_chunks --offset -32000 --chunk-size 28000
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.db import get_connection


OUTPUT_DIR = Path(__file__).parent.parent / "data" / "playlist" / "chunks"

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


def main():
    parser = argparse.ArgumentParser(
        description="Export playlist_tracks to parquet chunks for training",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("database", type=Path, help="Path to playlist SQLite database")
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        help="Output directory for chunks (default: data/playlist/chunks/)",
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

    if not args.database.exists():
        raise FileNotFoundError(f"Database not found: {args.database}")

    output_dir = args.output_dir or OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection(args.database)

    print(f"Database   : {args.database}")
    print(f"Output dir : {output_dir}")
    print(f"Chunk size : {args.chunk_size:,} playlists")
    print(f"Offset     : {args.offset}")
    print()
    print("Fetching playlist rowids...")
    all_playlist_rowids = pd.read_sql_query(PLAYLIST_ROWIDS_QUERY, conn)["rowid"].tolist()
    n = len(all_playlist_rowids)

    if args.offset != 0 and not (-(n - 1) <= args.offset <= n - 1):
        raise ValueError(
            f"--offset {args.offset} out of range; valid range is [{-(n-1)}, {n-1}] "
            f"for {n:,} playlists."
        )

    playlist_rowids = all_playlist_rowids[args.offset:] if args.offset != 0 else all_playlist_rowids
    total_playlists = len(playlist_rowids)

    batches = [
        playlist_rowids[i : i + args.chunk_size]
        for i in range(0, total_playlists, args.chunk_size)
    ]
    total_chunks = len(batches)

    if args.offset != 0:
        print(f"  {n:,} playlists in DB, {total_playlists:,} selected (offset={args.offset}) → {total_chunks:,} chunks")
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
        print(f"\n  {skipped} chunk(s) skipped (already exist). Use --overwrite to regenerate.")

    manifest = {
        "total_chunks": total_chunks,
        "playlists_per_chunk": args.chunk_size,
        "total_playlists": total_playlists,
        "offset": args.offset,
        "columns": ["playlist_rowid", "track_rowid", "position", "added_at", "added_by_id"],
        "filters": "is_episode=0, is_local=0, track_rowid IS NOT NULL",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"\nManifest written to {manifest_path}")
    written = total_chunks - skipped
    print(f"Done in {time.time() - t0:.0f}s  —  {written} chunk(s) written, {skipped} skipped  |  {total_rows_written:,} rows written")


if __name__ == "__main__":
    main()
