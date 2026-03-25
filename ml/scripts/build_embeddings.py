#!/usr/bin/env python3
"""Export track embeddings from a trained checkpoint to parquet.

By default, deduplicates by ISRC: for each group of track_rowids sharing the same
ISRC, keeps only the entry with the highest logcounts (playlist appearances). Tracks
with no ISRC are left untouched.

Pass --no-filter to skip deduplication and write all embeddings as-is.

Writes a wide-format parquet: track_rowid (int64) + e0…e{D-1} (float32).

Downstream usage:
    df     = pd.read_parquet("embeddings.parquet")
    matrix = df.filter(like="e").values   # (V, D) float32

Usage:
    python scripts/build_embeddings.py checkpoint --output PATH [--track-lookup PATH]
    python scripts/build_embeddings.py checkpoint --output PATH --no-filter

Examples:
    python scripts/build_embeddings.py models/vivid_dragon/model.pt

    python scripts/build_embeddings.py models/vivid_dragon/model.pt --no-filter \\
        --output outs/vivid_dragon_embeddings_tracks_unfiltered.parquet
"""

import argparse
import os
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch

from src.utils import extract_run_name

CHUNK_SIZE_DEFAULT = 500_000


def main():
    parser = argparse.ArgumentParser(
        description="Export track embeddings from model checkpoint to parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("model", type=Path, help="Path to .pt model checkpoint file")
    parser.add_argument(
        "--output",
        default=os.environ.get("SICK_EMBEDDING_DIR"),
        help="Output parquet path, either a directory or a file name. Set to `SICK_EMBEDDING_DIR` by default.",
    )
    parser.add_argument(
        "--track-lookup",
        default=os.environ.get("SICK_LOOKUP_TRACK"),
        help="Path to track_lookup.parquet (for ISRC deduplication). Set to `SICK_LOOKUP_TRACK` by default.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per parquet row group (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Skip ISRC deduplication; write all embeddings to --output",
    )
    args = parser.parse_args()

    model_path = Path(args.model)
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")

    if args.output is None:
        raise ValueError("No output path set. Use --output or set SICK_EMBEDDING_DIR.")
    output_path = Path(args.output)
    if output_path.is_dir():
        output_path = (
            output_path / f"{extract_run_name(model_path)}_embedding_track.parquet"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.no_filter:
        if args.track_lookup is None:
            raise ValueError(
                "No track-lookup path set. Use --track-lookup or set SICK_LOOKUP_TRACK."
            )
        lookup_path = Path(args.track_lookup)
        if not lookup_path.exists():
            raise FileNotFoundError(f"Track lookup not found: {lookup_path}")

    print(f"Checkpoint : {model_path}")
    print(f"Output     : {output_path}")
    if not args.no_filter:
        print(f"Lookup     : {lookup_path}")
    print(f"Chunk size : {args.chunk_size:,}")
    print()

    t0 = time.time()

    print("Loading checkpoint...")
    ckpt = torch.load(model_path, map_location="cpu", weights_only=False)
    emb = ckpt["model_state_dict"]["embeddings_in.weight"].numpy()  # (V, D) float32
    rowids = np.asarray(ckpt["vocab"]["track_rowid"], dtype=np.int64)
    embed_dim = ckpt["hparams"]["embed_dim"]
    del ckpt
    print(
        f"  Loaded in {time.time() - t0:.1f}s  —  vocab {len(rowids):,}, dim {embed_dim}"
    )

    if not args.no_filter:
        print("Loading lookup...")
        lookup = pd.read_parquet(
            lookup_path, columns=["track_rowid", "id_isrc", "logcounts"]
        )
        print(f"  {len(lookup):,} rows")

        print("Deduplicating by ISRC...")
        emb_rowids_set = set(rowids.tolist())
        sub = lookup[lookup["track_rowid"].isin(emb_rowids_set)]
        del emb_rowids_set

        valid = sub[sub["id_isrc"].notna() & (sub["id_isrc"] != "")]
        no_isrc = sub[sub["id_isrc"].isna() | (sub["id_isrc"] == "")]

        keep = valid.sort_values("logcounts", ascending=False).drop_duplicates(
            "id_isrc", keep="first"
        )
        n_dup_removed = len(valid) - len(keep)

        keep_rowids = set(keep["track_rowid"]) | set(no_isrc["track_rowid"])
        del sub, valid, no_isrc, keep, lookup

        print(f"  Duplicates removed : {n_dup_removed:,}")
        print(
            f"  Before: {len(rowids):,}  →  After: {len(keep_rowids):,}  (-{len(rowids) - len(keep_rowids):,})"
        )

        mask = np.array([r in keep_rowids for r in rowids.tolist()], dtype=bool)
        rowids = rowids[mask]
        emb = emb[mask]
        del mask, keep_rowids

    schema = pa.schema(
        [pa.field("track_rowid", pa.int64())]
        + [pa.field(f"e{i}", pa.float32()) for i in range(embed_dim)]
    )

    vocab_size = len(rowids)
    total_written = 0
    rate = 0.0
    t1 = time.time()

    print("Writing parquet...")
    with pq.ParquetWriter(output_path, schema) as writer:
        for lo in range(0, vocab_size, args.chunk_size):
            hi = min(lo + args.chunk_size, vocab_size)
            chunk_arr = emb[lo:hi]
            row = {"track_rowid": pa.array(rowids[lo:hi], type=pa.int64())}
            for i in range(embed_dim):
                row[f"e{i}"] = pa.array(chunk_arr[:, i], type=pa.float32())
            writer.write_table(pa.table(row, schema=schema))
            total_written += hi - lo
            elapsed = time.time() - t1
            rate = total_written / elapsed if elapsed > 0 else 0.0
            print(
                f"  {total_written:>10,} / {vocab_size:,}  ({rate:,.0f} rows/s)",
                end="\r",
            )

    elapsed = time.time() - t0
    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  {total_written:,} rows written  ({rate:,.0f} rows/s)      ")
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output : {output_path}  ({size_mb:.1f} MB)")
    print(f"Schema : track_rowid int64, e0…e{embed_dim - 1} float32")


if __name__ == "__main__":
    main()
