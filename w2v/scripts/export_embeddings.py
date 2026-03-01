#!/usr/bin/env python3
"""Export track embeddings from a trained checkpoint to parquet.

Writes a parquet with two columns: track_rowid (int64) and embedding
(fixed-size list of float32, length D).

Downstream usage:
    df     = pd.read_parquet("embeddings.parquet")
    matrix = np.stack(df["embedding"].values)   # (V, D) float32

The full embedding tensor is loaded into RAM (unavoidable). Writing is done
in chunks to 1. avoid a second full copy when building the PyArrow table; and
2. provide progress update status.

Usage:
    python scripts/export_embeddings.py <checkpoint> <output> [--chunk-size N]

Example:
    python scripts/export_embeddings.py models/vivid_dragon/model.pt embeddings.parquet
"""

import argparse
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch


CHUNK_SIZE_DEFAULT = 500_000


def main():
    parser = argparse.ArgumentParser(
        description="Export track embeddings from checkpoint to parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("checkpoint", type=Path, help="Path to .pt checkpoint file")
    parser.add_argument("output", type=Path, help="Output parquet path")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per parquet row group (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    if not args.checkpoint.exists():
        raise FileNotFoundError(f"Checkpoint not found: {args.checkpoint}")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(f"Checkpoint : {args.checkpoint}")
    print(f"Output     : {args.output}")
    print(f"Chunk size : {args.chunk_size:,}")
    print()

    print("Loading checkpoint...")
    t0 = time.time()
    ckpt = torch.load(args.checkpoint, map_location="cpu", weights_only=False)
    emb = ckpt["model_state_dict"]["embeddings_in.weight"].numpy()  # zero-copy, (V, D)
    rowids = np.asarray(ckpt["vocab"]["track_rowid"], dtype=np.int64)
    embed_dim = ckpt["hparams"]["embed_dim"]
    del ckpt
    print(f"  Loaded in {time.time() - t0:.1f}s  —  vocab {len(rowids):,}, dim {embed_dim}")

    schema = pa.schema([
        pa.field("track_rowid", pa.int64()),
        pa.field("embedding", pa.list_(pa.float32(), embed_dim)),
    ])

    vocab_size = len(rowids)
    total_written = 0
    rate = 0.0
    t1 = time.time()

    print("Writing parquet...")
    with pq.ParquetWriter(args.output, schema) as writer:
        for lo in range(0, vocab_size, args.chunk_size):
            hi = min(lo + args.chunk_size, vocab_size)
            chunk_emb = pa.FixedSizeListArray.from_arrays(
                pa.array(emb[lo:hi].ravel(), type=pa.float32()), embed_dim
            )
            writer.write_table(pa.table({
                "track_rowid": pa.array(rowids[lo:hi], type=pa.int64()),
                "embedding":   chunk_emb,
            }))
            total_written += hi - lo
            elapsed = time.time() - t1
            rate = total_written / elapsed if elapsed > 0 else 0.0
            print(f"  {total_written:>10,} / {vocab_size:,}  ({rate:,.0f} rows/s)", end="\r")

    elapsed = time.time() - t0
    size_mb = args.output.stat().st_size / 1_048_576
    print(f"  {total_written:,} rows written  ({rate:,.0f} rows/s)      ")
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output : {args.output}  ({size_mb:.1f} MB)")
    print(f"Schema : track_rowid int64, embedding float32[{embed_dim}]")


if __name__ == "__main__":
    main()
