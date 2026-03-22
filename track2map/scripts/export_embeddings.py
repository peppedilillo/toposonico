#!/usr/bin/env python3
"""Export track embeddings from a trained checkpoint to parquet.

Writes a wide-format parquet: track_rowid (int64) + e0…e{D-1} (float32).

Downstream usage:
    df     = pd.read_parquet("embeddings.parquet")
    matrix = df.filter(like="e").values   # (V, D) float32

The full embedding tensor is loaded into RAM (unavoidable). Writing is done
in chunks to avoid a second full copy when building the PyArrow table.

Usage:
    python scripts/export_embeddings.py checkpoint [--outdir DIR] [--chunk-size N]

Example:
    python scripts/export_embeddings.py models/vivid_dragon/model.pt
"""

import argparse
from pathlib import Path
import time
import os

import numpy as np
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
        "--outdir",
        default=os.environ.get("T2M_EMBEDDING_DIR"),
        help="Output directory path. $T2M_EMBEDDING_DIR",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per parquet row group (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    model_path = Path(args.model)
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")

    if args.outdir is None:
        raise ValueError(
            "No `T2M_EMBEDDING_DIR` environment variable set. "
            "Either run with --outdir argument or define the environment variable."
        )
    outdir_path = Path(args.outdir)
    outdir_path.mkdir(parents=True, exist_ok=True)
    out_path = outdir_path / f"embedding_track_{extract_run_name(model_path)}_unfiltered.parquet"

    print(f"Checkpoint : {model_path}")
    print(f"Output     : {out_path}")
    print(f"Chunk size : {args.chunk_size:,}")
    print()

    print("Loading checkpoint...")
    t0 = time.time()
    ckpt = torch.load(model_path, map_location="cpu", weights_only=False)
    emb = ckpt["model_state_dict"]["embeddings_in.weight"].numpy()  # zero-copy, (V, D)
    rowids = np.asarray(ckpt["vocab"]["track_rowid"], dtype=np.int64)
    embed_dim = ckpt["hparams"]["embed_dim"]
    del ckpt
    print(
        f"  Loaded in {time.time() - t0:.1f}s  —  vocab {len(rowids):,}, dim {embed_dim}"
    )

    schema = pa.schema(
        [pa.field("track_rowid", pa.int64())]
        + [pa.field(f"e{i}", pa.float32()) for i in range(embed_dim)]
    )

    vocab_size = len(rowids)
    total_written = 0
    rate = 0.0
    t1 = time.time()

    print("Writing parquet...")
    with pq.ParquetWriter(out_path, schema) as writer:
        for lo in range(0, vocab_size, args.chunk_size):
            hi = min(lo + args.chunk_size, vocab_size)
            chunk_arr = emb[lo:hi]  # (chunk_size, D)
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
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"  {total_written:,} rows written  ({rate:,.0f} rows/s)      ")
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output : {out_path}  ({size_mb:.1f} MB)")
    print(f"Schema : track_rowid int64, e0…e{embed_dim - 1} float32")


if __name__ == "__main__":
    main()
