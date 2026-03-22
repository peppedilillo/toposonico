#!/usr/bin/env python3
"""Deduplicate a track embedding parquet by ISRC.

For each group of track_rowids sharing the same ISRC, keeps only the one with
the highest logcounts (playlist appearances). Tracks with no ISRC are left
untouched. Writes a new parquet with the same schema as the input.

Runs after export_embeddings.py and before umap.ipynb / build_knn.py.

Usage:
    python scripts/dedup_embeddings.py EMBEDDING [--track-lookup PATH] [--output PATH]

Example:
    python scripts/dedup_embeddings.py \\
        outs/embedding_track_silent_whale_unfiltered.parquet \\
        --track-lookup outs/track_lookup.parquet \\
        --output outs/embedding_track_silent_whale.parquet
"""

import argparse
import os
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


CHUNK_SIZE = 500_000


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate track embeddings by ISRC, keeping highest-logcounts entry",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "embedding",
        help="Path to the unfiltered embedding parquet (track_rowid + e0…eD).",
    )
    parser.add_argument(
        "--track-lookup",
        default=os.environ.get("T2M_TRACK_LOOKUP"),
        help="Path to track_lookup.parquet (for id_isrc + logcounts). $T2M_TRACK_LOOKUP",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("T2M_EMBEDDING"),
        help="Output parquet path. $T2M_EMBEDDING",
    )
    args = parser.parse_args()

    if args.track_lookup is None:
        raise ValueError(
            "No track-lookup path set. Use --track-lookup or set $T2M_TRACK_LOOKUP."
        )
    if args.output is None:
        raise ValueError(
            "No output path set. Use --output or set $T2M_EMBEDDING."
        )

    embedding_path = Path(args.embedding)
    lookup_path = Path(args.track_lookup)
    output_path = Path(args.output)

    if not embedding_path.exists():
        raise FileNotFoundError(f"Embedding not found: {embedding_path}")
    if not lookup_path.exists():
        raise FileNotFoundError(f"Track lookup not found: {lookup_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Embedding  : {embedding_path}")
    print(f"Lookup     : {lookup_path}")
    print(f"Output     : {output_path}")
    print()

    t0 = time.time()

    print("Loading lookup...")
    lookup = pd.read_parquet(lookup_path, columns=["track_rowid", "id_isrc", "logcounts"])
    print(f"  {len(lookup):,} rows")

    print("Scanning embedding for vocab size...")
    pf = pq.ParquetFile(embedding_path)
    # read only track_rowid column to get vocab and embed_dim without loading embeddings
    rowids_table = pf.read(columns=["track_rowid"])
    all_rowids = rowids_table.column("track_rowid").to_pylist()
    embed_dim = len([n for n in pf.schema_arrow.names if n.startswith("e")])
    n_total = len(all_rowids)
    del rowids_table
    print(f"  {n_total:,} tracks, {embed_dim} dims")

    print("Deduplicating by ISRC...")
    emb_rowids = set(all_rowids)
    del all_rowids
    sub = lookup[lookup["track_rowid"].isin(emb_rowids)]

    valid = sub[sub["id_isrc"].notna() & (sub["id_isrc"] != "")]
    no_isrc = sub[sub["id_isrc"].isna() | (sub["id_isrc"] == "")]

    keep = valid.sort_values("logcounts", ascending=False).drop_duplicates("id_isrc", keep="first")
    n_dup_removed = len(valid) - len(keep)

    keep_rowids = set(keep["track_rowid"]) | set(no_isrc["track_rowid"])
    n_after = len(keep_rowids)

    print(f"  with ISRC    : {len(valid):,} tracks, {valid['id_isrc'].nunique():,} distinct ISRCs, {n_dup_removed:,} duplicates removed")
    print(f"  without ISRC : {len(no_isrc):,} tracks (kept as-is)")
    print(f"  Before: {n_total:,}  →  After: {n_after:,}  (-{n_total - n_after:,})")
    del sub, valid, no_isrc, keep, lookup

    schema = pf.schema_arrow

    print("Writing parquet...")
    written = 0
    with pq.ParquetWriter(output_path, schema) as writer:
        for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
            tbl = pa.Table.from_batches([batch])
            rowids = tbl.column("track_rowid").to_pylist()
            mask = pa.array([r in keep_rowids for r in rowids], type=pa.bool_())
            tbl = tbl.filter(mask)
            writer.write_table(tbl)
            written += len(tbl)
            print(f"  {written:>10,} / {n_after:,}", end="\r")

    elapsed = time.time() - t0
    size_mb = output_path.stat().st_size / 1_048_576
    print(f"  {written:,} rows written              ")
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Output : {output_path}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
