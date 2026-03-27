#!/usr/bin/env python3
"""Precompute K-nearest-neighbor tables for all entity types.

Builds 2 parquets per entity (neighbors and scores) using cosine KNN via FAISS
(CPU). Each entity embedding parquet is loaded and processed independently.
Uses IndexIVFFlat for N > 500K and IndexFlatIP for smaller entities.
Avoids VRAM entirely — safe on constrained GPUs.

Output (k+1 neighbors per row — may include self-match, filter at serve time):
    outs/knn/knn_track.parquet          — track_rowid, n0..n100
    outs/knn/knn_scores_track.parquet   — track_rowid, s0..s100
    outs/knn/knn_album.parquet          — album_rowid, n0..n50
    outs/knn/knn_scores_album.parquet   — album_rowid, s0..s50
    outs/knn/knn_artist.parquet         — artist_rowid, n0..n20
    outs/knn/knn_scores_artist.parquet  — artist_rowid, s0..s20
    outs/knn/knn_label.parquet          — label, n0..n10
    outs/knn/knn_scores_label.parquet   — label, s0..s10

n* = neighbor rowid/key (int64 or utf8), s* = cosine similarity (float32).

Usage:
    uv run python scripts/build_knn.py [options]

Examples:
    # smoke test on small entity first
    source config.env && uv run python scripts/build_knn.py --entities artist

    # full run (tracks ~10-20 min on CPU with IVF)
    source config.env && uv run python scripts/build_knn.py
"""

import argparse
import os
import time
from pathlib import Path

import faiss
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils import get_auxpaths, read_manifest


IVF_THRESHOLD = 500_000  # use IVFFlat above this, FlatIP below
IVF_NLIST_FACTOR = 4  # nlist = IVF_NLIST_FACTOR * sqrt(N), clamped to [256, 32768]


ENTITIES = {
    "track":  "track_rowid",
    "album":  "album_rowid",
    "artist": "artist_rowid",
    "label":  "label",
}


INDEX_MIN_LEN = 256
INDEX_MAX_LEN = 32768

def _knn_search(matrix: np.ndarray, k: int, batch_size: int, nprobe: int):
    """Cosine KNN over the full matrix via FAISS (CPU). Normalises in-place.

    Selects index type based on N: IVFFlat for N > IVF_THRESHOLD (approximate,
    fast), FlatIP otherwise (exact). Yields one batch at a time to avoid
    materialising the full (N, k+1) result array in RAM.

    Args:
        matrix:     (N, D) float32 embedding matrix. Modified in-place (L2-normalised).
        k:          number of neighbours to return (k+1 columns yielded to allow
                    self-match removal downstream).
        batch_size: number of query rows per FAISS search call.
        nprobe:     IVF nprobe — cells visited per query; ignored for FlatIP.

    Yields:
        (I, D_batch): row-index matrix (batch, k+1) int64 and cosine-similarity
        matrix (batch, k+1) float32. Results may include self-match.
    """
    N, D = matrix.shape
    mat = matrix  # normalise in-place (caller does not reuse matrix after this)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat /= norms

    if N > IVF_THRESHOLD:
        nlist = int(np.clip(IVF_NLIST_FACTOR * N**0.5, INDEX_MIN_LEN, INDEX_MAX_LEN))
        quantizer = faiss.IndexFlatIP(D)
        index = faiss.IndexIVFFlat(quantizer, D, nlist, faiss.METRIC_INNER_PRODUCT)
        train_n = min(N, 256 * nlist)
        if train_n < N:
            train_mat = mat[np.random.choice(N, train_n, replace=False)]
            print(f"  training IVF index (nlist={nlist}, subsample={train_n:,}/{N:,})...")
        else:
            train_mat = mat
            print(f"  training IVF index (nlist={nlist})...")
        index.train(train_mat)
        index.nprobe = nprobe
    else:
        index = faiss.IndexFlatIP(D)

    index.add(mat)

    t0 = time.time()
    done = 0
    for start in range(0, N, batch_size):
        chunk = mat[start : start + batch_size]
        D_batch, I = index.search(chunk, k + 1)
        B = len(chunk)
        done += B
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0.0
        print(f"  {done:>10,} / {N:,}  ({rate:,.0f} rows/s)", end="\r")
        yield I, D_batch

    print()


def _process_entity(
    name: str,
    key_col: str,
    k: int,
    emb_df: pd.DataFrame,
    knn_path: Path,
    score_path: Path,
    batch_size: int,
    nprobe: int,
) -> None:
    """Run KNN search for one entity type and write results to two parquets.

    Extracts the key column and the e0…eD embedding columns from emb_df, runs
    cosine KNN via _knn_search, and streams results to:
      - knn_path   : key_col + n0…n{k}  (neighbor keys, same type as key_col)
      - score_path : key_col + s0…s{k}  (float32 cosine similarities)

    k+1 columns are written per row so the caller can filter self-matches without
    losing a neighbour slot. Key type is utf8 for labels, int64 for all others.

    Args:
        name:       entity name used in log output (e.g. "track").
        key_col:    column name for the entity key (e.g. "track_rowid", "label").
        k:          number of neighbours per entity.
        emb_df:     DataFrame with key_col + e0…eD columns.
        knn_path:   output path for neighbor parquet.
        score_path: output path for scores parquet.
        batch_size: FAISS search batch size (halve if OOM).
        nprobe:     IVF nprobe passed through to _knn_search.
    """
    t0 = time.time()

    keys = emb_df[key_col].to_numpy()
    matrix = emb_df.filter(regex=r"^e\d+$").to_numpy(dtype=np.float32)

    N = len(matrix)
    print(f"\n[{name}]  {N:,} entities, k={k}")

    key_type = pa.utf8() if isinstance(keys[0], str) else pa.int64()
    knn_schema = pa.schema([(key_col, key_type)] + [(f"n{i}", key_type) for i in range(k + 1)])
    score_schema = pa.schema([(key_col, key_type)] + [(f"s{i}", pa.float32()) for i in range(k + 1)])

    start = 0
    with pq.ParquetWriter(knn_path, knn_schema) as knn_w, \
         pq.ParquetWriter(score_path, score_schema) as score_w:
        for I, D_batch in _knn_search(matrix, k, batch_size, nprobe):
            B = len(I)
            batch_keys = keys[start : start + B]
            neighbor_batch_keys = keys[I]  # (B, k+1)

            knn_row = {key_col: batch_keys}
            for i in range(k + 1):
                knn_row[f"n{i}"] = neighbor_batch_keys[:, i]
            knn_w.write_table(pa.table(knn_row, schema=knn_schema))

            score_row = {key_col: batch_keys}
            for i in range(k + 1):
                score_row[f"s{i}"] = D_batch[:, i]
            score_w.write_table(pa.table(score_row, schema=score_schema))

            start += B
    del matrix

    elapsed = time.time() - t0
    rate = N / elapsed if elapsed > 0 else 0.0
    for p in [knn_path, score_path]:
        print(f"  → {p}")
    print(f"  {name:<8} {N:>10,} rows  ({elapsed:.1f}s, {rate:,.0f} rows/s)")


def main():
    parser = argparse.ArgumentParser(
        description="Precompute KNN tables for all entity types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--manifest",
        default=os.environ.get("SICK_MANIFEST"),
        help="Path to ml manifest TOML. $SICK_MANIFEST",
    )
    parser.add_argument("--k-track",  type=int, default=os.environ.get("SICK_K_TRACK"),  help="Neighbors per track. $SICK_K_TRACK")
    parser.add_argument("--k-album",  type=int, default=os.environ.get("SICK_K_ALBUM"),  help="Neighbors per album. $SICK_K_ALBUM")
    parser.add_argument("--k-artist", type=int, default=os.environ.get("SICK_K_ARTIST"), help="Neighbors per artist. $SICK_K_ARTIST")
    parser.add_argument("--k-label",  type=int, default=os.environ.get("SICK_K_LABEL"),  help="Neighbors per label. $SICK_K_LABEL")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=65536,
        help="Query chunk size (halve if OOM)",
    )
    parser.add_argument(
        "--nprobe",
        type=int,
        default=64,
        help="IVF nprobe — higher = more accurate, slower (default: 64)",
    )
    args = parser.parse_args()

    if args.manifest is None:
        raise ValueError("--manifest / $SICK_MANIFEST not set")

    manifest = read_manifest(args.manifest)

    aux = get_auxpaths()
    knn_paths, score_paths = aux["knn"], aux["knn_scores"]

    ks = {
        "track":  args.k_track,
        "album":  args.k_album,
        "artist": args.k_artist,
        "label":  args.k_label,
    }

    for name in ENTITIES:
        if ks[name] is None:
            raise ValueError(f"--k-{name} / $SICK_K_{name.upper()} not set")

    next(iter(knn_paths.values())).parent.mkdir(parents=True, exist_ok=True)

    print(f"Batch size : {args.batch_size:,}")
    print(f"nprobe     : {args.nprobe}")
    print()

    t_total = time.time()
    for name in ENTITIES:
        t0 = time.time()
        print(f"Loading {name} embeddings...")
        emb_df = pd.read_parquet(manifest["embeddings"][name])
        ndim = emb_df.filter(regex=r"^e\d+$").shape[1]
        print(f"  {len(emb_df):,} rows, {ndim}d  ({time.time()-t0:.1f}s)")
        _process_entity(
            name,
            ENTITIES[name],
            ks[name],
            emb_df,
            knn_paths[name],
            score_paths[name],
            args.batch_size,
            args.nprobe,
        )
        del emb_df

    print(f"\nDone in {time.time() - t_total:.1f}s")


if __name__ == "__main__":
    main()
