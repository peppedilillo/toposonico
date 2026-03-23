#!/usr/bin/env python3
"""Precompute K-nearest-neighbor tables for all entity types.

Builds 2 parquets per entity (neighbors and scores) using cosine KNN via FAISS
(CPU). Track embeddings are loaded once; album/artist embeddings are derived via
mean-pooling (src.topo). Uses IndexIVFFlat for N > 500K (tracks, albums) and
IndexFlatIP for smaller entities. Avoids VRAM entirely — safe on constrained GPUs.

Output (k+1 neighbors per row — may include self-match, filter at serve time):
    outs/knn/track_knn.parquet          — track_rowid, n0..n100
    outs/knn/track_knn_scores.parquet   — track_rowid, s0..s100
    outs/knn/album_knn.parquet          — album_rowid, n0..n50
    outs/knn/album_knn_scores.parquet   — album_rowid, s0..s50
    outs/knn/artist_knn.parquet         — artist_rowid, n0..n20
    outs/knn/artist_knn_scores.parquet  — artist_rowid, s0..s20
    outs/knn/label_knn.parquet          — label, n0..n10
    outs/knn/label_knn_scores.parquet   — label, s0..s10

n* = neighbor rowid/key (int64 or utf8), s* = cosine similarity (float32).

Usage:
    uv run python scripts/build_knn.py [options]

Examples:
    # smoke test on small entity first
    source config.env && uv run python scripts/build_knn.py --entities artist

    # full run (tracks ~10-20 min on CPU with IVF)
    source config.env && uv run python scripts/build_knn.py

    # higher recall (slower)
    source config.env && uv run python scripts/build_knn.py --nprobe 128
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

from src.entities import Artists, Albums, Labels

K_TRACKS = 100
K_ALBUMS = 50
K_ARTISTS = 20
K_LABELS = 10

IVF_THRESHOLD = 500_000  # use IVFFlat above this, FlatIP below
IVF_NLIST_FACTOR = 4  # nlist = IVF_NLIST_FACTOR * sqrt(N), clamped to [256, 32768]
WRITE_CHUNK = 500_000  # rows per parquet write batch (avoids OOM on large DataFrames)


def _write_chunked(path, columns, schema, N):
    """Write columns dict to parquet in WRITE_CHUNK-sized batches."""
    cols = list(columns.keys())
    with pq.ParquetWriter(path, schema) as writer:
        for start in range(0, N, WRITE_CHUNK):
            end = min(start + WRITE_CHUNK, N)
            chunk = {c: columns[c][start:end] for c in cols}
            writer.write_table(pa.table(chunk, schema=schema))


ENTITIES = {
    "track": {"key": "track_rowid", "k": K_TRACKS},
    "album": {"key": "album_rowid", "k": K_ALBUMS},
    "artist": {"key": "artist_rowid", "k": K_ARTISTS},
    "label": {"key": "label", "k": K_LABELS},
}


def _knn_search(
    matrix: np.ndarray, k: int, batch_size: int, nprobe: int
) -> tuple[np.ndarray, np.ndarray]:
    """Cosine KNN via FAISS (CPU).

    Returns (neighbor_idx, neighbor_dist) — both (N, k+1). neighbor_idx contains
    int64 row-indices; neighbor_dist contains float32 cosine similarities (vectors
    are L2-normalized, index uses METRIC_INNER_PRODUCT).

    Results may include self-match — caller should filter by rowid if needed.
    """
    N, D = matrix.shape
    mat = matrix  # to_numpy() already returns a copy; normalise in-place
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat /= norms

    if N > IVF_THRESHOLD:
        nlist = int(np.clip(IVF_NLIST_FACTOR * N**0.5, 256, 32_768))
        quantizer = faiss.IndexFlatIP(D)
        index = faiss.IndexIVFFlat(quantizer, D, nlist, faiss.METRIC_INNER_PRODUCT)
        train_n = min(N, 256 * nlist)
        if train_n < N:
            train_mat = mat[np.random.choice(N, train_n, replace=False)]
            print(
                f"  training IVF index (nlist={nlist}, subsample={train_n:,}/{N:,})..."
            )
        else:
            train_mat = mat
            print(f"  training IVF index (nlist={nlist})...")
        index.train(train_mat)
        index.nprobe = nprobe
    else:
        index = faiss.IndexFlatIP(D)

    index.add(mat)

    neighbor_idx = np.empty((N, k + 1), dtype=np.int64)
    neighbor_dist = np.empty((N, k + 1), dtype=np.float32)
    t0 = time.time()
    done = 0
    for start in range(0, N, batch_size):
        chunk = mat[start : start + batch_size]
        D_batch, I = index.search(chunk, k + 1)
        B = len(chunk)
        neighbor_idx[start : start + B] = I
        neighbor_dist[start : start + B] = D_batch
        done += B
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0.0
        print(f"  {done:>10,} / {N:,}  ({rate:,.0f} rows/s)", end="\r")

    print()
    return neighbor_idx, neighbor_dist


def _process_entity(
    name: str,
    cfg: dict,
    embs_df: pd.DataFrame,
    track_lookup_df: pd.DataFrame,
    output_dir: Path,
    batch_size: int,
    nprobe: int,
) -> None:
    key_col = cfg["key"]
    k = cfg["k"]

    print(f"\n[{name}]  building embeddings...")
    t0 = time.time()

    if name == "track":
        keys = embs_df["track_rowid"].to_numpy(dtype=np.int64)
        matrix = embs_df.filter(regex=r"^e\d+$").to_numpy(dtype=np.float32)
    elif name == "album":
        agg = Albums.embeddings(embs_df, track_lookup_df)
        keys = agg["album_rowid"].to_numpy(dtype=np.int64)
        matrix = agg.filter(regex=r"^e\d+$").to_numpy(dtype=np.float32)
    elif name == "artist":
        agg = Artists.embeddings(embs_df, track_lookup_df)
        keys = agg["artist_rowid"].to_numpy(dtype=np.int64)
        matrix = agg.filter(regex=r"^e\d+$").to_numpy(dtype=np.float32)
    elif name == "label":
        agg = Labels.embeddings(embs_df, track_lookup_df)
        keys = agg["label"].to_numpy()
        matrix = agg.filter(regex=r"^e\d+$").to_numpy(dtype=np.float32)
    else:
        raise ValueError(f"Unknown entity: {name}")

    N = len(matrix)
    print(f"  {N:,} entities, k={k}")

    neighbor_idx, neighbor_dist = _knn_search(matrix, k, batch_size, nprobe)
    del matrix
    neighbor_keys = keys[neighbor_idx]  # (N, k+1)

    key_type = pa.utf8() if isinstance(keys[0], str) else pa.int64()

    # neighbor keys
    knn_cols = {key_col: keys}
    for i in range(k + 1):
        knn_cols[f"n{i}"] = neighbor_keys[:, i]
    knn_schema = pa.schema([(c, key_type) for c in knn_cols])
    knn_path = output_dir / f"{name}_knn.parquet"
    _write_chunked(knn_path, knn_cols, knn_schema, N)

    # cosine similarity scores
    score_cols = {key_col: keys}
    for i in range(k + 1):
        score_cols[f"s{i}"] = neighbor_dist[:, i]
    score_schema = pa.schema(
        [(c, pa.float32() if c.startswith("s") else key_type) for c in score_cols]
    )
    score_path = output_dir / f"{name}_knn_scores.parquet"
    _write_chunked(score_path, score_cols, score_schema, N)

    written = [knn_path, score_path]
    del neighbor_dist, neighbor_keys
    elapsed = time.time() - t0
    rate = N / elapsed if elapsed > 0 else 0.0
    for p in written:
        print(f"  → {p}")
    print(f"  {name:<8} {N:>10,} rows  ({elapsed:.1f}s, {rate:,.0f} rows/s)")


def main():
    parser = argparse.ArgumentParser(
        description="Precompute KNN tables for all entity types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--embedding",
        default=os.environ.get("T2M_EMBEDDING"),
        help="Path to embedding parquet (track_rowid + e0..e127). $T2M_EMBEDDING",
    )
    parser.add_argument(
        "--track-lookup",
        default=os.environ.get("T2M_TRACK_LOOKUP"),
        help="Path to track_lookup.parquet. $T2M_TRACK_LOOKUP",
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("T2M_KNN_DIR"),
        help="Directory for output parquets. $T2M_KNN_DIR",
    )
    parser.add_argument("--k-tracks", type=int, default=K_TRACKS)
    parser.add_argument("--k-albums", type=int, default=K_ALBUMS)
    parser.add_argument("--k-artists", type=int, default=K_ARTISTS)
    parser.add_argument("--k-labels", type=int, default=K_LABELS)
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
    parser.add_argument(
        "--entities",
        nargs="+",
        choices=list(ENTITIES),
        default=list(ENTITIES),
        metavar="ENTITY",
        help=f"Entities to process (default: all). Choices: {list(ENTITIES)}",
    )
    args = parser.parse_args()

    if args.embedding is None:
        raise ValueError("--embedding / $T2M_EMBEDDING not set")
    if args.track_lookup is None:
        raise ValueError("--track-lookup / $T2M_TRACK_LOOKUP not set")
    if args.output_dir is None:
        raise ValueError("--output-dir / $T2M_KNN_DIR not set")

    embedding_path = Path(args.embedding)
    track_lookup_path = Path(args.track_lookup)
    output_dir = Path(args.output_dir)

    if not embedding_path.exists():
        raise FileNotFoundError(f"Embedding not found: {embedding_path}")
    if not track_lookup_path.exists():
        raise FileNotFoundError(f"Track lookup not found: {track_lookup_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # apply CLI k overrides
    cfg = {k: dict(v) for k, v in ENTITIES.items()}
    cfg["track"]["k"] = args.k_tracks
    cfg["album"]["k"] = args.k_albums
    cfg["artist"]["k"] = args.k_artists
    cfg["label"]["k"] = args.k_labels

    print(f"Embedding  : {embedding_path}")
    print(f"Lookup     : {track_lookup_path}")
    print(f"Output dir : {output_dir}")
    print(f"Entities   : {args.entities}")
    print(f"Batch size : {args.batch_size:,}")
    print(f"nprobe     : {args.nprobe}")
    print()

    needs_lookup = any(e != "track" for e in args.entities)

    print("Loading embeddings...")
    t0 = time.time()
    embs_df = pd.read_parquet(embedding_path)
    ndim = embs_df.filter(regex=r"^e\d+$").shape[1]
    print(f"  {len(embs_df):,} tracks, {ndim}d  ({time.time()-t0:.1f}s)")

    track_lookup_df = None
    if needs_lookup:
        print("Loading track_lookup...")
        t0 = time.time()
        track_lookup_df = pd.read_parquet(
            track_lookup_path,
            columns=["track_rowid", "artist_rowid", "album_rowid", "label"],
        )
        print(f"  {len(track_lookup_df):,} rows  ({time.time()-t0:.1f}s)")

    t_total = time.time()
    for name in args.entities:
        _process_entity(
            name,
            cfg[name],
            embs_df,
            track_lookup_df,
            output_dir,
            args.batch_size,
            args.nprobe,
        )

    print(f"\nDone in {time.time() - t_total:.1f}s")


if __name__ == "__main__":
    main()
