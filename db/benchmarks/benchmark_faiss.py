#!/usr/bin/env python3
"""
FAISS Index Benchmark
=====================
Benchmarks IVF Flat, IVF Flat with HNSW quantizer, and OPQ+IVF/HNSW+PQ
fast-scan on recable entity embeddings.

Reads embedding paths from a manifest TOML and recable rowids from the DB.
Iterates over selected entities, filtering each embedding table to the
recable rowids before benchmarking.

All indexes are built via faiss.index_factory.  Build-time parameters
(nlist, training set size, ...) are derived automatically from the dataset
shape.  Search-time parameters (nprobe) are swept so you can evaluate the
speed/recall trade-off.

Usage:
    source config.env && python benchmark_faiss.py [--entities track label] [--n-query N] [benchmark ...]

Examples:
    python benchmark_faiss.py
    python benchmark_faiss.py --entities label --n-query 100 ivf
    python benchmark_faiss.py --entities track artist --n-query 2000 ivf opq_ivfhnsw_pq
"""

import argparse
import ctypes
import gc
import os
from pathlib import Path
import sqlite3
import sys
import time

import faiss
import numpy as np
import pandas as pd

from src.utils import ENTITIES
from src.utils import ENTITY_KEYS
from src.utils import read_manifest

K = 100
SEED = 666

ALL_BENCHMARKS = [
    "ivf",
    "ivf_hnsw_quantizer",
    "opq_ivfhnsw_pq",
]

BOLD = "\033[1m"
DIM_C = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
RESET = "\033[0m"

HLINE = f"{DIM_C}{'─' * 85}{RESET}"


def compute_params(n: int, dim: int) -> dict:
    """Derive build and sweep parameters from dataset shape."""
    nlist = int(np.clip(4 * np.sqrt(n), 256, 16384))

    ivf_train_size = min(n, 64 * nlist)
    pq_train_size = min(n, max(64 * nlist, 50_000))

    M_pq = 128
    d_opq = 128

    return {
        "nlist": nlist,
        "dim": dim,
        "n": n,
        "ivf_train_size": ivf_train_size,
        "pq_train_size": pq_train_size,
        "M_pq": M_pq,
        "d_opq": d_opq,
        "nprobe_values": (1, 4, 16, 64, 256),
    }


def subsample_training(xb: np.ndarray, max_size: int, seed: int = SEED) -> np.ndarray:
    """Random contiguous subsample of xb for training, or xb itself if small enough."""
    n = xb.shape[0]
    if n <= max_size:
        return xb
    rng = np.random.default_rng(seed + 1)
    idx = rng.choice(n, size=max_size, replace=False)
    return np.ascontiguousarray(xb[idx])


_LIBC = None


def _get_libc():
    global _LIBC
    if _LIBC is None:
        try:
            _LIBC = ctypes.CDLL("libc.so.6")
        except OSError:
            _LIBC = False
    return _LIBC


def free_memory():
    """Run Python GC then force glibc to return freed pages to the OS."""
    gc.collect()
    libc = _get_libc()
    if libc:
        libc.malloc_trim(0)


def format_bytes(n: float) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def get_index_size(index) -> int:
    """Index size in bytes.

    For large indexes (> 512 MB estimated) we use the estimate to avoid
    allocating a multi-GB serialization buffer.  Otherwise we serialize
    for an exact number.
    """
    ntotal = index.ntotal
    dim = index.d
    estimated = None

    if isinstance(index, faiss.IndexRefineFlat):
        refine_bytes = ntotal * dim * 4
        base_bytes = get_index_size(index.base_index) if index.base_index.ntotal > 0 else 0
        estimated = refine_bytes + base_bytes

    elif isinstance(index, faiss.IndexIVFFlat):
        vec_bytes = ntotal * dim * 4
        list_overhead = ntotal * 8
        estimated = vec_bytes + list_overhead

    if estimated is not None and estimated > 512 * 1024 * 1024:
        return estimated

    buf = faiss.serialize_index(index)
    return buf.size * buf.itemsize


def get_rss_bytes() -> int:
    """Current process RSS in bytes (VmRSS on Linux, fallback elsewhere)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except FileNotFoundError:
        pass
    import resource

    ru = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        return ru.ru_maxrss
    return ru.ru_maxrss * 1024


def print_memory(index, rss_before: int) -> int:
    """Print index size and process RSS delta.  Returns index size in bytes."""
    idx_bytes = get_index_size(index)
    rss_after = get_rss_bytes()
    rss_delta = rss_after - rss_before
    per_vec = idx_bytes / max(index.ntotal, 1)

    print(f"\n  {BOLD}Memory{RESET}")
    print(
        f"    Index size (estimated)  : {CYAN}{format_bytes(idx_bytes):>12}{RESET}" f"   ({per_vec:.0f} bytes/vector)"
    )
    sign = "+" if rss_delta >= 0 else ""
    print(
        f"    Process RSS delta       : {CYAN}{sign}{format_bytes(rss_delta):>11}{RESET}"
        f"   (current RSS: {format_bytes(rss_after)})"
    )
    return idx_bytes


def header(title: str) -> None:
    print(f"\n{HLINE}")
    print(f"{BOLD}{CYAN}  ◆  {title}{RESET}")
    print(HLINE)


def step(msg: str) -> None:
    print(f"  {DIM_C}▸{RESET} {msg} … ", end="", flush=True)


def done(elapsed: float | None = None) -> None:
    tag = f" {DIM_C}({elapsed:.2f}s){RESET}" if elapsed is not None else ""
    print(f"{GREEN}done{RESET}{tag}")


def print_table_header() -> None:
    print()
    print(f"  {'Parameters':<36} {'ms/query':>10} {'R@1':>8} {'R@10':>8} {'R@100':>8} {'missing':>8}")
    print(f"  {'─' * 36} {'─' * 10} {'─' * 8} {'─' * 8} {'─' * 8} {'─' * 8}")


def print_result(label: str, ms_per_q: float, r1: float, r10: float, r100: float, miss: float) -> None:
    def color_recall(v: float) -> str:
        if v >= 0.95:
            return f"{GREEN}{v:.4f}{RESET}"
        if v >= 0.70:
            return f"{YELLOW}{v:.4f}{RESET}"
        return f"{RED}{v:.4f}{RESET}"

    miss_s = f"{GREEN}{miss:.4f}{RESET}" if miss < 0.001 else f"{RED}{miss:.4f}{RESET}"
    print(
        f"  {label:<36} {ms_per_q:>10.3f} "
        f"{color_recall(r1):>8} {color_recall(r10):>8} {color_recall(r100):>8} {miss_s:>8}"
    )


def evaluate(index, xq: np.ndarray, gt: np.ndarray, k: int, label: str) -> None:
    t0 = time.time()
    D, I = index.search(xq, k)
    elapsed = time.time() - t0

    nq = xq.shape[0]
    ms_per_q = elapsed * 1000.0 / nq
    missing = (I == -1).sum() / float(k * nq)

    r1 = (I[:, :1] == gt[:, :1]).sum() / float(nq)
    r10 = sum(len(set(I[i, :10]) & set(gt[i, :10])) for i in range(nq)) / (nq * min(k, 10))
    r100 = sum(len(set(I[i, :100]) & set(gt[i, :100])) for i in range(nq)) / (nq * min(k, 100))

    print_result(label, ms_per_q, r1, r10, r100, missing)


def compute_groundtruth_chunked(xb: np.ndarray, xq: np.ndarray, k: int, chunk_size: int = 500_000) -> np.ndarray:
    """Compute exact k-NN ground truth in database chunks to avoid OOM."""
    nq, dim = xq.shape
    n = xb.shape[0]
    n_chunks = (n + chunk_size - 1) // chunk_size

    all_D = np.full((nq, k), -np.inf, dtype="float32")
    all_I = np.full((nq, k), -1, dtype="int64")

    for ci in range(n_chunks):
        start = ci * chunk_size
        end = min(start + chunk_size, n)

        idx = faiss.IndexFlatIP(dim)
        idx.add(np.ascontiguousarray(xb[start:end]))
        D_part, I_part = idx.search(xq, k)
        I_part += start

        merged_D = np.concatenate([all_D, D_part], axis=1)
        merged_I = np.concatenate([all_I, I_part], axis=1)
        order = np.argsort(-merged_D, axis=1)[:, :k]
        all_D = np.take_along_axis(merged_D, order, axis=1)
        all_I = np.take_along_axis(merged_I, order, axis=1)

        del idx, D_part, I_part, merged_D, merged_I, order
    return all_I


def load_filtered_data(
    embedding_path: str | Path, rowids: np.ndarray, rowid_col: str, n_query: int, k: int, seed: int = SEED
):
    """Load an embedding parquet, filter to recable rowids, prepare benchmark data."""
    header("Data Loading")

    step(f"Reading {embedding_path}")
    t = time.time()
    df = pd.read_parquet(embedding_path)
    n_raw = len(df)
    done(time.time() - t)

    step(f"Filtering to {len(rowids):,} recable rowids")
    t = time.time()
    df = df[df[rowid_col].isin(rowids)]
    n_filtered = len(df)
    done(time.time() - t)
    print(f"  {DIM_C}  {n_raw:,} → {n_filtered:,} ({100 * n_filtered / n_raw:.1f}%){RESET}")

    step("Extracting embedding matrix")
    t = time.time()
    matrix = np.ascontiguousarray(df.filter(regex=r"^e\d+$")).astype("float32")
    n, dim = matrix.shape
    done(time.time() - t)

    del df
    free_memory()

    step("L2-normalizing (cosine space)")
    t = time.time()
    faiss.normalize_L2(matrix)
    done(time.time() - t)

    step(f"Sampling {n_query:,} query vectors")
    t = time.time()
    rng = np.random.default_rng(seed)
    query_idx = rng.choice(n, size=n_query, replace=False)
    base_mask = np.ones(n, dtype=bool)
    base_mask[query_idx] = False
    xq = matrix[query_idx].copy()
    xb = matrix[base_mask].copy()
    del matrix
    free_memory()
    done(time.time() - t)

    step(f"Computing ground truth (brute-force FlatIP, k={k}, chunked)")
    t = time.time()
    gt = compute_groundtruth_chunked(xb, xq, k)
    free_memory()
    done(time.time() - t)

    print(f"\n  {BOLD}Dataset{RESET}")
    print(f"    Base vectors : {CYAN}{len(xb):>12,}{RESET}  ×  {dim}d")
    print(f"    Queries      : {CYAN}{n_query:>12,}{RESET}")
    print(f"    RSS after load: {CYAN}{format_bytes(get_rss_bytes()):>11}{RESET}")

    return xb, xq, gt


def bench_ivf(xb, xq, gt, k, params):
    nlist = params["nlist"]
    factory = f"IVF{nlist},Flat"
    header(f"IVF Flat  ({factory})")
    n, dim = xb.shape

    free_memory()
    rss0 = get_rss_bytes()
    index = faiss.index_factory(dim, factory, faiss.METRIC_INNER_PRODUCT)

    xt = subsample_training(xb, params["ivf_train_size"])
    step(f"Training IVF ({nlist:,} centroids, {len(xt):,} vectors)")
    t = time.time()
    index.train(xt)
    done(time.time() - t)
    del xt

    step(f"Adding {n:,} vectors")
    t = time.time()
    index.add(xb)
    done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    print_table_header()
    for nprobe in params["nprobe_values"]:
        index.nprobe = nprobe
        evaluate(index, xq, gt, k, f"nprobe={nprobe}")

    del index
    free_memory()
    return idx_bytes


def bench_ivf_hnsw_quantizer(xb, xq, gt, k, params):
    nlist = params["nlist"]
    factory = f"IVF{nlist}_HNSW32,Flat"
    header(f"IVF Flat + HNSW Quantizer  ({factory})")
    n, dim = xb.shape

    free_memory()
    rss0 = get_rss_bytes()
    index = faiss.index_factory(dim, factory, faiss.METRIC_INNER_PRODUCT)

    xt = subsample_training(xb, params["ivf_train_size"])
    step(f"Training IVF+HNSW quantizer ({nlist:,} centroids, {len(xt):,} vectors)")
    t = time.time()
    index.train(xt)
    done(time.time() - t)
    del xt

    step(f"Adding {n:,} vectors")
    t = time.time()
    index.add(xb)
    done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    quantizer = faiss.downcast_index(index.quantizer)
    efSearch = 64
    quantizer.hnsw.efSearch = efSearch
    print_table_header()
    for nprobe in params["nprobe_values"]:
        index.nprobe = nprobe
        evaluate(index, xq, gt, k, f"nprobe={nprobe}  (quantizer efSearch={efSearch})")

    del index
    free_memory()
    return idx_bytes


def bench_opq_ivfhnsw_pq(xb, xq, gt, k, params):
    nlist = params["nlist"]
    M_pq = params["M_pq"]
    d_opq = params["d_opq"]
    dim = params["dim"]
    factory = f"OPQ{M_pq}_{d_opq},IVF{nlist}_HNSW32,PQ{M_pq}x4fsr"
    header(f"OPQ + IVF/HNSW + PQ fast-scan  ({factory})")
    n = xb.shape[0]

    free_memory()
    rss0 = get_rss_bytes()
    index = faiss.index_factory(dim, factory, faiss.METRIC_INNER_PRODUCT)

    xt = subsample_training(xb, params["pq_train_size"])
    step(f"Training OPQ+IVF/HNSW+PQ ({nlist:,} centroids, {len(xt):,} vectors)")
    t = time.time()
    index.train(xt)
    done(time.time() - t)
    del xt

    step(f"Adding {n:,} vectors")
    t = time.time()
    index.add(xb)
    done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    ivf = faiss.extract_index_ivf(index)
    quantizer = faiss.downcast_index(ivf.quantizer)
    efSearch = 64
    quantizer.hnsw.efSearch = efSearch
    print_table_header()
    for nprobe in params["nprobe_values"]:
        ivf.nprobe = nprobe
        evaluate(index, xq, gt, k, f"nprobe={nprobe}  (efSearch={efSearch})")

    del index
    free_memory()
    return idx_bytes


def run_benchmarks(xb, xq, gt, k, params, benchmark_names):
    """Run selected benchmarks and return (name, idx_bytes) pairs."""
    dispatch = {
        "ivf": lambda: bench_ivf(xb, xq, gt, k, params),
        "ivf_hnsw_quantizer": lambda: bench_ivf_hnsw_quantizer(xb, xq, gt, k, params),
        "opq_ivfhnsw_pq": lambda: bench_opq_ivfhnsw_pq(xb, xq, gt, k, params),
    }
    mem_stats: list[tuple[str, int]] = []
    for name in benchmark_names:
        idx_bytes = dispatch[name]()
        mem_stats.append((name, idx_bytes))
    return mem_stats


def print_memory_summary(mem_stats, n_base, dim):
    raw_bytes = n_base * dim * 4
    header("Memory Summary")
    print(f"  {'Index':<28} {'Size':>12} {'bytes/vec':>12} {'vs raw f32':>12}")
    print(f"  {'─' * 28} {'─' * 12} {'─' * 12} {'─' * 12}")
    print(f"  {'(raw float32 baseline)':<28} {format_bytes(raw_bytes):>12}" f" {dim * 4:>12.0f} {'1.00x':>12}")
    for name, nbytes in mem_stats:
        per_vec = nbytes / max(n_base, 1)
        ratio = nbytes / raw_bytes
        print(f"  {name:<28} {format_bytes(nbytes):>12} {per_vec:>12.0f} {ratio:>11.2f}x")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark FAISS index types on recable entity embeddings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--manifest",
        default=os.environ.get("SICK_MANIFEST"),
        metavar="PATH",
        help="Path to ml manifest TOML. $SICK_MANIFEST",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("SICK_DB"),
        metavar="PATH",
        help="Path to sick.db. $SICK_DB",
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        default=list(ENTITIES),
        choices=ENTITIES,
        metavar="ENTITY",
        help=f"Entities to benchmark (default: all). Choices: {', '.join(ENTITIES)}",
    )
    parser.add_argument("--n-query", type=int, default=1000, help="Query vectors sampled from base (default: 1000)")
    parser.add_argument(
        "benchmarks",
        nargs="*",
        default=ALL_BENCHMARKS,
        metavar="benchmark",
        help=f"Subset to run (default: all). Choices: {', '.join(ALL_BENCHMARKS)}",
    )
    parser.add_argument("--raw", action="store_true", help="Disable ANSI escape code formatting")
    args = parser.parse_args()

    if args.raw:
        global BOLD, DIM_C, CYAN, GREEN, YELLOW, RED, RESET, HLINE
        BOLD = DIM_C = CYAN = GREEN = YELLOW = RED = RESET = ""
        HLINE = "─" * 85

    if args.manifest is None:
        parser.error("--manifest / $SICK_MANIFEST not set")
    if args.db is None:
        parser.error("--db / $SICK_DB not set")

    unknown = set(args.benchmarks) - set(ALL_BENCHMARKS)
    if unknown:
        parser.error(f"Unknown benchmark(s): {', '.join(unknown)}\nAvailable: {', '.join(ALL_BENCHMARKS)}")

    manifest = read_manifest(args.manifest)
    embedding_paths = manifest["embedding"]

    conn = sqlite3.connect(args.db)
    entity_rowids = {}
    for entity, key_col in (
        ("track", ENTITY_KEYS.track),
        ("artist", ENTITY_KEYS.artist),
        ("album", ENTITY_KEYS.album),
        ("label", ENTITY_KEYS.label),
    ):
        rows = conn.execute(
            f"SELECT {key_col} FROM {entity}s WHERE recable = 1 ORDER BY {key_col}"
        ).fetchall()
        entity_rowids[entity] = np.array([r[0] for r in rows], dtype=np.int64)
    conn.close()

    entity_specs = tuple(
        spec
        for spec in (
            ("track", embedding_paths.track, entity_rowids["track"], ENTITY_KEYS.track),
            ("artist", embedding_paths.artist, entity_rowids["artist"], ENTITY_KEYS.artist),
            ("album", embedding_paths.album, entity_rowids["album"], ENTITY_KEYS.album),
            ("label", embedding_paths.label, entity_rowids["label"], ENTITY_KEYS.label),
        )
        if spec[0] in args.entities
    )

    print(f"\n{BOLD}FAISS Index Benchmark{RESET}")
    print(f"  k={K}  |  n_query={args.n_query}  |  benchmarks: {', '.join(args.benchmarks)}")
    print(f"  entities: {', '.join(args.entities)}")
    print(f"  manifest: {args.manifest}")

    total_t0 = time.time()
    for entity, emb_path, rowids, rowid_col in entity_specs:
        print(f"\n{'═' * 85}")
        print(f"  {BOLD}{entity.upper()}{RESET}")
        print()

        xb, xq, gt = load_filtered_data(emb_path, rowids, rowid_col, args.n_query, K, SEED)
        n_base, dim = xb.shape
        params = compute_params(n_base, dim)

        print(f"\n  {BOLD}Auto-params{RESET}")
        print(f"    nlist            : {CYAN}{params['nlist']:>10,}{RESET}")
        print(f"    ivf_train_size   : {CYAN}{params['ivf_train_size']:>10,}{RESET}")
        print(f"    pq_train_size    : {CYAN}{params['pq_train_size']:>10,}{RESET}")

        mem_stats = run_benchmarks(xb, xq, gt, K, params, args.benchmarks)
        print_memory_summary(mem_stats, n_base, dim)

        del xb, xq, gt
        free_memory()

    elapsed = time.time() - total_t0
    print(f"\n{HLINE}")
    print(f"{BOLD}  Total wall time: {elapsed:.1f}s{RESET}")
    print(HLINE)


if __name__ == "__main__":
    main()
