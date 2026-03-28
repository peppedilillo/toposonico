#!/usr/bin/env python3
"""
FAISS Index Benchmark
=====================
Benchmarks HNSW, HNSW+SQ, IVF Flat, IVF Flat with HNSW quantizer,
OPQ+IVF+PQ (8-bit and 4-bit), and IVF+RaBitQ on real track embeddings
loaded from a parquet file.

Usage:
    python benchmark_faiss.py <embedding.parquet> [--n-query N] [benchmark ...]

Examples:
    python benchmark_faiss.py ../ml/outs/track_embeddings.parquet
    python benchmark_faiss.py ../ml/outs/track_embeddings.parquet --n-query 2000 hnsw ivf
    python benchmark_faiss.py ../ml/outs/track_embeddings.parquet opq_pq rabitq
"""

import argparse
import gc
import sys
import time

import numpy as np
import faiss
import pandas as pd


K = 100
SEED = 666

ALL_BENCHMARKS = [
    "hnsw", "hnsw_sq", "ivf", "ivf_hnsw_quantizer", "opq_pq", "rabitq",
]

BOLD  = "\033[1m"
DIM_C = "\033[2m"
CYAN  = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED   = "\033[31m"
RESET = "\033[0m"

HLINE = f"{DIM_C}{'─' * 85}{RESET}"


def format_bytes(n: float) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def get_index_size(index) -> int:
    """Index size in bytes.

    For small indexes (< 512 MB estimated) we serialize for an exact
    number.  For large indexes we estimate from FAISS internals to avoid
    allocating a multi-GB byte buffer that could cause OOM.
    """
    ntotal = index.ntotal
    dim = index.d

    estimated = None

    if isinstance(index, faiss.IndexHNSWFlat):
        M = index.hnsw.cum_nneighbor_per_level.at(1)  # links per node
        # stored vectors (float32) + HNSW graph links (int32, 2*M per level)
        vec_bytes = ntotal * dim * 4
        # graph: each node stores neighbors per level; rough estimate
        graph_bytes = ntotal * 2 * M * 4
        estimated = vec_bytes + graph_bytes

    elif isinstance(index, faiss.IndexHNSWSQ):
        M = index.hnsw.cum_nneighbor_per_level.at(1)
        # SQ 8-bit: 1 byte per dimension
        vec_bytes = ntotal * dim * 1
        graph_bytes = ntotal * 2 * M * 4
        estimated = vec_bytes + graph_bytes

    elif isinstance(index, faiss.IndexIVFFlat):
        # stored vectors + overhead (inverted lists, quantizer)
        vec_bytes = ntotal * dim * 4
        list_overhead = ntotal * 8  # IDs (int64)
        estimated = vec_bytes + list_overhead

    # Use exact serialization if the estimate is small (< 512 MB)
    # or if we couldn't estimate
    if estimated is None or estimated < 512 * 1024 * 1024:
        buf = faiss.serialize_index(index)
        return buf.size * buf.itemsize

    return estimated


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
    print(f"    Index size (estimated)  : {CYAN}{format_bytes(idx_bytes):>12}{RESET}"
          f"   ({per_vec:.0f} bytes/vector)")
    sign = "+" if rss_delta >= 0 else ""
    print(f"    Process RSS delta       : {CYAN}{sign}{format_bytes(rss_delta):>11}{RESET}"
          f"   (current RSS: {format_bytes(rss_after)})")
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
        f"{color_recall(r1):>17} {color_recall(r10):>17} {color_recall(r100):>17} {miss_s:>17}"
    )


def evaluate(index, xq: np.ndarray, gt: np.ndarray, k: int, label: str) -> None:
    t0 = time.time()
    D, I = index.search(xq, k)
    elapsed = time.time() - t0

    nq = xq.shape[0]
    ms_per_q = elapsed * 1000.0 / nq
    missing = (I == -1).sum() / float(k * nq)

    r1  = (I[:, :1]   == gt[:, :1]).sum()   / float(nq)
    r10 = sum(len(set(I[i, :10])  & set(gt[i, :10]))  for i in range(nq)) / (nq * min(k, 10))
    r100 = sum(len(set(I[i, :100]) & set(gt[i, :100])) for i in range(nq)) / (nq * min(k, 100))

    print_result(label, ms_per_q, r1, r10, r100, missing)


def compute_groundtruth_chunked(xb: np.ndarray, xq: np.ndarray, k: int,
                                 chunk_size: int = 500_000) -> np.ndarray:
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


def load_data(path: str, n_query: int, k: int, n_train: int | None = None, seed: int = SEED):
    header("Data Loading")

    step(f"Reading {path}")
    t = time.time()
    df = pd.read_parquet(path)
    done(time.time() - t)

    step("Extracting embedding matrix")
    t = time.time()
    matrix = np.ascontiguousarray(df.filter(regex=r"^e\d+$")).astype("float32")
    n, dim = matrix.shape
    done(time.time() - t)

    del df
    gc.collect()

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
    gc.collect()
    done(time.time() - t)

    # Training subset
    if n_train is not None and n_train < len(xb):
        step(f"Sampling {n_train:,} training vectors")
        t = time.time()
        train_idx = rng.choice(len(xb), size=n_train, replace=False)
        xt = xb[train_idx].copy()
        done(time.time() - t)
    else:
        xt = xb

    step(f"Computing ground truth (brute-force FlatIP, k={k})")
    t = time.time()
    gt = compute_groundtruth_chunked(xb, xq, k)
    done(time.time() - t)

    print(f"\n  {BOLD}Dataset{RESET}")
    print(f"    Base vectors : {CYAN}{len(xb):>12,}{RESET}  ×  {dim}d")
    print(f"    Train vectors: {CYAN}{len(xt):>12,}{RESET}")
    print(f"    Queries      : {CYAN}{n_query:>12,}{RESET}")

    return xb, xq, xt, gt


def bench_hnsw(xb, xq, gt, k):
    header("HNSW Flat  (IndexHNSWFlat)")
    n, dim = xb.shape

    gc.collect()
    rss0 = get_rss_bytes()
    index = faiss.IndexHNSWFlat(dim, 32)
    index.hnsw.efConstruction = 40

    step(f"Adding {n:,} vectors")
    t = time.time(); index.add(xb); done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    print_table_header()
    for ef in (16, 32, 64, 128, 256):
        for bq in (True, False):
            index.hnsw.efSearch = ef
            index.hnsw.search_bounded_queue = bq
            label = f"efSearch={ef:<4} bounded_queue={bq}"
            evaluate(index, xq, gt, k, label)

    del index; gc.collect()
    return [("hnsw", idx_bytes)]


def bench_hnsw_sq(xb, xq, xt, gt, k):
    header("HNSW + Scalar Quantizer  (IndexHNSWSQ)")
    n, dim = xb.shape

    gc.collect()
    rss0 = get_rss_bytes()
    index = faiss.IndexHNSWSQ(dim, faiss.ScalarQuantizer.QT_8bit, 16)
    index.hnsw.efConstruction = 40

    step("Training scalar quantizer")
    t = time.time(); index.train(xt); done(time.time() - t)

    step(f"Adding {n:,} vectors")
    t = time.time(); index.add(xb); done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    print_table_header()
    for ef in (16, 32, 64, 128, 256):
        index.hnsw.efSearch = ef
        evaluate(index, xq, gt, k, f"efSearch={ef}")

    del index; gc.collect()
    return [("hnsw_sq", idx_bytes)]


def bench_ivf(xb, xq, xt, gt, k):
    n, dim = xb.shape
    nlist = int(np.clip(4 * np.sqrt(n), 256, 32768))
    header(f"IVF Flat  (IndexIVFFlat, nlist={nlist:,})")

    gc.collect()
    rss0 = get_rss_bytes()
    quantizer = faiss.IndexFlatIP(dim)
    index = faiss.IndexIVFFlat(quantizer, dim, nlist, faiss.METRIC_INNER_PRODUCT)
    index.cp.min_points_per_centroid = 5

    step(f"Training IVF ({nlist:,} centroids)")
    t = time.time(); index.train(xt); done(time.time() - t)

    step(f"Adding {n:,} vectors")
    t = time.time(); index.add(xb); done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    print_table_header()
    for nprobe in (1, 4, 16, 64, 256):
        index.nprobe = nprobe
        evaluate(index, xq, gt, k, f"nprobe={nprobe}")

    del index, quantizer; gc.collect()
    return [("ivf", idx_bytes)]


def bench_ivf_hnsw_quantizer(xb, xq, xt, gt, k):
    n, dim = xb.shape
    nlist = int(np.clip(4 * np.sqrt(n), 256, 32768))
    header(f"IVF Flat + HNSW Quantizer  (nlist={nlist:,})")

    gc.collect()
    rss0 = get_rss_bytes()
    quantizer = faiss.IndexHNSWFlat(dim, 32)
    index = faiss.IndexIVFFlat(quantizer, dim, nlist, faiss.METRIC_INNER_PRODUCT)
    index.cp.min_points_per_centroid = 5
    index.quantizer_trains_alone = 2

    step(f"Training IVF with HNSW quantizer ({nlist:,} centroids)")
    t = time.time(); index.train(xt); done(time.time() - t)

    step(f"Adding {n:,} vectors")
    t = time.time(); index.add(xb); done(time.time() - t)

    idx_bytes = print_memory(index, rss0)
    quantizer.hnsw.efSearch = 64
    print_table_header()
    for nprobe in (1, 4, 16, 64, 256):
        index.nprobe = nprobe
        evaluate(index, xq, gt, k, f"nprobe={nprobe}  (quantizer efSearch=64)")

    del index, quantizer; gc.collect()
    return [("ivf_hnsw_quantizer", idx_bytes)]


def bench_opq_pq(xb, xq, xt, gt, k):
    """OPQ + IVF + PQ: compressed IVF with product-quantized codes.

    Tests both 8-bit (PQ{M}, M bytes/vec) and 4-bit fast-scan
    (PQ{M}x4fsr, M/2 bytes/vec) variants across several sub-quantizer
    counts.
    """
    n, dim = xb.shape
    nlist = int(np.clip(4 * np.sqrt(n), 256, 32768))
    header(f"OPQ + IVF + PQ  (nlist={nlist:,})")

    # Pick M values where the recommended D=4*M fits within dim.
    m_candidates = [m for m in (16, 32, 64) if 4 * m <= dim]
    if not m_candidates:
        # Fallback: largest M such that M divides dim and 4*M <= dim,
        # or simply dim//4 as a last resort.
        m_candidates = [max(4, dim // 4)]

    mem_results: list[tuple[str, int]] = []

    for M in m_candidates:
        D = 4 * M

        variants = [
            (f"PQ{M}",      f"8-bit  ({M} B/vec)"),
            (f"PQ{M}x4fsr", f"4-bit fast-scan  ({M // 2} B/vec)"),
        ]

        for pq_part, description in variants:
            factory_str = f"OPQ{M}_{D},IVF{nlist},{pq_part}"

            gc.collect()
            rss0 = get_rss_bytes()

            print(f"\n  {BOLD}{description}{RESET}  →  {DIM_C}{factory_str}{RESET}")

            step("Training (OPQ rotation + IVF + PQ codebooks)")
            t = time.time()
            try:
                index = faiss.index_factory(dim, factory_str, faiss.METRIC_INNER_PRODUCT)
            except RuntimeError as exc:
                print(f"{RED}failed{RESET}  ({exc})")
                continue
            index.train(xt)
            done(time.time() - t)

            step(f"Adding {n:,} vectors")
            t = time.time()
            index.add(xb)
            done(time.time() - t)

            idx_bytes = print_memory(index, rss0)

            label_prefix = f"OPQ{M}_{D},{pq_part}"
            mem_results.append((label_prefix, idx_bytes))

            print_table_header()
            ps = faiss.ParameterSpace()
            for nprobe in (1, 4, 16, 64, 256):
                ps.set_index_parameter(index, "nprobe", nprobe)
                evaluate(index, xq, gt, k, f"nprobe={nprobe}")

            del index
            gc.collect()

    return mem_results if mem_results else [("opq_pq (none built)", 0)]


def bench_rabitq(xb, xq, xt, gt, k):
    """IVF + RaBitQ: extreme binary quantization.

    Tests 1-bit, 2-bit, and 4-bit RaBitQ (FastScan variant) behind
    an IVF partitioner.  A random rotation (RR) is prepended as
    recommended by the FAISS documentation.

    Vectors are L2-normalized so L2 nearest neighbours coincide with
    inner-product nearest neighbours; the ground truth (computed via
    FlatIP) therefore remains valid even though RaBitQ uses L2
    internally.

    Requires FAISS ≥ 1.9.0.
    """
    n, dim = xb.shape
    nlist = int(np.clip(4 * np.sqrt(n), 256, 32768))
    header(f"RaBitQ  (IVF nlist={nlist:,}, with random rotation)")

    mem_results: list[tuple[str, int]] = []

    for nbits in (1, 2, 4):
        # Factory suffix: "RaBitQfs" for 1-bit, "RaBitQfs2" etc. for N>1.
        rq_tag = "RaBitQfs" if nbits == 1 else f"RaBitQfs{nbits}"
        factory_str = f"RR{dim},IVF{nlist},{rq_tag}"

        approx_bpv = dim * nbits / 8 + 8  # bytes per vector (codes only)
        description = f"{rq_tag}  ({nbits}-bit, ~{approx_bpv:.0f} B/vec codes)"

        gc.collect()
        rss0 = get_rss_bytes()

        print(f"\n  {BOLD}{description}{RESET}  →  {DIM_C}{factory_str}{RESET}")

        try:
            step("Training (random rotation + IVF + RaBitQ)")
            t = time.time()
            index = faiss.index_factory(dim, factory_str)
            index.train(xt)
            done(time.time() - t)
        except RuntimeError as exc:
            print(f"{RED}skipped{RESET}  ({exc})")
            print(f"    {DIM_C}(RaBitQ requires FAISS ≥ 1.9.0){RESET}")
            continue

        step(f"Adding {n:,} vectors")
        t = time.time()
        index.add(xb)
        done(time.time() - t)

        idx_bytes = print_memory(index, rss0)
        mem_results.append((f"IVF,{rq_tag}", idx_bytes))

        print_table_header()
        ps = faiss.ParameterSpace()
        for nprobe in (1, 4, 16, 64, 256):
            ps.set_index_parameter(index, "nprobe", nprobe)
            evaluate(index, xq, gt, k, f"nprobe={nprobe}")

        del index
        gc.collect()

    return mem_results if mem_results else [("rabitq (none built)", 0)]


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark FAISS index types on real track embeddings."
    )
    parser.add_argument("embedding", help="Path to track embedding parquet (track_rowid + e0…e{D-1})")
    parser.add_argument("--n-query", type=int, default=1000, help="Query vectors sampled from base (default: 1000)")
    parser.add_argument("benchmarks", nargs="*", default=ALL_BENCHMARKS,
                        metavar="benchmark", help=f"Subset to run (default: all). Choices: {', '.join(ALL_BENCHMARKS)}")
    parser.add_argument("--n-train", type=int, default=1_000_000,
                        help="Training subset size (default: 1M; use 0 for full base set)")
    args = parser.parse_args()

    unknown = set(args.benchmarks) - set(ALL_BENCHMARKS)
    if unknown:
        parser.error(f"Unknown benchmark(s): {', '.join(unknown)}\nAvailable: {', '.join(ALL_BENCHMARKS)}")

    print(f"\n{BOLD}FAISS Index Benchmark{RESET}")
    print(f"  k={K}  |  n_query={args.n_query}  |  benchmarks: {', '.join(args.benchmarks)}")
    print(f"  embedding: {args.embedding}")

    n_train = args.n_train if args.n_train > 0 else None
    xb, xq, xt, gt = load_data(args.embedding, args.n_query, K, n_train=n_train, seed=SEED)
    n_base, dim = xb.shape

    dispatch = {
        "hnsw":                lambda: bench_hnsw(xb, xq, gt, K),
        "hnsw_sq":             lambda: bench_hnsw_sq(xb, xq, xt, gt, K),
        "ivf":                 lambda: bench_ivf(xb, xq, xt, gt, K),
        "ivf_hnsw_quantizer":  lambda: bench_ivf_hnsw_quantizer(xb, xq, xt, gt, K),
        "opq_pq":              lambda: bench_opq_pq(xb, xq, xt, gt, K),
        "rabitq":              lambda: bench_rabitq(xb, xq, xt, gt, K),
    }

    total_t0 = time.time()
    mem_stats: list[tuple[str, int]] = []
    for name in args.benchmarks:
        results = dispatch[name]()
        # Each benchmark returns a list of (label, bytes) tuples.
        mem_stats.extend(results)

    elapsed = time.time() - total_t0

    raw_bytes = n_base * dim * 4
    header("Memory Summary")
    print(f"  {'Index':<36} {'Size':>12} {'bytes/vec':>12} {'vs raw f32':>12}")
    print(f"  {'─' * 36} {'─' * 12} {'─' * 12} {'─' * 12}")
    print(f"  {'(raw float32 baseline)':<36} {format_bytes(raw_bytes):>12}"
          f" {dim * 4:>12.0f} {'1.00x':>12}")
    for name, nbytes in mem_stats:
        per_vec = nbytes / max(n_base, 1)
        ratio = nbytes / raw_bytes if raw_bytes else 0
        print(f"  {name:<36} {format_bytes(nbytes):>12} {per_vec:>12.0f} {ratio:>11.2f}x")

    print(f"\n{HLINE}")
    print(f"{BOLD}  Total wall time: {elapsed:.1f}s{RESET}")
    print(HLINE)


if __name__ == "__main__":
    main()