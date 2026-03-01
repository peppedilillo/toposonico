from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
from typing import Callable

import numpy as np
import pandas as pd
import torch


def split(chunk_paths: list[Path], valid_fraction: float, seed: int):
    """File-level test/train split."""
    rng = np.random.default_rng(seed)
    chunk_order = rng.permutation(len(chunk_paths)).tolist()
    n_valid = max(1, int(len(chunk_paths) * valid_fraction))
    valid_chunk_paths = [chunk_paths[i] for i in chunk_order[-n_valid:]]
    train_chunk_paths = [chunk_paths[i] for i in chunk_order[:-n_valid]]
    return train_chunk_paths, valid_chunk_paths


# noinspection PyUnusedLocal
def build_vocab_from_chunks(chunk_paths: list[Path], cmin: int):
    """ Builds a table mappings between Spotify track row-ids, counts playlist appearances
     and filters them for tracks showing in greater-equal than `cmin` playlists.
    """
    # for each track, counts in how many playlist it appears
    counter = Counter()
    for path in chunk_paths:
        pt = pd.read_parquet(path, columns=["playlist_rowid", "track_rowid"])
        counter.update(pt.groupby("track_rowid")["playlist_rowid"].nunique().to_dict())
    # removes tracks appearing in less than `cmin` playlist
    vocab = (
        pd.DataFrame(counter.items(), columns=["track_rowid", "playlist_count"])
        .query("playlist_count >= @cmin")
        .sort_values("track_rowid")
        .reset_index(drop=True)
    )
    vocab["track_id"] = vocab.index.astype("int32")
    return vocab


def get_nsampler(weights_gpu: torch.Tensor, k: int, batch_size: int, block: int = 64) -> tuple[Callable, Callable]:
    total = batch_size * k * block
    cache = [torch.empty(0, dtype=torch.long, device=weights_gpu.device), 0]

    def sample(n: int) -> torch.Tensor:
        needed = n * k
        if cache[1] + needed > cache[0].shape[0]:
            cache[0] = torch.multinomial(weights_gpu, total, replacement=True)
            cache[1] = 0
        start = cache[1]
        cache[1] = start + needed
        return cache[0][start : start + needed].view(n, k)

    def flush():
        cache[0] = torch.empty(0, dtype=torch.long, device=weights_gpu.device)
        cache[1] = 0

    return sample, flush


def precompute_pairs(pt: pd.DataFrame, w: int) -> torch.Tensor:
    """Generate all (center, context) skip-gram pairs for a chunk.

    Works entirely in numpy: finds playlist boundaries, builds per-track
    metadata with np.repeat, then loops over the 2W offsets (~10 iters)
    doing vectorised index arithmetic across all tracks at once.
    """
    # sort by playlist_rowid so boundary detection works even if the
    # parquet rows aren't perfectly grouped (no ORDER BY in the SQL)
    if not pt["playlist_rowid"].is_monotonic_increasing:
        pt = pt.sort_values("playlist_rowid", kind="mergesort")

    pids = pt["playlist_rowid"].values
    tids = pt["track_id"].values

    # --- playlist boundaries ---
    breaks = np.empty(len(pids), dtype=bool)
    breaks[0] = True
    breaks[1:] = pids[1:] != pids[:-1]
    starts = np.flatnonzero(breaks)
    lengths = np.diff(np.append(starts, len(pids)))

    # keep only playlists with >= 2 tracks
    valid = lengths >= 2
    starts, lengths = starts[valid], lengths[valid]
    if len(starts) == 0:
        return torch.empty(2, 0, dtype=torch.long)

    # --- per-track metadata (vectorised via np.repeat) ---
    total = lengths.sum()
    pos = np.arange(total, dtype=np.int32)
    cum = np.cumsum(lengths)
    # position within playlist: 0,1,..,L-1, 0,1,..,L-1, ...
    pos -= np.repeat(np.concatenate(([0], cum[:-1])), lengths).astype(np.int32)

    flat_start = np.repeat(starts, lengths)    # absolute start index per track
    flat_len   = np.repeat(lengths, lengths)   # playlist length per track

    # effective window per track: min(2W, L-1), mirrors the original code
    ew = np.minimum(2 * w, flat_len - 1)

    # --- generate pairs for each offset ---
    all_centers = []
    all_contexts = []
    for k in range(-w, w + 1):
        if k == 0:
            continue
        # offset k is valid iff it falls inside range((-ew)//2, ew//2+1),
        # matching the original playlist_pairs logic exactly
        mask = ((-ew) // 2 <= k) & (k <= ew // 2)
        m_pos   = pos[mask]
        m_start = flat_start[mask]
        m_len   = flat_len[mask]

        ctx_pos = (m_pos + k) % m_len
        ctx_idx = m_start + ctx_pos

        all_centers.append(tids[m_start + m_pos])
        all_contexts.append(tids[ctx_idx])

    if not all_centers:
        return torch.empty(2, 0, dtype=torch.long)

    centers  = np.concatenate(all_centers)
    contexts = np.concatenate(all_contexts)
    return torch.from_numpy(np.stack([centers, contexts]))


def make_cached_reader() -> Callable:
    """Return a thread-safe reader that loads each parquet file once and caches it in RAM.

    On the first call for a given path the file is read from disk; subsequent calls
    return the cached DataFrame immediately.  Useful on cloud machines with ample RAM
    (e.g. A100 + 200 GiB) to eliminate per-epoch disk I/O.

    Usage::

        reader = make_cached_reader()
        process_chunk = init_chunk_processor(vocab, W, reader=reader)
    """
    _cache: dict[Path, pd.DataFrame] = {}
    _lock = threading.Lock()

    def reader(path: Path) -> pd.DataFrame:
        with _lock:
            if path not in _cache:
                _cache[path] = pd.read_parquet(path, columns=["playlist_rowid", "track_rowid"])
            return _cache[path]

    return reader


def init_chunk_processor(
        vocab: pd.DataFrame,
        w: int,
        thr_quantile: float = 0.99,
        reader: Callable | None = None,
) -> Callable:
    """Initialise a stateless chunk-processing closure.

    Args:
        vocab: DataFrame with columns track_rowid, track_id, playlist_count.
        w: Context-window half-width for skip-gram pair generation.
        thr_quantile: Subsampling threshold expressed as a frequency quantile.
        reader: Callable ``(path) -> DataFrame`` used to load raw chunk data.
            Defaults to a plain ``pd.read_parquet`` call.
    """
    if reader is None:
        reader = lambda path: pd.read_parquet(path, columns=["playlist_rowid", "track_rowid"])

    vocab_trowids = vocab["track_rowid"].values
    # we are going to binary search track row ids when filtering oov tracks
    assert np.all(vocab_trowids[:-1] <= vocab_trowids[1:])
    vocab_tids = vocab["track_id"].values

    counts = vocab["playlist_count"].values.astype(np.float64)
    freq = counts / counts.sum()

    sub_threshold = float(np.quantile(freq, thr_quantile))
    keep_probs = np.minimum(1.0, np.sqrt(sub_threshold / freq)).astype(np.float32)  # indexed by track_id

    def subsample(pt: pd.DataFrame, chunk_rng: np.random.RandomState) -> pd.DataFrame:
        p = keep_probs[pt["track_id"].values]
        mask = chunk_rng.random(len(pt)) < p
        return pt[mask].reset_index(drop=True)

    def remap_chunk(pt: pd.DataFrame) -> pd.DataFrame:
        """Filter OOV tracks and add track_id via binary search (replaces pd.merge)."""
        rowids = pt["track_rowid"].values
        idx = np.searchsorted(vocab_trowids, rowids)
        idx = np.clip(idx, 0, len(vocab_trowids) - 1)
        match = vocab_trowids[idx] == rowids
        out = pt[match].copy()
        out["track_id"] = vocab_tids[idx[match]]
        return out

    def process_chunk(path: Path, chunk_rng: np.random.RandomState) -> torch.Tensor:
        pt = reader(path)
        pt = remap_chunk(pt)
        pt = subsample(pt, chunk_rng)
        pairs = precompute_pairs(pt, w)
        if pairs.shape[1] == 0:
            return pairs
        perm = torch.from_numpy(chunk_rng.permutation(pairs.shape[1]))
        return pairs[:, perm].pin_memory()
    return process_chunk


class PrefetchPairStream:
    """Producer-consumer pair stream with background chunk preprocessing.

    A thread pool preprocesses chunks in parallel. Results are always consumed
    in submission order (chunk 0, 1, 2, ...) so training is reproducible across
    runs. A semaphore limits how many chunks are held in memory at once.
    Each chunk gets its own deterministic RNG seeded from (seed, epoch, chunk_index).
    """

    def __init__(
            self,
            chunk_paths: list[Path],
            process_chunk: Callable,
            epoch: int,
            seed: int,
            n_workers: int = 4,
    ):
        self._buffer = torch.empty(2, 0, dtype=torch.long)  # leftover buffer
        self._n_chunks = len(chunk_paths)
        self._chunks_done = 0
        self._pairs_produced = 0
        self._pairs_consumed = 0
        # the semaphore counter starts from the argument and decrements by one at
        # each `sem.acquire()` call, or increment by one at each `sem.release()` call.
        # when it gets to zero the calling thread blocks and waits.
        # the goal is to have at most `n_worker` batch can be processed but not yet consumed.
        self._sem = threading.Semaphore(n_workers)

        def _process_with_sem(path, rng):
            # this will cause semaphore to get to zero.
            # when it happens, the thread will wait for some release, to start processing.
            self._sem.acquire()
            return process_chunk(path, rng)

        executor = ThreadPoolExecutor(max_workers=n_workers)
        # puts all chunks processing, ordered, into a deque
        self._futures = deque(
            executor.submit(_process_with_sem, path, np.random.default_rng(seed + epoch * 10_000 + idx))
            for idx, path in enumerate(chunk_paths)
        )
        executor.shutdown(wait=False)

    def next_batch(self, batch_size: int) -> torch.Tensor:
        # keep pulling data until we enough for a batch
        while self._buffer.shape[1] < batch_size:
            # when all processing is finished, return what's left
            if not self._futures:
                remainder = self._buffer
                self._buffer = torch.empty(2, 0, dtype=torch.long)
                return remainder
            # this enforce determinism.
            # processing happens in parallel but we wait the first to finish before the rest can return too.
            # this is why the thing is called semaphore.
            tensor = self._futures.popleft().result()  # blocks until chunk N is ready
            self._sem.release()
            self._chunks_done += 1
            if tensor.shape[1] > 0:
                self._pairs_produced += tensor.shape[1]
                self._buffer = torch.cat([self._buffer, tensor], dim=1) if self._buffer.shape[1] > 0 else tensor

        batch = self._buffer[:, :batch_size]
        self._buffer = self._buffer[:, batch_size:]
        self._pairs_consumed += batch.shape[1]
        return batch

    @property
    def estimated_total_pairs(self) -> int | None:
        if self._chunks_done == 0:
            return None
        return int(self._pairs_produced / self._chunks_done * self._n_chunks)


class SerialPairStream:
    """Single-threaded pair stream (e.g. for validation with few chunks)."""

    def __init__(self, chunk_paths: list[Path], process_chunk: Callable, epoch: int, seed: int):
        self._buffer = torch.empty(2, 0, dtype=torch.long)
        self._paths = deque(enumerate(chunk_paths))
        self._process_chunk = process_chunk
        self._epoch = epoch
        self._seed = seed
        self._n_chunks = len(chunk_paths)
        self._chunks_done = 0
        self._pairs_produced = 0
        self._pairs_consumed = 0

    def next_batch(self, batch_size: int) -> torch.Tensor:
        while self._buffer.shape[1] < batch_size:
            if not self._paths:
                remainder = self._buffer
                self._buffer = torch.empty(2, 0, dtype=torch.long)
                self._pairs_consumed += remainder.shape[1]
                return remainder
            idx, path = self._paths.popleft()
            chunk_rng = np.random.default_rng(self._seed + self._epoch * 10_000 + idx)
            new = self._process_chunk(path, chunk_rng)
            self._chunks_done += 1
            if new.shape[1] == 0:
                continue
            self._pairs_produced += new.shape[1]
            self._buffer = torch.cat([self._buffer, new], dim=1) if self._buffer.shape[1] > 0 else new

        batch = self._buffer[:, :batch_size]
        self._buffer = self._buffer[:, batch_size:]
        self._pairs_consumed += batch.shape[1]
        return batch

    @property
    def estimated_total_pairs(self) -> int | None:
        if self._chunks_done == 0:
            return None
        return int(self._pairs_produced / self._chunks_done * self._n_chunks)


def get_pair_stream_serial(chunk_paths: list[Path], process_chunk: Callable, epoch: int, seed: int) -> SerialPairStream:
    """Single-threaded fallback (e.g. for validation with few chunks)."""
    return SerialPairStream(chunk_paths, process_chunk, epoch, seed)