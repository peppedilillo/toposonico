# Optimise `13_training_w2v.py` for cloud training

## Context

The Playlist2Vec training script processes parquet chunks sequentially on CPU (read,
merge, subsample, pair-generate) then feeds batches to the GPU. The GPU work (sparse
embedding lookup + dot product + logsigmoid) is trivially cheap — the entire per-epoch
wall time is CPU-bound preprocessing. On a cloud A100 billed by the minute, every
second of GPU idle time is wasted money.

Production scale: 67 chunks × ~200MB each, 6.6M playlists, up to 47M-track vocab.

---

## Optimisations (highest → lowest impact)

### 1. Prefetch chunks with background threads
**Problem**: `get_pair_stream` calls `process_chunk` synchronously — the GPU blocks
waiting for each chunk to be read, merged, subsampled, and pair-generated.

**Fix**: Thread pool (2–4 workers) + bounded `Queue`. Workers call `process_chunk` and
put tensors into the queue; the training loop pops from it. Pandas/numpy release the
GIL, so threads give true parallelism for this workload. Each chunk gets its own RNG
seeded from `SEED + epoch * 10_000 + chunk_index` (thread-safe, deterministic).

Replace `get_pair_stream` with a `PrefetchPairStream` class that manages the thread
pool and buffer internally.

### 2. Load `training_vocab.parquet` instead of scanning chunks
**Problem**: Lines 40–54 scan every chunk with a `Counter` to build vocab. For
production (67 × 200MB = ~13GB I/O) this takes minutes of pure waste —
`training_vocab.parquet` (260MB, already on disk) has identical data.

**Fix**: `pd.read_parquet(PATH_VOCAB)`, filter by CMIN, re-index `track_id`.
Eliminates the entire initial scan.

### 3. Vectorise `precompute_pairs` — eliminate Python loop over playlists
**Problem**: `flatten()` (groupby→apply(list)→to_dict) then a list comprehension
calling `playlist_pairs()` per playlist (~100K iterations per chunk), each creating
a tensor and doing `torch.roll` W times.

**Fix**: Work entirely in numpy. Find playlist boundaries with `np.diff` on sorted
`playlist_rowid`. Build per-track metadata (`position_in_playlist`, `playlist_start`,
`playlist_length`) with `np.repeat`. Then loop over the 2W offsets (only ~10
iterations) — for each offset, mask tracks whose playlist is long enough and compute
context indices with `(pos + k) % length` vectorised across all tracks at once.
Single `torch.from_numpy` at the end. This replaces ~100K Python iterations with ~10
numpy broadcast operations.

### 4. Replace pandas merge with `np.searchsorted` for OOV filtering
**Problem**: `pt.merge(vocab, on="track_rowid", how="inner")` is a pandas hash join
on millions of rows per chunk.

**Fix**: Since `training_vocab` is sorted by `track_rowid`, use `np.searchsorted` to
do binary search lookup. Zero extra memory, much faster than a hash join. Build
`VOCAB_ROWIDS` and `VOCAB_TIDS` arrays once at startup.

### 5. Vectorise `subsample` with numpy array indexing
**Problem**: `pt["track_id"].map(keep_prob)` does pandas Series dictionary lookup.

**Fix**: Pre-compute `KEEP_PROB` as a numpy array indexed by `track_id` (contiguous
0..vocab_size-1). Then `KEEP_PROB[pt["track_id"].values]` is a single numpy advanced
index operation.

### 6. Pre-sample negatives in blocks
**Problem**: `torch.multinomial` called every training step (kernel launch overhead).

**Fix**: Pre-sample a large block (e.g. 128 batches worth) in one call, slice per
step. Refill when exhausted.

### 7. Pin memory on `process_chunk` output
**Fix**: `.pin_memory()` on the returned pair tensor so that `.to(device,
non_blocking=True)` in the training loop truly overlaps the transfer.

### 8. Minor cleanups
- `build_weights`: drop `.sort_index()` (vocab already sorted by track_id)
- `list.pop(0)` → `deque.popleft()` (moot after #1, but apply in validation stream)
- Validation: use larger batch size (no gradients → can double it)
- BATCH_SIZE: make configurable, note in comments that 512K+ is appropriate for A100

---

## Implementation order

| Step | Change | Why this order |
|------|--------|----------------|
| 1 | Load `training_vocab.parquet` (#2) | Trivial, standalone, immediate payoff |
| 2 | Vectorise `subsample` (#5) | Simple, no dependencies |
| 3 | Replace merge with `np.searchsorted` (#4) | Simple, standalone |
| 4 | Vectorise `precompute_pairs` (#3) | Most complex; self-contained |
| 5 | Clean up `build_weights` (#8) | Trivial |
| 6 | Prefetch thread pool (#1) | Depends on process_chunk being efficient (steps 1-4) |
| 7 | Pin memory (#7) | Goes into process_chunk, pairs with #6 |
| 8 | Block-sample negatives (#6) | Standalone |
| 9 | Minor cleanups (#8 remainder) | Polish |

After each step we run training on mini_chunks to verify correctness and measure
speedup.

---

## Files to modify

- `notebooks/13_training_w2v.py` — all changes go here

## Files to reference (read-only)

- `data/playlist/training_vocab.parquet` — pre-built vocab (260MB)
- `scripts/build_training_vocab.py` — vocab schema reference
- `data/playlist/mini_chunks/manifest.json` — 11 chunks, 105K playlists (test data)

## Verification

After all changes:
1. Run on `mini_chunks/` — verify training completes, loss curve is comparable to
   before optimisation (not bitwise identical due to reordering, but same ballpark)
2. Compare wall time per epoch before/after
3. Check GPU utilisation with `nvidia-smi` during training — should show near-100%
   instead of bursty usage
4. Verify qualitative nearest-neighbour results are reasonable
