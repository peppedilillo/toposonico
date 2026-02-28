# spotify-rec

## Setup
```bash
uv sync                  # default: PyTorch with latest CUDA
uv sync --extra cu118    # PyTorch with CUDA 11.8 (older GPUs, e.g. RTX 2060)
```

## Cloud Computing
From project root, to vm:
```bash
rsync -avz --exclude '.idea' --exclude 'data/raw'  --exclude 'data/clean' --exclude '.venv' --exclude '__pycache__' ./ ubuntu@150.136.147.169:~/spotify-rec/
```

to local machine:
```bash
rsync -avz --exclude '.idea' --exclude 'data/raw' --exclude 'data/clean' --exclude '.venv' --exclude '__pycache__' ubuntu@150.136.147.169:~/spotify-rec/ ./
```

## Data Pipeline

### 1. Merge databases
```bash
python scripts/merge_databases.py \
    --spotify <spotify_clean.sqlite3> \
    --audio <audio_features.sqlite3> \
    <output_merged.sqlite3>
```

### 2. Extract metadata
```bash
python scripts/extract_metadata.py <merged.sqlite3> -p 50 -g
```
- `-p`: popularity threshold (default: 80)
- `-g`: include artist genres

Output: `data/raw/training_pop{POP}[_genres].parquet`

### 3. Clean metadata
```bash
python scripts/clean_metadata.py <raw_dataset.parquet>
```
Input defaults to `data/raw/` if filename only.
Output: `data/clean/<filename>.parquet`

### 4. Run feature selection
```bash
python scripts/engineer_features <clean_dataset.parquet>
```

Input defaults to `data/clean/` if filename only.
Output: `data/engineered/<filename>.parquet`

---

## Playlist2Vec Data Assets

One-time builds from the playlist database. Results are written to `data/playlist/`.

### Build global track vocabulary
```bash
uv run scripts/build_track_vocab.py <spotify_clean_playlists.sqlite3>
```
Scans all 1.7B rows of `playlist_tracks` and writes every unique `track_rowid`
(filtered: no episodes, no local files, no nulls) with a stable sequential index.

Output: `data/playlist/global_track_vocab.parquet` — columns: `track_rowid, index`

> **Note:** full scan takes ~45 min on HDD. Run once and cache.
> Confirmed size: 47.3M tracks, 399 MB.

### Build track lookup table
```bash
uv run scripts/build_track_lookup.py <spotify_clean.sqlite3> \
    --vocab data/playlist/global_track_vocab.parquet
```
Extracts display metadata (name, artist, album, popularity, release date, ISRC, label)
for all tracks in the vocab. Used to inspect nearest-neighbour results during evaluation.

Output: `data/playlist/track_lookup.parquet`

- `--vocab`: recommended — filters output to tracks that appear in playlists.
  Without it, the join fans out to ~256M rows due to the DB schema.
- `-o`: override output path
- `--chunk-size`: rows per chunk for streaming write (default: 500,000)

### Build training artefacts (for full-scale / cloud training)

Run in order:

**1. Per-track playlist counts** — full scan, run once:
```bash
uv run scripts/build_track_counts.py <spotify_clean_playlists.sqlite3>
```
Output: `data/playlist/track_playlist_counts.parquet` — columns: `track_rowid, playlist_count`

> Full table scan of ~1.7B rows. Takes a while on HDD.

**2. Training vocabulary** — filter rare tracks, assign contiguous IDs:
```bash
uv run scripts/build_training_vocab.py data/playlist/track_playlist_counts.parquet \
    [--min-count N]
```
Output: `data/playlist/training_vocab.parquet` — columns: `track_rowid, track_id, playlist_count`

Run once without `--min-count` to inspect the count distribution and memory estimates,
then re-run with a threshold that fits your VRAM budget.

**3. Playlist chunks** — export all playlists as parquet chunks for streaming training:
```bash
uv run scripts/build_playlist_chunks.py <spotify_clean_playlists.sqlite3> \
    [--chunk-size 100000]
```
Output: `data/playlist/chunks/chunk_NNNNNN.parquet` + `manifest.json`

Chunks store raw `track_rowid`; remapping to `track_id` happens at training time via
`training_vocab.parquet`. Resumable — existing chunks are skipped unless `--overwrite` is passed.

