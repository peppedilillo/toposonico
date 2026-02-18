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

