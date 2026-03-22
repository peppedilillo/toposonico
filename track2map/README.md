# track2map

Trains a Word2Vec (SGNS) track embedding from Spotify playlist co-occurrence data,
projects it to 2D with UMAP, and produces geo-normalized coordinates ready for tile map rendering.

## Setup

```sh
uv sync
```

Torch and cuML are not bundled as optional dependencies — install the right wheels for your
CUDA version manually.

CUDA 12.x:
```sh
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128
uv pip install cuml-cu12 --extra-index-url https://pypi.nvidia.com/cuml-cu12/
```

CUDA 11.8:
```sh
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118
uv pip install cuml-cu11 --extra-index-url https://pypi.nvidia.com/cuml-cu11/
```

## Environment

```sh
cp config.env.sample config.env
# edit config.env — fill in all paths
source config.env
```

| Variable | Points to |
|---|---|
| `T2M_ROOT` | Repo root (convenience base; no script reads it directly) |
| `T2M_PLAYLIST_DB` | SQLite DB with `playlists` + `playlist_tracks` tables |
| `T2M_TRACKS_DB` | SQLite DB with track/artist/album metadata |
| `T2M_TRACK_COUNT` | Output: per-track playlist counts parquet |
| `T2M_TRAINING_VOCAB` | Output: filtered vocab with `track_id` mapping |
| `T2M_TRACK_LOOKUP` | Output: track metadata joined to vocab |
| `T2M_LOOKUP_DIR` | Output dir for artist/album/label lookup parquets |
| `T2M_MODEL_DIR` | Output dir for model checkpoints |
| `T2M_EMBEDDING_DIR` | Output dir for exported embedding parquets |
| `T2M_EMBEDDING` | Path to deduplicated embedding parquet — canonical input for all downstream steps |
| `T2M_UMAP_DIR` | Output dir for UMAP projection parquets |
| `T2M_GEO_DIR` | Output dir for geo-normalized coordinate parquets |
| `T2M_KNN_DIR` | Output dir for KNN parquets |
| `T2M_DB` | Output path for the SQLite database (`build_db.py`) |

## Pipeline

### Phase 1 — Data prep (one-time; slow — full DB scan)

```sh
python scripts/build_track_counts.py
python scripts/build_training_vocab.py --min-count 5
python scripts/build_track_lookup.py
python scripts/build_playlist_chunks.py $T2M_ROOT/outs/chunks
```

- `build_training_vocab.py` prints a memory estimate — pick `--min-count` based on that
- `build_track_lookup.py` requires both `T2M_TRACKS_DB` and `T2M_TRAINING_VOCAB`
- Chunks are resumable; pass `--overwrite` to regenerate. `manifest.json` is written on completion
- Chunks store raw `track_rowid` — vocab remapping happens at training time

### Phase 2 — Training (interactive, GPU)

```sh
# open notebooks/train.ipynb
# checkpoint saved to $T2M_MODEL_DIR/model_<run>_t<size>_ep<N>_v<loss>.pt
python scripts/export_embeddings.py $T2M_MODEL_DIR/<checkpoint>.pt --output $T2M_EMBEDDING
# --track-lookup defaults to $T2M_TRACK_LOOKUP
```

Exports `embeddings_in.weight` from the checkpoint and deduplicates by ISRC: multiple
Spotify track IDs often point to the same recording (re-releases, compilations, re-uploads).
For each group sharing an ISRC, only the entry with the highest `logcounts` is kept.
Tracks with no ISRC are left untouched. Reduces vocab by ~23% on a typical run (~11M → ~8.6M).

`$T2M_EMBEDDING` is the canonical embedding used by all downstream steps.

Pass `--no-filter` to skip deduplication and write all embeddings as-is.

### Phase 3 — UMAP projection (GPU, cuML)

```sh
# open notebooks/umap.ipynb
# outputs written to $T2M_UMAP_DIR/umap_{track,album,artist,label}_2d_<run>_nn<N>_md<M>_<metric>.parquet
```

The notebook `fit_transform`s on tracks then `transform`s other entities — run all 4 entity
types in the same session to keep the coordinate space consistent.

### Phase 4 — Geo normalization

```sh
python scripts/build_geomap.py \
    --track-umap  $T2M_UMAP_DIR/umap_track_2d_<run>_nn<N>_md<M>_<metric>.parquet \
    --album-umap  $T2M_UMAP_DIR/umap_album_2d_<run>_nn<N>_md<M>_<metric>.parquet \
    --artist-umap $T2M_UMAP_DIR/umap_artist_2d_<run>_nn<N>_md<M>_<metric>.parquet \
    --label-umap  $T2M_UMAP_DIR/umap_label_2d_<run>_nn<N>_md<M>_<metric>.parquet
```

Always pass all 4 entity types together — running a subset shifts the bounding box and
breaks spatial alignment across entity types.

### Phase 5 — KNN precomputation (CPU, FAISS)

```sh
python scripts/build_knn.py
```

Computes cosine KNN for all entity types (track, album, artist, label) using FAISS on CPU.
Uses IVFFlat for large entities (>500K) and brute-force FlatIP for smaller ones.

Override per-entity K: `--k-tracks 100 --k-albums 50 --k-artists 20 --k-labels 10`

Run a subset: `--entities artist label`

Labels are keyed by name (string), not rowid. All other entities use int64 rowids.

### Phase 6 — Database build

```sh
python scripts/build_db.py
```

Assembles a single SQLite DB (`$T2M_DB`) from all parquet outputs:

- **Entity tables** (`tracks`, `albums`, `artists`, `labels`) — denormalized with geo
  coordinates for fast cross-entity navigation (e.g. `tracks` carries `artist_lon/lat`,
  `album_lon/lat`, `label_lon/lat`). Indexed on rowid.
- **KNN tables** (`track_knn`, `album_knn`, `artist_knn`, `label_knn`) — flat format
  `(entity_rowid, rank, neighbor_rowid, score)`, self-matches excluded, capped at
  `--knn-k` neighbours per entity (default: 20).

Optional: pass `--tracks-db $T2M_TRACKS_DB` to enrich artists with popularity/genre and
albums with album_type, total_tracks, release_date from the source Spotify metadata DB.

The output DB is the sole input to the `map2web` backend.

## Outputs reference

| Artifact | Path | Key columns |
|---|---|---|
| Track counts | `$T2M_TRACK_COUNT` | `track_rowid`, `playlist_count` |
| Training vocab | `$T2M_TRAINING_VOCAB` | `track_rowid`, `track_id`, `playlist_count` |
| Playlist chunks | `$T2M_ROOT/outs/chunks/chunk_NNNNNN.parquet` | `playlist_rowid`, `track_rowid`, `position`, `added_at` |
| Track lookup | `$T2M_TRACK_LOOKUP` | `track_rowid`, `track_name`, `artist_name`, `track_popularity`, `artist_rowid`, `album_rowid`, `label`, `release_date` |
| Artist lookup | `$T2M_LOOKUP_DIR/artist_lookup.parquet` | `artist_rowid`, `artist_name`, `track_count`, `mean_popularity` |
| Album lookup | `$T2M_LOOKUP_DIR/album_lookup.parquet` | `album_rowid`, `album_name`, `track_count`, `mean_popularity` |
| Label lookup | `$T2M_LOOKUP_DIR/label_lookup.parquet` | `label`, `track_count`, `mean_popularity` |
| Embeddings | `$T2M_EMBEDDING` | `track_rowid`, `e0`…`e127` (ISRC-deduplicated) |
| UMAP projection | `$T2M_UMAP_DIR/umap_{entity}_2d_<run>_nn<N>_md<M>_<metric>.parquet` | entity key, `umap_x`, `umap_y` |
| Geo coords | `$T2M_GEO_DIR/{entity}_geo.parquet` | entity key, `lon`, `lat` |
| KNN neighbors | `$T2M_KNN_DIR/{entity}_knn.parquet` | entity key, `n0`…`nK` |
| KNN scores | `$T2M_KNN_DIR/{entity}_knn_scores.parquet` | entity key, `s0`…`sK` |
| SQLite DB | `$T2M_DB` | all entity + KNN tables |
