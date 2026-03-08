# vec2map

Projects track embeddings into 2D/3D via UMAP (cuML GPU implementation), then computes
album and artist centroids as arithmetic means in that space.

## Setup

```bash
uv sync # base deps (numpy, pandas, pyarrow, jupyterlab)
uv pip install cuml-cu12 --extra-index-url https://pypi.nvidia.com/cuml-cu12/
# or if cuda11 is required:
# uv pip install cuml-cu11 --extra-index-url https://pypi.nvidia.com/cuml-cu11/
```

## Usage

### 1. Project track embeddings

```bash
python scripts/umap_tracks.py <embeddings.parquet> <out_dir> [options]

# single run
python scripts/umap_tracks.py embeddings_pure_bolt.parquet outs/ \
    --n-neighbors 100 --min-dist 0.01 --metric cosine

# parameter sweep (resumable with --skip-existing)
python scripts/umap_tracks.py embeddings_pure_bolt.parquet outs/ \
    --n-components 2 3 --n-neighbors 10 50 100 --min-dist 0.01 0.1 0.5 1.0 \
    --metric cosine euclidean --skip-existing
```

### 2. Compute album centroids

```bash
python scripts/umap_albums.py <umap.parquet> <track_lookup.parquet> <out_dir> [--min-tracks N]

python scripts/umap_albums.py outs/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \
    ../data/playlist/track_lookup.parquet outs/
```

### 3. Compute artist centroids

```bash
python scripts/umap_artists.py <umap.parquet> <track_lookup.parquet> <out_dir> [--min-tracks N]

python scripts/umap_artists.py outs/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \
    ../data/playlist/track_lookup.parquet outs/
```
