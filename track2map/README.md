# annas-w2v

Trains a track embedding from the Anna's archive spotify dataset via word2vec of playlists.

## Setup

```
uv sync
```

Note Torch and CuML are not included as an optional dependency. 
This is intended as you may need to install the right wheels for your architecture.
For installing torch and cuML for CUDA 12.8:
```bash
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128
uv pip install cuml-cu12 --extra-index-url https://pypi.nvidia.com/cuml-cu12/
```
While for CUDA 11.8:
```bash
uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118
uv pip install cuml-cu11 --extra-index-url https://pypi.nvidia.com/cuml-cu11/
```

## Usage:

### Training

1. Set the environment variable with `source config.env`
2. Pre-compute playlist track counts with `scripts/build_track_counts.py`
3. Pre-compute the training vocabulary with `scripts/build_training_vocab.py`
4. Partition the dataset into parquet chunks with `scripts/build_playlist_chunks.py path/to/chunkdir`
5. Run the training notebook `train.ipynb`

### Inference

1. Compute the track lookup with `scripts/build_track_lookup.py`.
2. Run the inference notebook `inference.ipynb`

### Exporting

1. Export a parquet of spotify track row ids and embeddings running `scripts/export_embeddings.py`