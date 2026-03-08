# annas-w2v

Trains a track embedding from the Anna's archive spotify dataset via word2vec of playlists.

## Setup

Either `pip install .` or `uv sync`.
Note Torch is included as an optional dependency. 
Use `uv sync --extra cu128` for installing with torch and CUDA 12.8 dependencies.

## Usage:

### Training

1. Pre-compute the training vocabulary with `scripts/build_training_vocab.py`
2. Partition the dataset into parquet chunks with `scripts/build_playlist_chunks.py`
3. Run the training notebook `train.ipynb`

### Inference

1. Compute the track lookup with `scripts/build_track_lookup.py`.
2. Run the inference notebook `inference.ipynb`

### Exporting

1. Export a parquet of spotify track row ids and embeddings running `scripts/export_embeddings.py`