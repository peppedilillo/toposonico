# annas-w2v

Trains a track embedding from the Anna's archive spotify dataset via word2vec of playlists.

## Usage:

## Training

1. Pre-compute the training vocabulary with `scripts/build_training_vocab.py`
2. Partition the dataset into parquet chunks with `scripts/build_playlist_chunks.py`
3. Run the training notebook `train.ipynb`

## Inference

1. Compute the track lookup with `scripts/build_track_lookup.py`.
2. Run the inference notebook `inference.ipynb`