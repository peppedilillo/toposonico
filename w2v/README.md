# annas-w2v

Trains a track embedding from the Anna's archive spotify dataset via word2vec of playlists.

## Usage:

1. Pre-compute the training vocabulary with `scripts/build_trainig_vocab.py`
2. Partition the dataset into parquet chunks with `scripts/build_playlist_chunks.py`
3. Run the training notebook `train.ipynb`