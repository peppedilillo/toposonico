from pathlib import Path
import random

import numpy as np
import pandas as pd
import torch
from torch.optim import SparseAdam

from src.data import build_vocab_from_chunks
from src.data import get_nsampler
from src.data import init_chunk_processor
from src.data import PrefetchPairStream
from src.data import split
from src.model import skipgram_loss
from src.model import Word2Vec


def _make_chunks(
    tmp_path: Path, n_chunks: int, n_playlists: int, vocab_size: int, seed: int
) -> list[Path]:
    """Synthetic parquet chunks: random playlists with random track rowids."""
    rng = np.random.default_rng(seed)
    paths = []
    for i in range(n_chunks):
        rows = []
        for pid in range(i * n_playlists, (i + 1) * n_playlists):
            length = int(rng.integers(3, 12))
            for tid in rng.integers(0, vocab_size, size=length):
                rows.append(
                    {"playlist_rowid": np.int32(pid), "track_rowid": np.int64(tid)}
                )
        path = tmp_path / f"chunk_{i:06d}.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False)
        paths.append(path)
    return paths


def _run(chunk_paths: list[Path], seed: int, n_workers: int) -> torch.Tensor:
    """Full training loop (CPU, 2 epochs). Returns final input embeddings."""
    SEED = seed
    W, K, NBLOCK = 2, 5, 4
    EMBED_DIM, BATCH_SIZE = 16, 128

    train_paths, _ = split(chunk_paths, valid_fraction=0.2, seed=SEED)
    SEED += 1

    vocab = build_vocab_from_chunks(chunk_paths, cmin=1)

    counts = vocab["playlist_count"].values.astype(np.float64)
    w75 = counts**0.75
    neg_sample, _ = get_nsampler(
        torch.tensor(w75 / w75.sum(), dtype=torch.float32),
        K,
        BATCH_SIZE,
        NBLOCK,
    )
    process_chunk = init_chunk_processor(vocab, W)

    torch.manual_seed(SEED)
    SEED += 1
    model = Word2Vec(vocab_size=len(vocab), embed_dim=EMBED_DIM)
    optimizer = SparseAdam(model.parameters(), lr=1e-3)

    for epoch in range(2):
        random.seed(SEED + epoch)
        torch.manual_seed(SEED + epoch)
        random.shuffle(train_paths)

        stream = PrefetchPairStream(
            train_paths,
            process_chunk,
            epoch=epoch,
            seed=SEED,
            n_workers=n_workers,
        )
        model.train()
        while True:
            batch = stream.next_batch(BATCH_SIZE)
            if batch.shape[1] == 0:
                break
            c, x = batch[0], batch[1]
            n = neg_sample(len(c))
            optimizer.zero_grad()
            skipgram_loss(*model(c, x, n)).backward()
            optimizer.step()

    return model.track_embeddings


def test_deterministic_single_worker(tmp_path):
    paths = _make_chunks(tmp_path, n_chunks=6, n_playlists=20, vocab_size=80, seed=42)
    assert torch.equal(
        _run(paths, seed=0, n_workers=1), _run(paths, seed=0, n_workers=1)
    )


def test_deterministic_multi_worker(tmp_path):
    paths = _make_chunks(tmp_path, n_chunks=6, n_playlists=20, vocab_size=80, seed=42)
    assert torch.equal(
        _run(paths, seed=0, n_workers=4), _run(paths, seed=0, n_workers=4)
    )


def test_different_seeds_differ(tmp_path):
    """Sanity check: different seeds must produce different embeddings."""
    paths = _make_chunks(tmp_path, n_chunks=6, n_playlists=20, vocab_size=80, seed=42)
    assert not torch.equal(
        _run(paths, seed=0, n_workers=1), _run(paths, seed=1, n_workers=1)
    )


def test_training_completes_with_zero_valid_fraction(tmp_path):
    paths = _make_chunks(tmp_path, n_chunks=6, n_playlists=20, vocab_size=80, seed=42)

    train_paths, valid_paths = split(paths, valid_fraction=0.0, seed=0)

    # All chunks go to training; nothing held out
    assert valid_paths == []
    assert len(train_paths) == len(paths)

    # Training loop runs to completion without error
    W, K, NBLOCK = 2, 5, 4
    EMBED_DIM, BATCH_SIZE, SEED = 16, 128, 0

    vocab = build_vocab_from_chunks(paths, cmin=1)
    counts = vocab["playlist_count"].values.astype(np.float64)
    w75 = counts**0.75
    neg_sample, _ = get_nsampler(
        torch.tensor(w75 / w75.sum(), dtype=torch.float32),
        K,
        BATCH_SIZE,
        NBLOCK,
    )
    process_chunk = init_chunk_processor(vocab, W)

    torch.manual_seed(SEED)
    model = Word2Vec(vocab_size=len(vocab), embed_dim=EMBED_DIM)
    optimizer = SparseAdam(model.parameters(), lr=1e-3)

    stream = PrefetchPairStream(
        train_paths, process_chunk, epoch=0, seed=SEED, n_workers=1
    )
    model.train()
    while True:
        batch = stream.next_batch(BATCH_SIZE)
        if batch.shape[1] == 0:
            break
        c, x = batch[0], batch[1]
        n = neg_sample(len(c))
        optimizer.zero_grad()
        skipgram_loss(*model(c, x, n)).backward()
        optimizer.step()

    emb = model.track_embeddings
    assert emb.shape == (len(vocab), EMBED_DIM)
    assert torch.isfinite(emb).all()
