from pathlib import Path

import numpy as np
import pandas as pd

from src import sim


def _embedding_frame(rowids: list[int]) -> pd.DataFrame:
    rows: list[dict[str, float | int]] = []
    for i, rowid in enumerate(rowids):
        row = {"track_rowid": rowid}
        for dim in range(sim.EMBEDDING_DIM):
            row[f"e{dim}"] = np.float32(1.0 if dim == i else 0.0)
        rows.append(row)
    return pd.DataFrame(rows)


def _write_embedding_parquet(tmp_path: Path, rowids: list[int]) -> Path:
    path = tmp_path / "embedding.parquet"
    _embedding_frame(rowids).to_parquet(path)
    return path


def test_spec_builders_return_expected_entities_and_factories():
    filter_index = np.array([101, 102, 103], dtype=np.int64)

    track = sim.track_spec(filter_index)
    album = sim.album_spec(filter_index)
    artist = sim.artist_spec(filter_index)
    label = sim.label_spec(filter_index)

    assert track.entity == "track"
    assert track.factory_string.startswith("OPQ128_128,IVF")
    assert track.factory_string.endswith("_HNSW32,PQ128x4fsr")
    np.testing.assert_array_equal(track.filter_index, filter_index)

    assert album.entity == "album"
    assert album.factory_string.startswith("OPQ128_128,IVF")
    assert album.factory_string.endswith("_HNSW32,PQ128x4fsr")
    np.testing.assert_array_equal(album.filter_index, filter_index)

    assert artist.entity == "artist"
    assert artist.factory_string.startswith("IVF")
    assert artist.factory_string.endswith("_HNSW32,Flat")
    np.testing.assert_array_equal(artist.filter_index, filter_index)

    assert label.entity == "label"
    assert label.factory_string.startswith("IVF")
    assert label.factory_string.endswith(",Flat")
    np.testing.assert_array_equal(label.filter_index, filter_index)

    assert track.d == sim.EMBEDDING_DIM
    assert track.n == 3


def test_subsample_training_returns_input_when_small():
    xb = np.arange(12, dtype=np.float32).reshape(3, 4)

    out = sim.subsample_training(xb, max_size=5)

    assert out is xb
    np.testing.assert_array_equal(out, xb)


def test_subsample_training_returns_contiguous_subset_when_large():
    xb = np.arange(40, dtype=np.float32).reshape(10, 4)

    out = sim.subsample_training(xb, max_size=4)

    assert out.shape == (4, 4)
    assert out.flags.c_contiguous
    assert {tuple(row) for row in out}.issubset({tuple(row) for row in xb})


def test_load_filtered_embeddings_filters_reorders_and_normalizes(tmp_path):
    path = _write_embedding_parquet(tmp_path, [10, 20, 30, 40])
    spec = sim.SimIndexSpec(
        entity="track",
        factory_string="Flat",
        filter_index=np.array([30, 10], dtype=np.int64),
    )

    matrix = sim.load_filtered_embeddings(path, spec, "track_rowid")

    assert matrix.shape == (2, sim.EMBEDDING_DIM)
    assert matrix.dtype == np.float32
    np.testing.assert_allclose(np.linalg.norm(matrix, axis=1), np.ones(2, dtype=np.float32))
    np.testing.assert_array_equal(np.argmax(matrix, axis=1), np.array([2, 0]))


def test_train_index_returns_searchable_id_map(monkeypatch):
    monkeypatch.setattr(sim, "ivf_compute_nlist", lambda n: 2)
    spec = sim.label_spec(np.array([101, 102, 103, 104], dtype=np.int64))
    xb = np.ascontiguousarray(_embedding_frame([101, 102, 103, 104]).filter(regex=r"^e\d+$").to_numpy(dtype=np.float32))

    index = sim.train_index(spec, xb)

    assert index.ntotal == 4
    assert index.d == sim.EMBEDDING_DIM

    distances, ids = index.search(xb, 1)

    assert distances.shape == (4, 1)
    np.testing.assert_array_equal(ids[:, 0], spec.filter_index)


def test_save_and_load_index_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(sim, "ivf_compute_nlist", lambda n: 2)
    spec = sim.label_spec(np.array([201, 202, 203, 204], dtype=np.int64))
    xb = np.ascontiguousarray(_embedding_frame([201, 202, 203, 204]).filter(regex=r"^e\d+$").to_numpy(dtype=np.float32))
    index = sim.train_index(spec, xb)
    path = tmp_path / "sim.index"

    sim.save_index(index, path)
    loaded = sim.load_index(path)

    assert path.is_file()
    assert loaded.ntotal == index.ntotal
    assert loaded.d == index.d
    _, ids = loaded.search(xb, 1)
    np.testing.assert_array_equal(ids[:, 0], spec.filter_index)
