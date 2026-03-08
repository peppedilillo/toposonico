import numpy as np
import pandas as pd
import pytest

from src.topo import album_embeddings, artist_embeddings, label_embeddings


def _emb(*track_rowids):
    n = len(track_rowids)
    return pd.DataFrame(
        {
            "track_rowid": list(track_rowids),
            "e0": np.arange(n, dtype="float32"),
            "e1": np.arange(n, dtype="float32") * 0.1,
        }
    )


def _lookup_artist(*pairs):
    """pairs: (track_rowid, artist_rowid)"""
    return pd.DataFrame(pairs, columns=["track_rowid", "artist_rowid"])


def _lookup_album(*pairs):
    """pairs: (track_rowid, album_rowid)"""
    return pd.DataFrame(pairs, columns=["track_rowid", "album_rowid"])


def _lookup_label(*pairs):
    """pairs: (track_rowid, label)"""
    return pd.DataFrame(pairs, columns=["track_rowid", "label"])


# --- artist_embeddings ---

def test_artist_columns():
    emb = _emb(1, 2)
    lookup = _lookup_artist((1, 10), (2, 10))
    result = artist_embeddings(emb, lookup)
    assert list(result.columns) == ["artist_rowid", "e0", "e1"]


def test_artist_mean_pool():
    emb = _emb(1, 2)
    lookup = _lookup_artist((1, 10), (2, 10))
    result = artist_embeddings(emb, lookup)
    assert len(result) == 1
    assert result.loc[0, "e0"] == pytest.approx(0.5)


def test_artist_min_tracks_filter():
    emb = _emb(1, 2, 3)
    lookup = _lookup_artist((1, 10), (2, 10), (3, 20))
    result = artist_embeddings(emb, lookup, min_tracks=2)
    assert len(result) == 1
    assert result.loc[0, "artist_rowid"] == 10


def test_artist_dtype():
    emb = _emb(1, 2)
    lookup = _lookup_artist((1, 10), (2, 20))
    result = artist_embeddings(emb, lookup)
    assert result["e0"].dtype == np.float32
    assert result["artist_rowid"].dtype == np.int64


def test_artist_no_overlap():
    emb = _emb(1, 2)
    lookup = _lookup_artist((3, 10), (4, 20))
    result = artist_embeddings(emb, lookup)
    assert len(result) == 0


# --- album_embeddings ---

def test_album_columns():
    emb = _emb(1, 2)
    lookup = _lookup_album((1, 100), (2, 100))
    result = album_embeddings(emb, lookup)
    assert list(result.columns) == ["album_rowid", "e0", "e1"]


def test_album_mean_pool():
    emb = _emb(1, 2)
    lookup = _lookup_album((1, 100), (2, 100))
    result = album_embeddings(emb, lookup)
    assert len(result) == 1
    assert result.loc[0, "e0"] == pytest.approx(0.5)


def test_album_min_tracks_filter():
    emb = _emb(1, 2, 3)
    lookup = _lookup_album((1, 100), (2, 100), (3, 200))
    result = album_embeddings(emb, lookup, min_tracks=2)
    assert len(result) == 1
    assert result.loc[0, "album_rowid"] == 100


def test_album_dtype():
    emb = _emb(1, 2)
    lookup = _lookup_album((1, 100), (2, 200))
    result = album_embeddings(emb, lookup)
    assert result["e0"].dtype == np.float32
    assert result["album_rowid"].dtype == np.int64


# --- label_embeddings ---

def test_label_columns():
    emb = _emb(1, 2)
    lookup = _lookup_label((1, "Sony"), (2, "Sony"))
    result = label_embeddings(emb, lookup)
    assert list(result.columns) == ["label", "e0", "e1"]


def test_label_mean_pool():
    emb = _emb(1, 2)
    lookup = _lookup_label((1, "Sony"), (2, "Sony"))
    result = label_embeddings(emb, lookup)
    assert len(result) == 1
    assert result.loc[0, "e0"] == pytest.approx(0.5)


def test_label_two_groups():
    emb = _emb(1, 2, 3, 4)
    lookup = _lookup_label((1, "Sony"), (2, "Sony"), (3, "WMG"), (4, "WMG"))
    result = label_embeddings(emb, lookup, min_tracks=2)
    assert len(result) == 2
    assert set(result["label"]) == {"Sony", "WMG"}


def test_label_min_tracks_filter():
    emb = _emb(1, 2, 3)
    lookup = _lookup_label((1, "Sony"), (2, "Sony"), (3, "WMG"))
    result = label_embeddings(emb, lookup, min_tracks=2)
    assert len(result) == 1
    assert result.loc[0, "label"] == "Sony"


def test_label_dtype():
    emb = _emb(1, 2)
    lookup = _lookup_label((1, "Sony"), (2, "WMG"))
    result = label_embeddings(emb, lookup)
    assert result["e0"].dtype == np.float32
    assert result["label"].dtype == object


def test_label_no_overlap():
    emb = _emb(1, 2)
    lookup = _lookup_label((3, "Sony"), (4, "WMG"))
    result = label_embeddings(emb, lookup)
    assert len(result) == 0


def test_label_drops_nan():
    emb = _emb(1, 2, 3)
    lookup = pd.DataFrame({"track_rowid": [1, 2, 3], "label": ["Sony", None, "Sony"]})
    result = label_embeddings(emb, lookup)
    assert len(result) == 1
    assert result.loc[0, "label"] == "Sony"


def test_label_drops_empty_string():
    emb = _emb(1, 2, 3)
    lookup = pd.DataFrame({"track_rowid": [1, 2, 3], "label": ["Sony", "", "Sony"]})
    result = label_embeddings(emb, lookup)
    assert len(result) == 1
    assert result.loc[0, "label"] == "Sony"
