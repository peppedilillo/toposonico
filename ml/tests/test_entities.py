import os

import numpy as np
import pandas as pd
import pytest
import torch

os.environ.setdefault("SICK_ARTIST_MINTRACK", "2")
os.environ.setdefault("SICK_ALBUM_MINTRACK", "2")
os.environ.setdefault("SICK_LABEL_MINTRACK", "2")

from src.entities import Albums
from src.entities import Artists
from src.entities import Labels
from src.entities import Tracks


def _model_dict() -> dict:
    return {
        "vocab": {"track_rowid": np.array([10, 20, 30], dtype=np.int64)},
        "hparams": {"embed_dim": 2},
        "model_state_dict": {
            "embeddings_in.weight": torch.tensor(
                [[1.0, 1.5], [2.0, 2.5], [3.0, 3.5]], dtype=torch.float32
            )
        },
    }


def _t1_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "track_rowid": pd.Series([10, 20, 30, 40], dtype="int64"),
            "playlist_count": pd.Series([10, 100, 50, 1000], dtype="int32"),
            "artist_rowid": pd.Series([1, 1, 1, 2], dtype="int64"),
            "album_rowid": pd.Series([11, 11, 11, 22], dtype="int64"),
            "label_rowid": pd.Series([111, 111, None, 222], dtype="Int32"),
        }
    )


def test_tracks_valid_ids_intersects_t1_and_checkpoint():
    valid_ids = Tracks.valid_ids(_t1_df(), _model_dict())
    assert valid_ids.tolist() == [10, 20]


def test_tracks_valid_ids_requires_unique_track_rowid():
    t1_df = pd.concat([_t1_df(), _t1_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(AssertionError, match="unique track_rowid"):
        Tracks.valid_ids(t1_df, _model_dict())


def test_tracks_lookup_returns_checkpoint_tracks_with_logcounts():
    out = Tracks.lookup(_t1_df(), _model_dict())

    assert out["track_rowid"].tolist() == [10, 20]
    assert list(out.columns) == ["track_rowid", "logcounts"]
    assert out["track_rowid"].dtype == np.int64
    assert out["logcounts"].dtype == np.float32
    np.testing.assert_allclose(
        out["logcounts"].to_numpy(),
        np.array([1.0, 2.0], dtype=np.float32),
    )


def test_tracks_embeddings_returns_checkpoint_embeddings_for_t1_tracks():
    out = Tracks.embeddings(_t1_df(), _model_dict())

    assert out["track_rowid"].tolist() == [10, 20]
    assert list(out.columns) == ["track_rowid", "e0", "e1"]
    assert out["track_rowid"].dtype == np.int64
    assert out["e0"].dtype == np.float32
    assert out["e1"].dtype == np.float32
    np.testing.assert_allclose(
        out[["e0", "e1"]].to_numpy(),
        np.array([[1.0, 1.5], [2.0, 2.5]], dtype=np.float32),
    )


def test_unlabeled_checkpoint_track_is_excluded_from_exported_entities():
    t1_df = _t1_df()
    model_dict = _model_dict()

    assert 30 not in Tracks.valid_ids(t1_df, model_dict).tolist()
    assert Artists.lookup(t1_df, model_dict)["artist_rowid"].tolist() == [1]
    assert Albums.lookup(t1_df, model_dict)["album_rowid"].tolist() == [11]
    assert Labels.lookup(t1_df, model_dict)["label_rowid"].tolist() == [111]

    np.testing.assert_allclose(
        Artists.lookup(t1_df, model_dict)["logcounts"].to_numpy(),
        np.array([1.5], dtype=np.float32),
    )
    np.testing.assert_allclose(
        Albums.lookup(t1_df, model_dict)["logcounts"].to_numpy(),
        np.array([1.5], dtype=np.float32),
    )


@pytest.mark.parametrize(
    ("entity_cls", "id_col", "lookup_cols", "embedding_cols"),
    [
        (
            Tracks,
            "track_rowid",
            ["track_rowid", "logcounts"],
            ["track_rowid", "e0", "e1"],
        ),
        (
            Artists,
            "artist_rowid",
            ["artist_rowid", "logcounts"],
            ["artist_rowid", "e0", "e1"],
        ),
        (
            Albums,
            "album_rowid",
            ["album_rowid", "logcounts"],
            ["album_rowid", "e0", "e1"],
        ),
        (
            Labels,
            "label_rowid",
            ["label_rowid", "logcounts"],
            ["label_rowid", "e0", "e1"],
        ),
    ],
)
def test_entity_lookup_and_embedding_columns_match(
    entity_cls, id_col, lookup_cols, embedding_cols
):
    lookup = entity_cls.lookup(_t1_df(), _model_dict())
    embeddings = entity_cls.embeddings(_t1_df(), _model_dict())

    assert list(lookup.columns) == lookup_cols
    assert list(embeddings.columns) == embedding_cols
    assert lookup[id_col].tolist() == embeddings[id_col].tolist()
