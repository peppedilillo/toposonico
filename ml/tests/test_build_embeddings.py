from pathlib import Path
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import torch

os.environ.setdefault("SICK_ARTIST_MINTRACK", "2")
os.environ.setdefault("SICK_ALBUM_MINTRACK", "2")
os.environ.setdefault("SICK_LABEL_MINTRACK", "2")

from src.entities import Albums
from src.entities import Artists
from src.entities import Labels
from src.entities import Tracks


def _write_t1_vocab(path: Path) -> None:
    pd.DataFrame(
        {
            "track_rowid": pd.Series([10, 20, 30, 40], dtype="int64"),
            "track_id": pd.Series([0, 1, 2, 3], dtype="int32"),
            "playlist_count": pd.Series([10, 100, 50, 1000], dtype="int32"),
            "artist_rowid": pd.Series([1, 1, 1, 2], dtype="int64"),
            "album_rowid": pd.Series([11, 11, 11, 22], dtype="int64"),
            "label_rowid": pd.Series([111, 111, None, 222], dtype="Int32"),
            "id_isrc": pd.Series(["ISRC10", "ISRC20", "ISRC30", "ISRC40"], dtype="string"),
        }
    ).to_parquet(path, index=False)


def _write_model(path: Path) -> None:
    torch.save(
        {
            "vocab": {"track_rowid": np.array([10, 20, 30], dtype=np.int64)},
            "hparams": {"embed_dim": 2},
            "model_state_dict": {
                "embeddings_in.weight": torch.tensor(
                    [[1.0, 1.5], [2.0, 2.5], [3.0, 3.5]], dtype=torch.float32
                )
            },
        },
        path,
    )


def _load_t1_df(path: Path) -> pd.DataFrame:
    t1_df = pd.read_parquet(
        path,
        columns=[
            "track_rowid",
            "playlist_count",
            "artist_rowid",
            "album_rowid",
            "label_rowid",
        ],
    )
    t1_df["track_rowid"] = t1_df["track_rowid"].astype("int64")
    t1_df["playlist_count"] = t1_df["playlist_count"].astype("int32")
    t1_df["artist_rowid"] = t1_df["artist_rowid"].astype("int64")
    t1_df["album_rowid"] = t1_df["album_rowid"].astype("int64")
    t1_df["label_rowid"] = t1_df["label_rowid"].astype("Int32")
    return t1_df


def test_build_embeddings_script_writes_expected_embedding_artifacts(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    t1_path = tmp_path / "vocab_t1.parquet"
    model_path = tmp_path / "model.pt"
    track_output = tmp_path / "embedding_track.parquet"
    artist_output = tmp_path / "embedding_artist.parquet"
    album_output = tmp_path / "embedding_album.parquet"
    label_output = tmp_path / "embedding_label.parquet"

    _write_t1_vocab(t1_path)
    _write_model(model_path)

    env = os.environ.copy()
    env["SICK_ARTIST_MINTRACK"] = "2"
    env["SICK_ALBUM_MINTRACK"] = "2"
    env["SICK_LABEL_MINTRACK"] = "2"

    subprocess.run(
        [
            sys.executable,
            "scripts/build_embeddings.py",
            str(model_path),
            "--input",
            str(t1_path),
            "--chunk-size",
            "1",
            "--track-output",
            str(track_output),
            "--artist-output",
            str(artist_output),
            "--album-output",
            str(album_output),
            "--label-output",
            str(label_output),
        ],
        cwd=repo_root,
        env=env,
        check=True,
    )

    track_embeddings = pd.read_parquet(track_output)
    artist_embeddings = pd.read_parquet(artist_output)
    album_embeddings = pd.read_parquet(album_output)
    label_embeddings = pd.read_parquet(label_output)

    assert list(track_embeddings.columns) == ["track_rowid", "e0", "e1"]
    assert list(artist_embeddings.columns) == ["artist_rowid", "e0", "e1"]
    assert list(album_embeddings.columns) == ["album_rowid", "e0", "e1"]
    assert list(label_embeddings.columns) == ["label_rowid", "e0", "e1"]

    t1_df = _load_t1_df(t1_path)
    model_dict = torch.load(model_path, map_location="cpu", weights_only=False)

    pd.testing.assert_frame_equal(
        track_embeddings, Tracks.embeddings(t1_df, model_dict), check_dtype=True
    )
    pd.testing.assert_frame_equal(
        artist_embeddings, Artists.embeddings(t1_df, model_dict), check_dtype=True
    )
    pd.testing.assert_frame_equal(
        album_embeddings, Albums.embeddings(t1_df, model_dict), check_dtype=True
    )
    pd.testing.assert_frame_equal(
        label_embeddings, Labels.embeddings(t1_df, model_dict), check_dtype=True
    )
