from pathlib import Path
import os
import sqlite3
import subprocess
import sys

import numpy as np
import pandas as pd
import torch


def _write_tracks_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE artists (
            rowid INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE albums (
            rowid INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            label TEXT,
            release_date TEXT
        );

        CREATE TABLE tracks (
            rowid INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            popularity INTEGER,
            external_id_isrc TEXT,
            album_rowid INTEGER NOT NULL
        );

        CREATE TABLE track_artists (
            track_rowid INTEGER NOT NULL,
            artist_rowid INTEGER NOT NULL
        );
        """
    )
    conn.executemany(
        "INSERT INTO artists(rowid, name) VALUES (?, ?)",
        [(1, "Artist One"), (2, "Artist Two")],
    )
    conn.executemany(
        "INSERT INTO albums(rowid, name, label, release_date) VALUES (?, ?, ?, ?)",
        [
            (11, "Album One", "Label One", "2024-01-01"),
            (22, "Album Two", "Label Two", "2023-05-05"),
        ],
    )
    conn.executemany(
        "INSERT INTO tracks(rowid, name, popularity, external_id_isrc, album_rowid) VALUES (?, ?, ?, ?, ?)",
        [
            (10, "Track One", 7, "ISRC10", 11),
            (20, "Track Two", 9, "ISRC20", 11),
            (30, "Track Null Label", 8, "ISRC30", 11),
            (40, "Track Three", 5, "ISRC40", 22),
        ],
    )
    conn.executemany(
        "INSERT INTO track_artists(track_rowid, artist_rowid) VALUES (?, ?)",
        [(10, 1), (20, 1), (30, 1), (40, 2)],
    )
    conn.commit()
    conn.close()


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


def test_build_lookups_script_writes_expected_lookup_artifacts(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    db_path = tmp_path / "tracks.db"
    t1_path = tmp_path / "vocab_t1.parquet"
    model_path = tmp_path / "model.pt"
    track_output = tmp_path / "lookup_track.parquet"
    artist_output = tmp_path / "lookup_artist.parquet"
    album_output = tmp_path / "lookup_album.parquet"
    label_output = tmp_path / "lookup_label.parquet"

    _write_tracks_db(db_path)
    _write_t1_vocab(t1_path)
    _write_model(model_path)

    env = os.environ.copy()
    env["SICK_ARTIST_MINTRACK"] = "2"
    env["SICK_ALBUM_MINTRACK"] = "2"
    env["SICK_LABEL_MINTRACK"] = "2"

    subprocess.run(
        [
            sys.executable,
            "scripts/build_lookups.py",
            str(model_path),
            "--database",
            str(db_path),
            "--input",
            str(t1_path),
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

    track_lookup = pd.read_parquet(track_output)
    artist_lookup = pd.read_parquet(artist_output)
    album_lookup = pd.read_parquet(album_output)
    label_lookup = pd.read_parquet(label_output)

    assert list(track_lookup.columns) == [
        "track_rowid",
        "track_name",
        "artist_rowid",
        "artist_name",
        "album_rowid",
        "album_name",
        "track_popularity",
        "release_date",
        "id_isrc",
        "label",
        "label_rowid",
        "logcounts",
    ]
    assert track_lookup["track_rowid"].tolist() == [10, 20]
    assert track_lookup["track_name"].tolist() == ["Track One", "Track Two"]
    assert track_lookup["label_rowid"].tolist() == [111, 111]
    assert 30 not in track_lookup["track_rowid"].tolist()
    np.testing.assert_allclose(
        track_lookup["logcounts"].to_numpy(),
        np.array([1.0, 2.0], dtype=np.float32),
    )

    assert list(artist_lookup.columns) == ["artist_rowid", "artist_name", "logcounts"]
    assert artist_lookup["artist_rowid"].tolist() == [1]
    assert artist_lookup["artist_name"].tolist() == ["Artist One"]
    np.testing.assert_allclose(
        artist_lookup["logcounts"].to_numpy(),
        np.array([1.5], dtype=np.float32),
    )

    assert list(album_lookup.columns) == [
        "album_rowid",
        "album_name",
        "artist_rowid",
        "artist_name",
        "logcounts",
    ]
    assert album_lookup["album_rowid"].tolist() == [11]
    assert album_lookup["album_name"].tolist() == ["Album One"]
    assert album_lookup["artist_rowid"].tolist() == [1]
    assert album_lookup["artist_name"].tolist() == ["Artist One"]
    np.testing.assert_allclose(
        album_lookup["logcounts"].to_numpy(),
        np.array([1.5], dtype=np.float32),
    )

    assert list(label_lookup.columns) == ["label_rowid", "label", "logcounts"]
    assert label_lookup["label_rowid"].tolist() == [111]
    assert label_lookup["label"].tolist() == ["Label One"]
    np.testing.assert_allclose(
        label_lookup["logcounts"].to_numpy(),
        np.array([1.5], dtype=np.float32),
    )
