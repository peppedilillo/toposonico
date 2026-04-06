import sqlite3

import faiss
import numpy as np
import pytest

from src.shared import FaissIndexes

DIM = 128


def _make_embeddings(n: int, rng: np.random.Generator) -> np.ndarray:
    emb = rng.standard_normal((n, DIM)).astype(np.float32)
    emb /= np.linalg.norm(emb, axis=1, keepdims=True)
    return emb


def _build_db() -> sqlite3.Connection:
    rng = np.random.default_rng(42)
    conn = sqlite3.connect(":memory:")

    # -- entity tables --

    conn.execute(
        """
        CREATE TABLE tracks (
            track_rowid INTEGER PRIMARY KEY,
            track_name TEXT,
            artist_rowid INTEGER,
            artist_name TEXT,
            album_rowid INTEGER,
            album_name TEXT,
            label_rowid INTEGER,
            label TEXT,
            lon REAL,
            lat REAL,
            album_lon REAL,
            album_lat REAL,
            artist_lon REAL,
            artist_lat REAL,
            label_lon REAL,
            label_lat REAL,
            logcount REAL,
            release_date TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "Blue in Green", 10, "Miles Davis", 20, "Kind of Blue", 30, "Columbia",
             1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 4.7, "1959-08-17"),
            (2, "So What", 10, "Miles Davis", 20, "Kind of Blue", 30, "Columbia",
             1.2, 2.3, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 4.5, "1959-08-17"),
            (3, "Nefertiti", 10, "Miles Davis", 21, "Nefertiti", 30, "Columbia",
             1.3, 2.4, 3.4, 4.5, 5.5, 6.6, 7.7, 8.8, 3.9, "1968-01-01"),
            (4, "Maiden Voyage", 11, "Herbie Hancock", 22, "Maiden Voyage", 31, "Blue Note",
             1.4, 2.5, 3.5, 4.6, 5.6, 6.7, 7.8, 8.9, 4.1, "1965-05-17"),
        ],
    )

    conn.execute(
        """
        CREATE TABLE albums (
            album_rowid INTEGER PRIMARY KEY,
            album_name_norm TEXT,
            artist_rowid INTEGER,
            artist_name TEXT,
            label_rowid INTEGER,
            label TEXT,
            lon REAL,
            lat REAL,
            artist_lon REAL,
            artist_lat REAL,
            label_lon REAL,
            label_lat REAL,
            logcount REAL,
            total_tracks INTEGER,
            release_date TEXT,
            album_type TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO albums VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (20, "kind of blue", 10, "Miles Davis", 30, "Columbia",
             3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 6.1, 5, "1959-08-17", "album"),
            (21, "nefertiti", 10, "Miles Davis", 30, "Columbia",
             3.4, 4.5, 5.5, 6.6, 7.7, 8.8, 5.2, 6, "1968-01-01", "album"),
            (22, "maiden voyage", 11, "Herbie Hancock", 31, "Blue Note",
             3.5, 4.6, 5.6, 6.7, 7.8, 8.9, 5.8, 8, "1965-05-17", "album"),
        ],
    )

    conn.execute(
        """
        CREATE TABLE artists (
            artist_rowid INTEGER PRIMARY KEY,
            artist_name TEXT,
            lon REAL,
            lat REAL,
            logcount REAL,
            nalbum INTEGER,
            artist_genre TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO artists VALUES (?,?,?,?,?,?,?)",
        [
            (10, "Miles Davis", 5.5, 6.6, 7.3, 42, "jazz"),
            (11, "Herbie Hancock", 5.6, 6.7, 6.8, 30, "jazz"),
        ],
    )

    conn.execute(
        """
        CREATE TABLE labels (
            label_rowid INTEGER PRIMARY KEY,
            label TEXT,
            lon REAL,
            lat REAL,
            logcount REAL,
            nalbum INTEGER,
            nartist INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO labels VALUES (?,?,?,?,?,?,?)",
        [
            (30, "Columbia", 7.7, 8.8, 5.0, 100, 50),
            (31, "Blue Note", 7.8, 8.9, 5.5, 80, 40),
        ],
    )

    # -- embedding tables --

    track_embs = _make_embeddings(4, rng)
    conn.execute(
        "CREATE TABLE track_embedding (track_rowid INTEGER PRIMARY KEY, embedding BLOB)"
    )
    for rowid, emb in zip([1, 2, 3, 4], track_embs):
        conn.execute("INSERT INTO track_embedding VALUES (?, ?)", (rowid, emb.tobytes()))

    album_embs = _make_embeddings(3, rng)
    conn.execute(
        "CREATE TABLE album_embedding (album_rowid INTEGER PRIMARY KEY, embedding BLOB)"
    )
    for rowid, emb in zip([20, 21, 22], album_embs):
        conn.execute("INSERT INTO album_embedding VALUES (?, ?)", (rowid, emb.tobytes()))

    artist_embs = _make_embeddings(2, rng)
    conn.execute(
        "CREATE TABLE artist_embedding (artist_rowid INTEGER PRIMARY KEY, embedding BLOB)"
    )
    for rowid, emb in zip([10, 11], artist_embs):
        conn.execute("INSERT INTO artist_embedding VALUES (?, ?)", (rowid, emb.tobytes()))

    label_embs = _make_embeddings(2, rng)
    conn.execute(
        "CREATE TABLE label_embedding (label_rowid INTEGER PRIMARY KEY, embedding BLOB)"
    )
    for rowid, emb in zip([30, 31], label_embs):
        conn.execute("INSERT INTO label_embedding VALUES (?, ?)", (rowid, emb.tobytes()))

    # -- repr tables --

    conn.execute(
        """
        CREATE TABLE album_repr_tracks (
            album_rowid INTEGER, rank INTEGER, track_rowid INTEGER, score REAL,
            PRIMARY KEY (album_rowid, rank)
        )
        """
    )
    conn.executemany(
        "INSERT INTO album_repr_tracks VALUES (?,?,?,?)",
        [(20, 0, 1, 0.9), (20, 1, 2, 0.8), (21, 0, 3, 0.95)],
    )

    conn.execute(
        """
        CREATE TABLE artist_repr_albums (
            artist_rowid INTEGER, rank INTEGER, album_rowid INTEGER, score REAL,
            PRIMARY KEY (artist_rowid, rank)
        )
        """
    )
    conn.executemany(
        "INSERT INTO artist_repr_albums VALUES (?,?,?,?)",
        [(10, 0, 20, 0.9), (10, 1, 21, 0.85), (11, 0, 22, 0.92)],
    )

    conn.execute(
        """
        CREATE TABLE label_repr_artists (
            label_rowid INTEGER, rank INTEGER, artist_rowid INTEGER, score REAL,
            PRIMARY KEY (label_rowid, rank)
        )
        """
    )
    conn.executemany(
        "INSERT INTO label_repr_artists VALUES (?,?,?,?)",
        [(30, 0, 10, 0.88), (31, 0, 11, 0.91)],
    )

    conn.commit()
    return conn, (track_embs, album_embs, artist_embs, label_embs)


def _build_faiss_indexes(
    track_embs: np.ndarray,
    album_embs: np.ndarray,
    artist_embs: np.ndarray,
    label_embs: np.ndarray,
) -> FaissIndexes:
    def _flat_index(embs: np.ndarray, rowids: list[int]) -> faiss.Index:
        index = faiss.IndexFlatIP(DIM)
        index = faiss.IndexIDMap2(index)
        index.add_with_ids(embs, np.array(rowids, dtype=np.int64))
        return index

    return FaissIndexes(
        track=_flat_index(track_embs, [1, 2, 3, 4]),
        album=_flat_index(album_embs, [20, 21, 22]),
        artist=_flat_index(artist_embs, [10, 11]),
        label=_flat_index(label_embs, [30, 31]),
    )


@pytest.fixture(scope="session")
def _shared_state():
    conn, embs = _build_db()
    indexes = _build_faiss_indexes(*embs)
    return conn, indexes


@pytest.fixture(scope="session")
def db(_shared_state):
    return _shared_state[0]


@pytest.fixture(scope="session")
def faiss_indexes(_shared_state):
    return _shared_state[1]
