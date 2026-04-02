from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.build_db import (
    DDL,
    build_album_repr_tracks,
    build_albums,
    build_artists,
    build_embedding,
    build_labels,
    build_tracks,
    canonicalize_albums,
    canonicalize_tracks,
    compute_searchable_recable,
    get_album_canonical_updates,
    get_track_canonical_updates,
    normalize_album_title,
)
from src.utils import ENTITY_KEYS as EKEYS


@pytest.fixture
def db():
    """In-memory SQLite DB with DDL applied."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    return conn


def _label_lookup():
    return pd.DataFrame(
        {
            EKEYS.label: [1, 2],
            "label": ["Warp Records", "Hyperdub"],
            "logcount": [3.5, 2.8],
            "ntrack": [500, 200],
            "nalbum": [80, 40],
            "nartist": [30, 15],
        }
    )


def _label_geo():
    return pd.DataFrame(
        {EKEYS.label: [1, 2], "lon": [1.0, 2.0], "lat": [10.0, 20.0]}
    )


def _artist_lookup():
    return pd.DataFrame(
        {
            EKEYS.artist: [101, 102],
            "artist_name": ["Autechre", "Burial"],
            "artist_genre": ["IDM", "dubstep"],
            "logcount": [3.0, 2.5],
            "ntrack": [120, 60],
            "nalbum": [15, 5],
        }
    )


def _artist_geo():
    return pd.DataFrame(
        {EKEYS.artist: [101, 102], "lon": [5.0, 6.0], "lat": [50.0, 60.0]}
    )


def _album_lookup():
    return pd.DataFrame(
        {
            EKEYS.album: [201, 202, 203],
            "album_name": [
                "Untitled",
                "Untitled (Remastered Edition)",
                "Kindred",
            ],
            EKEYS.artist: [101, 101, 102],
            EKEYS.label: [1, 1, 2],
            "label": ["Warp Records", "Warp Records", "Hyperdub"],
            "artist_name": ["Autechre", "Autechre", "Burial"],
            "album_type": ["album", "album", "ep"],
            "release_date": ["2024-01-01", "2024-06-01", "2023-01-01"],
            "release_date_precision": ["day", "day", "day"],
            "logcount": [3.2, 2.9, 2.4],
            "total_tracks": [10, 10, 4],
        }
    )


def _album_geo():
    return pd.DataFrame(
        {
            EKEYS.album: [201, 202, 203],
            "lon": [7.0, 7.1, 8.0],
            "lat": [70.0, 70.1, 80.0],
        }
    )


def _track_lookup():
    return pd.DataFrame(
        {
            EKEYS.track: [1001, 1002, 1003, 1004],
            "track_name": [
                "Gantz Graf",
                "Rae",
                "Archangel",
                "Etched Headplate",
            ],
            "track_popularity": [45, 30, 60, 20],
            "logcount": [3.1, 2.8, 3.5, 1.9],
            "release_date": ["2024-01-01"] * 4,
            "id_isrc": ["ISRC001", "ISRC002", "ISRC003", "ISRC004"],
            EKEYS.artist: [101, 101, 102, 101],
            "artist_name": ["Autechre", "Autechre", "Burial", "Autechre"],
            EKEYS.album: [201, 201, 203, 202],
            "album_name": [
                "Untitled",
                "Untitled",
                "Kindred",
                "Untitled (Remastered Edition)",
            ],
            EKEYS.label: [1, 1, 2, 1],
            "label": ["Warp Records", "Warp Records", "Hyperdub", "Warp Records"],
        }
    )


def _track_geo():
    return pd.DataFrame(
        {
            EKEYS.track: [1001, 1002, 1003, 1004],
            "lon": [0.1, 0.2, 0.3, 0.4],
            "lat": [1.1, 1.2, 1.3, 1.4],
        }
    )


def _write_track_parquet(tmp_path: Path) -> Path:
    path = tmp_path / "lookup_track.parquet"
    _track_lookup().to_parquet(path)
    return path


def _build_full_db(db, tmp_path: Path):
    """Populate all entity tables so repr queries have data to work with."""
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    album_geo = build_albums(
        db, _album_lookup(), _album_geo(), artist_geo, label_geo
    )
    canonicalize_albums(db)
    db.commit()
    lookup_path = _write_track_parquet(tmp_path)
    build_tracks(
        db, lookup_path, _track_geo(), artist_geo, album_geo, label_geo, batch_size=100
    )
    canonicalize_tracks(db)
    db.commit()


def _rows_as_dicts(db, sql):
    cur = db.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


@pytest.mark.parametrize(
    "title, expected",
    [
        ("Untitled", "Untitled"),
        ("Untitled (Remastered Edition)", "Untitled"),
        ("OK Computer (Deluxe)", "OK Computer"),
        ("In Rainbows (Disk 2)", "In Rainbows (Disk 2)"),
        ("  Spaced   (Expanded)  ", "Spaced"),
    ],
    ids=["plain", "remaster", "deluxe", "non-marker-kept", "whitespace"],
)
def test_normalize_album_title(title, expected):
    assert normalize_album_title(title) == expected


def test_canonical_updates_prefers_highest_logcount():
    albums = pd.DataFrame(
        {
            "album_rowid": [201, 202],
            "artist_rowid": [101, 101],
            "album_type": ["album", "album"],
            "album_name": ["Untitled", "Untitled (Remastered Edition)"],
            "album_name_norm": ["Untitled", "Untitled"],
            "logcount": [2.5, 3.0],
        }
    )
    updates = get_album_canonical_updates(albums)
    canonical = updates.set_index("album_rowid")["album_canonical_rowid"]
    assert canonical[201] == 202, "lower logcount should point to higher logcount"
    assert canonical[202] == 202, "highest logcount should be self-canonical"


def test_canonical_updates_tiebreak_by_lowest_rowid():
    albums = pd.DataFrame(
        {
            "album_rowid": [201, 202],
            "artist_rowid": [101, 101],
            "album_type": ["album", "album"],
            "album_name": ["Untitled", "Untitled (Remastered Edition)"],
            "album_name_norm": ["Untitled", "Untitled"],
            "logcount": [3.0, 3.0],
        }
    )
    updates = get_album_canonical_updates(albums)
    canonical = updates.set_index("album_rowid")["album_canonical_rowid"]
    assert canonical[201] == 201
    assert canonical[202] == 201


def test_track_canonical_groups_by_isrc_picks_highest_logcount():
    tracks = pd.DataFrame(
        {
            "track_rowid": [1, 2, 3],
            "id_isrc": ["ISRC_A", "ISRC_A", "ISRC_B"],
            "logcount": [1.0, 3.0, 2.0],
        }
    )
    updates = get_track_canonical_updates(tracks)
    canonical = updates.set_index("track_rowid")["track_canonical_rowid"]
    assert canonical[1] == 2, "lower logcount should point to higher"
    assert canonical[2] == 2, "highest logcount should be self-canonical"
    assert canonical[3] == 3, "sole ISRC should be self-canonical"


def test_track_canonical_null_isrc_is_self_canonical():
    tracks = pd.DataFrame(
        {
            "track_rowid": [1, 2, 3],
            "id_isrc": [np.nan, "", "  "],
            "logcount": [1.0, 2.0, 3.0],
        }
    )
    updates = get_track_canonical_updates(tracks)
    canonical = updates.set_index("track_rowid")["track_canonical_rowid"]
    assert canonical[1] == 1
    assert canonical[2] == 2
    assert canonical[3] == 3


def test_track_canonical_tiebreak_by_lowest_rowid():
    tracks = pd.DataFrame(
        {
            "track_rowid": [10, 20],
            "id_isrc": ["ISRC_A", "ISRC_A"],
            "logcount": [2.0, 2.0],
        }
    )
    updates = get_track_canonical_updates(tracks)
    canonical = updates.set_index("track_rowid")["track_canonical_rowid"]
    assert canonical[10] == 10
    assert canonical[20] == 10


def test_build_labels_writes_all_lookup_columns(db):
    build_labels(db, _label_lookup(), _label_geo())

    rows = _rows_as_dicts(db, "SELECT * FROM labels ORDER BY label_rowid")
    assert len(rows) == 2
    assert rows[0]["label"] == "Warp Records"
    assert rows[0]["nartist"] == 30
    assert rows[0]["label_canonical_rowid"] == rows[0]["label_rowid"]


def test_build_artists_writes_genre_and_counts(db):
    build_artists(db, _artist_lookup(), _artist_geo())

    rows = _rows_as_dicts(db, "SELECT * FROM artists ORDER BY artist_rowid")
    assert len(rows) == 2
    assert rows[0]["artist_genre"] == "IDM"
    assert rows[0]["ntrack"] == 120
    assert rows[0]["nalbum"] == 15
    assert rows[0]["artist_canonical_rowid"] == rows[0]["artist_rowid"]


def test_build_albums_denormalizes_artist_and_label_geo(db):
    build_albums(
        db, _album_lookup(), _album_geo(), _artist_geo(), _label_geo()
    )

    rows = _rows_as_dicts(db, "SELECT * FROM albums WHERE album_rowid = 201")
    row = rows[0]
    assert row["artist_lon"] == 5.0
    assert row["artist_lat"] == 50.0
    assert row["label_lon"] == 1.0
    assert row["label_lat"] == 10.0
    assert row["album_name_norm"] == "Untitled"


def test_canonicalize_albums_groups_remaster_variants(db):
    build_albums(
        db, _album_lookup(), _album_geo(), _artist_geo(), _label_geo()
    )
    canonicalize_albums(db)
    db.commit()

    rows = _rows_as_dicts(
        db, "SELECT album_rowid, album_canonical_rowid FROM albums ORDER BY album_rowid"
    )
    canonical = {r["album_rowid"]: r["album_canonical_rowid"] for r in rows}
    assert canonical[201] == canonical[202], "remaster should share canonical with original"
    assert canonical[203] == 203, "unrelated album should be self-canonical"


def test_build_tracks_denormalizes_geo_and_includes_isrc(db, tmp_path):
    lookup_path = _write_track_parquet(tmp_path)
    build_tracks(
        db, lookup_path,
        _track_geo(), _artist_geo(), _album_geo(), _label_geo(),
        batch_size=100,
    )

    rows = _rows_as_dicts(db, "SELECT * FROM tracks WHERE track_rowid = 1001")
    row = rows[0]
    assert row["id_isrc"] == "ISRC001"
    assert row["artist_lon"] == 5.0
    assert row["album_lon"] == 7.0
    assert row["label_lon"] == 1.0
    assert row["track_canonical_rowid"] == 1001


def test_build_tracks_filters_to_geo_subset(db, tmp_path):
    """Tracks absent from track_geo are excluded (implicit geo filter)."""
    lookup_path = _write_track_parquet(tmp_path)
    partial_geo = pd.DataFrame(
        {EKEYS.track: [1001, 1003], "lon": [0.1, 0.3], "lat": [1.1, 1.3]}
    )
    build_tracks(
        db, lookup_path,
        partial_geo, _artist_geo(), _album_geo(), _label_geo(),
        batch_size=100,
    )

    count = db.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    assert count == 2


def test_build_tracks_nullifies_empty_label(db, tmp_path):
    """Tracks with empty-string label get NULL in the DB."""
    track = _track_lookup().copy()
    track.loc[track[EKEYS.track] == 1001, "label"] = ""
    path = tmp_path / "lookup_track.parquet"
    track.to_parquet(path)

    build_tracks(
        db, path,
        _track_geo(), _artist_geo(), _album_geo(), _label_geo(),
        batch_size=100,
    )

    row = _rows_as_dicts(db, "SELECT label FROM tracks WHERE track_rowid = 1001")[0]
    assert row["label"] is None


def test_canonicalize_tracks_in_db(db, tmp_path):
    """Tracks sharing ISRC get the same canonical rowid in the DB."""
    tracks = pd.DataFrame(
        {
            EKEYS.track: [1, 2, 3],
            "track_name": ["A", "B", "C"],
            "track_popularity": [10, 20, 30],
            "logcount": [1.0, 3.0, 2.0],
            "release_date": ["2024-01-01"] * 3,
            "id_isrc": ["DUP", "DUP", "UNIQUE"],
            EKEYS.artist: [101, 101, 102],
            "artist_name": ["Autechre", "Autechre", "Burial"],
            EKEYS.album: [201, 201, 203],
            "album_name": ["Untitled", "Untitled", "Kindred"],
            EKEYS.label: [1, 1, 2],
            "label": ["Warp", "Warp", "Hyperdub"],
        }
    )
    path = tmp_path / "lookup_track.parquet"
    tracks.to_parquet(path)
    track_geo = pd.DataFrame({EKEYS.track: [1, 2, 3], "lon": [0.1, 0.2, 0.3], "lat": [1.1, 1.2, 1.3]})

    build_tracks(db, path, track_geo, _artist_geo(), _album_geo(), _label_geo(), batch_size=100)
    canonicalize_tracks(db)
    db.commit()

    canonical = {
        r[0]: r[1]
        for r in db.execute("SELECT track_rowid, track_canonical_rowid FROM tracks").fetchall()
    }
    assert canonical[1] == 2, "lower logcount DUP should point to track 2"
    assert canonical[2] == 2, "highest logcount DUP should be self-canonical"
    assert canonical[3] == 3, "unique ISRC should be self-canonical"


def test_searchable_recable_flags(db, tmp_path):
    _build_full_db(db, tmp_path)
    compute_searchable_recable(
        db,
        searchable_track_min_logcount=2.0,
        searchable_album_min_total_tracks=2,
        searchable_artist_min_ntrack=10,
        searchable_label_min_nartist=10,
        recable_track_min_logcount=3.0,
    )
    db.commit()

    track_flags = {
        r[0]: (r[1], r[2])
        for r in db.execute(
            "SELECT track_rowid, searchable, recable FROM tracks"
        ).fetchall()
    }
    assert track_flags[1001] == (1, 1), "logcount 3.1 >= both thresholds"
    assert track_flags[1003] == (1, 1), "logcount 3.5 >= both thresholds"
    assert track_flags[1002] == (1, 0), "logcount 2.8 >= searchable but < recable"
    assert track_flags[1004] == (0, 0), "logcount 1.9 < searchable threshold"

    label_flags = {
        r[0]: (r[1], r[2])
        for r in db.execute(
            "SELECT label_rowid, searchable, recable FROM labels"
        ).fetchall()
    }
    assert label_flags[1] == (1, 1), "nartist 30 >= 10"
    assert label_flags[2] == (1, 1), "nartist 15 >= 10"


def test_build_embedding_stores_normalized_blobs(db, tmp_path):
    dim = 128
    rowids = [1001, 1002, 1003]
    rng = np.random.default_rng(42)
    matrix = rng.standard_normal((3, dim)).astype(np.float32)

    emb_df = pd.concat(
        [pd.DataFrame({EKEYS.track: rowids}),
         pd.DataFrame(matrix, columns=[f"e{i}" for i in range(dim)])],
        axis=1,
    )
    emb_path = tmp_path / "embedding_track.parquet"
    emb_df.to_parquet(emb_path)

    build_embedding(db, emb_path, EKEYS.track, "track_embedding", batch_size=100)

    rows = db.execute(
        "SELECT track_rowid, embedding FROM track_embedding ORDER BY track_rowid"
    ).fetchall()
    assert len(rows) == 3
    assert [r[0] for r in rows] == [1001, 1002, 1003]

    blob = rows[0][1]
    assert len(blob) == dim * 4
    vec = np.frombuffer(blob, dtype=np.float32)
    np.testing.assert_allclose(np.linalg.norm(vec), 1.0, atol=1e-5)


def test_album_repr_tracks_ranks_by_logcount(db, tmp_path):
    _build_full_db(db, tmp_path)
    compute_searchable_recable(
        db,
        searchable_track_min_logcount=2.0,
        searchable_album_min_total_tracks=2,
        searchable_artist_min_ntrack=10,
        searchable_label_min_nartist=10,
        recable_track_min_logcount=3.0,
    )
    db.commit()
    build_album_repr_tracks(db, limit=2)

    rows = db.execute(
        "SELECT track_rowid, score FROM album_repr_tracks"
        " WHERE album_rowid = 201 ORDER BY rank"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 1001, "track 1001 (logcount=3.1) should rank first"
    assert rows[1][0] == 1002, "track 1002 (logcount=2.8) should rank second"
    assert rows[0][1] > rows[1][1]
