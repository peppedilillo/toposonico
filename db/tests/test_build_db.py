from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.build_db import build_album_repr_tracks
from scripts.build_db import build_albums
from scripts.build_db import build_artist_repr_albums
from scripts.build_db import rank_artist_repr_albums
from scripts.build_db import build_artists
from scripts.build_db import build_embedding
from scripts.build_db import build_label_repr_artists
from scripts.build_db import rank_label_repr_artists
from scripts.build_db import build_labels
from scripts.build_db import build_representatives
from scripts.build_db import build_tracks
from scripts.build_db import canonicalize_albums
from scripts.build_db import canonicalize_tracks
from scripts.build_db import compute_searchable_recable
from scripts.build_db import DDL
from scripts.build_db import get_album_canonical_updates
from scripts.build_db import get_track_canonical_updates
from scripts.build_db import normalize_title
from src.utils import ENTITY_KEYS as EKEYS


@pytest.fixture
def db():
    """In-memory SQLite DB with DDL applied."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
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
    return pd.DataFrame({EKEYS.label: [1, 2], "lon": [1.0, 2.0], "lat": [10.0, 20.0]})


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
    return pd.DataFrame({EKEYS.artist: [101, 102], "lon": [5.0, 6.0], "lat": [50.0, 60.0]})


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
    album_geo = build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)
    canonicalize_albums(db)
    db.commit()
    lookup_path = _write_track_parquet(tmp_path)
    build_tracks(db, lookup_path, _track_geo(), artist_geo, album_geo, label_geo, batch_size=100)
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
        ("...And Justice For All", "..And Justice For All"),
        ("....And Justice For All", "..And Justice For All"),
        ("Foo.....Bar", "Foo..Bar"),
        ("...Album [Deluxe]", "..Album"),
        ("..And Justice For All", "..And Justice For All"),
    ],
    ids=[
        "plain",
        "remaster",
        "deluxe",
        "non-marker-kept",
        "whitespace",
        "triple-dots-collapsed",
        "quadruple-dots-collapsed",
        "mid-string-dot-run-collapsed",
        "dot-run-plus-marker-stripped",
        "double-dots-unchanged",
    ],
)
def test_normalize_title(title, expected):
    assert normalize_title(title) == expected


def test_album_canonical_picks_smallest_rowid():
    albums = pd.DataFrame(
        {
            "album_rowid": [201, 202],
            "artist_rowid": [101, 101],
            "album_type": ["album", "album"],
            "album_name": ["Untitled", "Untitled (Remastered Edition)"],
            "album_name_norm": ["Untitled", "Untitled"],
        }
    )
    updates = get_album_canonical_updates(albums)
    canonical = updates.set_index("album_rowid")["album_canonical_rowid"]
    assert canonical[201] == 201, "smallest rowid should be self-canonical"
    assert canonical[202] == 201, "larger rowid should point to smallest"


def test_album_canonical_is_case_insensitive():
    albums = pd.DataFrame(
        {
            "album_rowid": [201, 202],
            "artist_rowid": [101, 101],
            "album_type": ["album", "album"],
            "album_name": ["OK Computer", "ok computer"],
            "album_name_norm": ["OK Computer", "ok computer"],
        }
    )
    updates = get_album_canonical_updates(albums)
    canonical = updates.set_index("album_rowid")["album_canonical_rowid"]
    assert canonical[201] == 201
    assert canonical[202] == 201, "case-different album should share canonical"


def test_album_canonical_collapses_long_dot_runs():
    albums = pd.DataFrame(
        {
            "album_rowid": [201, 202],
            "artist_rowid": [101, 101],
            "album_type": ["album", "album"],
            "album_name": ["...And Justice For All", "..And Justice For All"],
            "album_name_norm": ["..And Justice For All", "..And Justice For All"],
        }
    )
    updates = get_album_canonical_updates(albums)
    canonical = updates.set_index("album_rowid")["album_canonical_rowid"]
    assert canonical[201] == 201
    assert canonical[202] == 201, "albums differing only by long dot runs should share canonical"


def test_track_canonical_picks_smallest_rowid():
    tracks = pd.DataFrame(
        {
            "track_rowid": [1, 2, 3],
            "album_rowid": [201, 201, 202],
            "track_name_norm": ["Song", "Song", "Other"],
        }
    )
    updates = get_track_canonical_updates(tracks)
    canonical = updates.set_index("track_rowid")["track_canonical_rowid"]
    assert canonical[1] == 1, "smallest rowid should be self-canonical"
    assert canonical[2] == 1, "larger rowid should point to smallest"
    assert canonical[3] == 3, "different album should be self-canonical"


def test_track_canonical_is_case_insensitive():
    tracks = pd.DataFrame(
        {
            "track_rowid": [1, 2, 3],
            "album_rowid": [201, 201, 202],
            "track_name_norm": ["Blue in Green", "blue in green", "Blue in Green"],
        }
    )
    updates = get_track_canonical_updates(tracks)
    canonical = updates.set_index("track_rowid")["track_canonical_rowid"]
    assert canonical[1] == 1, "smallest rowid should be self-canonical"
    assert canonical[2] == 1, "case-different name on same album should share canonical"
    assert canonical[3] == 3, "different album should be self-canonical"


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
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)

    rows = _rows_as_dicts(db, "SELECT * FROM albums WHERE album_rowid = 201")
    row = rows[0]
    assert row["artist_lon"] == 5.0
    assert row["artist_lat"] == 50.0
    assert row["label_lon"] == 1.0
    assert row["label_lat"] == 10.0
    assert row["album_name_norm"] == "Untitled"


def test_canonicalize_albums_groups_remaster_variants(db):
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)
    canonicalize_albums(db)
    db.commit()

    rows = _rows_as_dicts(db, "SELECT album_rowid, album_canonical_rowid FROM albums ORDER BY album_rowid")
    canonical = {r["album_rowid"]: r["album_canonical_rowid"] for r in rows}
    assert canonical[201] == canonical[202], "remaster should share canonical with original"
    assert canonical[203] == 203, "unrelated album should be self-canonical"


def test_build_tracks_denormalizes_geo_and_includes_isrc(db, tmp_path):
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    album_geo = build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)
    lookup_path = _write_track_parquet(tmp_path)
    build_tracks(
        db,
        lookup_path,
        _track_geo(),
        artist_geo,
        album_geo,
        label_geo,
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
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    album_geo = build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)
    lookup_path = _write_track_parquet(tmp_path)
    partial_geo = pd.DataFrame({EKEYS.track: [1001, 1003], "lon": [0.1, 0.3], "lat": [1.1, 1.3]})
    build_tracks(
        db,
        lookup_path,
        partial_geo,
        artist_geo,
        album_geo,
        label_geo,
        batch_size=100,
    )

    count = db.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    assert count == 2


def test_build_tracks_rejects_empty_label(db, tmp_path):
    """Tracks with empty-string label fail fast under the strict schema."""
    track = _track_lookup().copy()
    track.loc[track[EKEYS.track] == 1001, "label"] = ""
    path = tmp_path / "lookup_track.parquet"
    track.to_parquet(path)

    with pytest.raises(ValueError, match=r"tracks: required columns contain null/empty values: label=1"):
        build_tracks(
            db,
            path,
            _track_geo(),
            _artist_geo(),
            _album_geo(),
            _label_geo(),
            batch_size=100,
        )


def test_canonicalize_tracks_in_db(db, tmp_path):
    """Tracks with same album + normalized name share canonical in the DB."""
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)
    canonicalize_albums(db)
    db.commit()

    tracks = pd.DataFrame(
        {
            EKEYS.track: [1, 2, 3],
            "track_name": ["A", "A", "C"],
            "track_popularity": [10, 20, 30],
            "logcount": [1.0, 3.0, 2.0],
            "release_date": ["2024-01-01"] * 3,
            "id_isrc": ["ISRC1", "ISRC2", "ISRC3"],
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

    canonical = {r[0]: r[1] for r in db.execute("SELECT track_rowid, track_canonical_rowid FROM tracks").fetchall()}
    assert canonical[1] == 1, "smallest rowid should be self-canonical"
    assert canonical[2] == 1, "same name+album should share canonical"
    assert canonical[3] == 3, "different album should be self-canonical"


def test_canonicalize_tracks_across_canonical_albums(db, tmp_path):
    """Tracks with the same name in duplicate albums share a canonical track."""
    label_geo = build_labels(db, _label_lookup(), _label_geo())
    artist_geo = build_artists(db, _artist_lookup(), _artist_geo())
    build_albums(db, _album_lookup(), _album_geo(), artist_geo, label_geo)
    canonicalize_albums(db)
    db.commit()

    # Albums 201 and 202 are canonical duplicates (same artist, type, normalized name).
    # Insert a track named "Gantz Graf" in each album — they should be deduplicated.
    tracks = pd.DataFrame(
        {
            EKEYS.track: [1001, 1002],
            "track_name": ["Gantz Graf", "Gantz Graf"],
            "track_popularity": [45, 30],
            "logcount": [3.1, 2.8],
            "release_date": ["2024-01-01", "2024-01-01"],
            "id_isrc": ["ISRC001", "ISRC002"],
            EKEYS.artist: [101, 101],
            "artist_name": ["Autechre", "Autechre"],
            EKEYS.album: [201, 202],
            "album_name": ["Untitled", "Untitled (Remastered Edition)"],
            EKEYS.label: [1, 1],
            "label": ["Warp Records", "Warp Records"],
        }
    )
    path = tmp_path / "lookup_track.parquet"
    tracks.to_parquet(path)
    track_geo = pd.DataFrame({EKEYS.track: [1001, 1002], "lon": [0.1, 0.2], "lat": [1.1, 1.2]})

    build_tracks(db, path, track_geo, artist_geo, _album_geo(), label_geo, batch_size=100)
    canonicalize_tracks(db)
    db.commit()

    canonical = {
        r[0]: r[1]
        for r in db.execute("SELECT track_rowid, track_canonical_rowid FROM tracks").fetchall()
    }
    assert canonical[1001] == 1001, "smallest rowid should be self-canonical"
    assert canonical[1002] == 1001, "same track name across canonical-duplicate albums should share canonical"


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
        r[0]: (r[1], r[2]) for r in db.execute("SELECT track_rowid, searchable, recable FROM tracks").fetchall()
    }
    assert track_flags[1001] == (1, 1), "logcount 3.1 >= both thresholds"
    assert track_flags[1003] == (1, 1), "logcount 3.5 >= both thresholds"
    assert track_flags[1002] == (1, 0), "logcount 2.8 >= searchable but < recable"
    assert track_flags[1004] == (0, 0), "logcount 1.9 < searchable threshold"

    label_flags = {
        r[0]: (r[1], r[2]) for r in db.execute("SELECT label_rowid, searchable, recable FROM labels").fetchall()
    }
    assert label_flags[1] == (1, 1), "nartist 30 >= 10"
    assert label_flags[2] == (1, 1), "nartist 15 >= 10"


def test_build_embedding_stores_normalized_blobs(db, tmp_path):
    _build_full_db(db, tmp_path)

    dim = 128
    rowids = [1001, 1002, 1003]
    rng = np.random.default_rng(42)
    matrix = rng.standard_normal((3, dim)).astype(np.float32)

    emb_df = pd.concat(
        [pd.DataFrame({EKEYS.track: rowids}), pd.DataFrame(matrix, columns=[f"e{i}" for i in range(dim)])],
        axis=1,
    )
    emb_path = tmp_path / "embedding_track.parquet"
    emb_df.to_parquet(emb_path)

    build_embedding(db, emb_path, EKEYS.track, "track_embedding", batch_size=100)

    rows = db.execute("SELECT track_rowid, embedding FROM track_embedding ORDER BY track_rowid").fetchall()
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
        "SELECT track_rowid, score FROM album_repr_tracks" " WHERE album_rowid = 201 ORDER BY rank"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 1001, "track 1001 (logcount=3.1) should rank first"
    assert rows[1][0] == 1002, "track 1002 (logcount=2.8) should rank second"
    assert rows[0][1] > rows[1][1]

    # Track 1004 (logcount=1.9) is not searchable — must not appear
    all_track_ids = {r[0] for r in db.execute("SELECT track_rowid FROM album_repr_tracks").fetchall()}
    assert 1004 not in all_track_ids, "non-searchable track must not appear in repr"


def test_rank_artist_repr_albums():
    """Pure-function test: canonical grouping, type preference, median scoring, ranking."""
    tracks = pd.DataFrame(
        {
            "album_canonical_rowid": [1, 1, 1, 2, 2, 3, 3],
            "logcount": [3.0, 1.0, 2.0, 5.0, 4.0, 1.0, 1.0],
        }
    )
    albums = pd.DataFrame(
        {
            "album_rowid": [1, 2, 3],
            "artist_rowid": [10, 10, 10],
            "logcount": [3.0, 2.0, 1.0],
            "album_type": ["album", "album", "ep"],
        }
    )
    result = rank_artist_repr_albums(tracks, albums, limit=2)

    assert list(result.columns) == ["artist_rowid", "rank", "album_rowid", "score"]
    rows = list(result.itertuples(index=False, name=None))
    # Album 2 (median=4.5) ranks above album 1 (median=2.0); EP 3 excluded by type preference
    assert rows[0] == (10, 0, 2, 4.5)
    assert rows[1] == (10, 1, 1, 2.0)


def test_artist_repr_albums_excludes_non_searchable(db, tmp_path):
    """Non-searchable albums must not appear as representative albums."""
    _build_full_db(db, tmp_path)
    # total_tracks threshold of 5 makes album 203 (total_tracks=4) non-searchable
    compute_searchable_recable(
        db,
        searchable_track_min_logcount=2.0,
        searchable_album_min_total_tracks=5,
        searchable_artist_min_ntrack=10,
        searchable_label_min_nartist=10,
        recable_track_min_logcount=3.0,
    )
    db.commit()
    build_artist_repr_albums(db, limit=10)

    rows = db.execute(
        "SELECT artist_rowid, album_rowid FROM artist_repr_albums ORDER BY artist_rowid, rank"
    ).fetchall()
    repr_album_ids = {album_rowid for _, album_rowid in rows}
    non_searchable = {
        r[0]
        for r in db.execute("SELECT album_rowid FROM albums WHERE searchable = 0").fetchall()
    }
    assert repr_album_ids.isdisjoint(non_searchable), "non-searchable albums must not appear in repr"
    assert (101, 201) in rows, "artist 101 should retain searchable album 201 in repr"
    assert not any(artist_rowid == 102 for artist_rowid, _ in rows), (
        "artist 102 only has non-searchable albums and should not get repr rows"
    )


def test_rank_label_repr_artists():
    """Pure-function test: scoring, tiebreak, ranking."""
    tracks = pd.DataFrame(
        {
            "label_rowid": [1, 1, 1, 1, 1],
            "artist_rowid": [10, 10, 20, 20, 20],
            "album_rowid": [100, 101, 200, 200, 201],
            "logcount": [4.0, 3.0, 2.0, 2.0, 2.0],
        }
    )
    result = rank_label_repr_artists(tracks, limit=2)

    assert list(result.columns) == ["label_rowid", "rank", "artist_rowid", "score"]
    rows = list(result.itertuples(index=False, name=None))
    # artist 10: sum=7.0, count=2, score=7/sqrt(2)≈4.95, albums=2, best=4.0
    # artist 20: sum=6.0, count=3, score=6/sqrt(3)≈3.46, albums=2, best=2.0
    assert rows[0][2] == 10, "artist 10 should rank first (higher score)"
    assert rows[1][2] == 20, "artist 20 should rank second"
    assert rows[0][1] == 0
    assert rows[1][1] == 1


def test_label_repr_artists_excludes_non_searchable(db, tmp_path):
    """Non-searchable artists must not appear as representative artists."""
    _build_full_db(db, tmp_path)
    # ntrack threshold of 100 makes artist 102 (ntrack=60) non-searchable
    compute_searchable_recable(
        db,
        searchable_track_min_logcount=2.0,
        searchable_album_min_total_tracks=2,
        searchable_artist_min_ntrack=100,
        searchable_label_min_nartist=10,
        recable_track_min_logcount=3.0,
    )
    db.commit()
    build_label_repr_artists(db, limit=10)

    rows = db.execute(
        "SELECT label_rowid, artist_rowid FROM label_repr_artists ORDER BY label_rowid, rank"
    ).fetchall()
    repr_artist_ids = {artist_rowid for _, artist_rowid in rows}
    non_searchable = {
        r[0]
        for r in db.execute("SELECT artist_rowid FROM artists WHERE searchable = 0").fetchall()
    }
    assert repr_artist_ids.isdisjoint(non_searchable), "non-searchable artists must not appear in repr"
    assert (1, 101) in rows, "label 1 should retain searchable artist 101 in repr"
    assert not any(label_rowid == 2 for label_rowid, _ in rows), (
        "label 2 only has non-searchable artists and should not get repr rows"
    )


def test_compute_nrepr(db, tmp_path):
    """nrepr must match actual repr table row counts per parent."""
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
    build_representatives(db, limit=3)

    for parent_key, table, repr_table in (
        ("album_rowid", "albums", "album_repr_tracks"),
        ("artist_rowid", "artists", "artist_repr_albums"),
        ("label_rowid", "labels", "label_repr_artists"),
    ):
        rows = db.execute(f"SELECT {parent_key}, nrepr FROM {table}").fetchall()
        for rowid, nrepr in rows:
            actual = db.execute(
                f"SELECT COUNT(*) FROM {repr_table} WHERE {parent_key} = ?", (rowid,)
            ).fetchone()[0]
            assert nrepr == actual, (
                f"{table} rowid={rowid}: nrepr={nrepr} but repr table has {actual} rows"
            )
