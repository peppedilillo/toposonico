from src.repr import repr_fetch
from src.utils import ALBUM
from src.utils import ARTIST
from src.utils import LABEL
from src.utils import TRACK


def test_repr_track_returns_empty(db):
    assert repr_fetch(TRACK, 1, limit=3, db=db) == []


def test_repr_album(db):
    results = repr_fetch(ALBUM, 20, limit=3, db=db)

    assert results == [
        {
            "track_rowid": 1,
            "track_name_norm": "Blue in Green",
            "artist_name": "Miles Davis",
            "lon": 1.1,
            "lat": 2.2,
        },
        {
            "track_rowid": 2,
            "track_name_norm": "So What",
            "artist_name": "Miles Davis",
            "lon": 1.2,
            "lat": 2.3,
        },
    ]


def test_repr_album_respects_limit(db):
    results = repr_fetch(ALBUM, 20, limit=1, db=db)

    assert len(results) == 1
    assert results[0]["track_rowid"] == 1


def test_repr_artist(db):
    results = repr_fetch(ARTIST, 10, limit=3, db=db)

    assert results == [
        {
            "album_rowid": 20,
            "album_name_norm": "kind of blue",
            "artist_name": "Miles Davis",
            "lon": 3.3,
            "lat": 4.4,
        },
        {
            "album_rowid": 21,
            "album_name_norm": "nefertiti",
            "artist_name": "Miles Davis",
            "lon": 3.4,
            "lat": 4.5,
        },
    ]


def test_repr_label(db):
    results = repr_fetch(LABEL, 30, limit=3, db=db)

    assert results == [
        {
            "artist_rowid": 10,
            "artist_name": "Miles Davis",
            "lon": 5.5,
            "lat": 6.6,
        },
    ]


def test_repr_no_entries(db):
    assert repr_fetch(ALBUM, 22, limit=3, db=db) == []
