from src.info import info_fetch
from src.utils import ALBUM
from src.utils import ARTIST
from src.utils import LABEL
from src.utils import TRACK


def test_info_track(db):
    result = info_fetch(TRACK, 1, db)

    assert result == {
        "track_rowid": 1,
        "track_name": "Blue in Green",
        "artist_rowid": 10,
        "artist_name": "Miles Davis",
        "album_rowid": 20,
        "album_name": "Kind of Blue",
        "label_rowid": 30,
        "label": "Columbia",
        "lon": 1.1,
        "lat": 2.2,
        "album_lon": 3.3,
        "album_lat": 4.4,
        "artist_lon": 5.5,
        "artist_lat": 6.6,
        "label_lon": 7.7,
        "label_lat": 8.8,
        "logcount": 4.7,
        "release_date": "1959-08-17",
    }


def test_info_album(db):
    result = info_fetch(ALBUM, 20, db)

    assert result == {
        "album_rowid": 20,
        "album_name_norm": "kind of blue",
        "artist_rowid": 10,
        "artist_name": "Miles Davis",
        "label_rowid": 30,
        "label": "Columbia",
        "lon": 3.3,
        "lat": 4.4,
        "artist_lon": 5.5,
        "artist_lat": 6.6,
        "label_lon": 7.7,
        "label_lat": 8.8,
        "logcount": 6.1,
        "total_tracks": 5,
        "release_date": "1959-08-17",
        "album_type": "album",
    }


def test_info_artist(db):
    result = info_fetch(ARTIST, 10, db)

    assert result == {
        "artist_rowid": 10,
        "artist_name": "Miles Davis",
        "lon": 5.5,
        "lat": 6.6,
        "logcount": 7.3,
        "nalbum": 42,
        "artist_genre": "jazz",
    }


def test_info_label(db):
    result = info_fetch(LABEL, 30, db)

    assert result == {
        "label_rowid": 30,
        "label": "Columbia",
        "lon": 7.7,
        "lat": 8.8,
        "logcount": 5.0,
        "nalbum": 100,
        "nartist": 50,
    }


def test_info_missing_row_returns_none(db):
    assert info_fetch(TRACK, 999, db) is None
