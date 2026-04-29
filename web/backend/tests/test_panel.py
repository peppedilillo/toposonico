from src.panel import panel_fetch
from src.utils import ALBUM
from src.utils import ARTIST
from src.utils import LABEL
from src.utils import TRACK


def test_panel_track(db):
    result = panel_fetch(TRACK, 1, repr_limit=3, db=db)

    assert result == {
        "entity_type": "track",
        "rowid": 1,
        "track_name_norm": "Blue in Green",
        "artist_rowid": 10,
        "artist_name": "Miles Davis",
        "artist_logcount": 7.3,
        "album_rowid": 20,
        "album_name": "Kind of Blue",
        "album_name_norm": "kind of blue",
        "album_logcount": 6.1,
        "label_rowid": 30,
        "label": "Columbia",
        "label_logcount": 5.0,
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
        "reprs": [],
    }


def test_panel_album_with_repr(db):
    result = panel_fetch(ALBUM, 20, repr_limit=3, db=db)

    assert result == {
        "entity_type": "album",
        "rowid": 20,
        "album_name_norm": "kind of blue",
        "artist_rowid": 10,
        "artist_name": "Miles Davis",
        "artist_logcount": 7.3,
        "label_rowid": 30,
        "label": "Columbia",
        "label_logcount": 5.0,
        "lon": 3.3,
        "lat": 4.4,
        "artist_lon": 5.5,
        "artist_lat": 6.6,
        "label_lon": 7.7,
        "label_lat": 8.8,
        "logcount": 6.1,
        "nrepr": 2,
        "total_tracks": 5,
        "release_date": "1959-08-17",
        "album_type": "album",
        "reprs": [
            {
                "entity_type": "track",
                "rowid": 1,
                "track_name_norm": "Blue in Green",
                "artist_name": "Miles Davis",
                "lon": 1.1,
                "lat": 2.2,
                "logcount": 4.7,
            },
            {
                "entity_type": "track",
                "rowid": 2,
                "track_name_norm": "So What",
                "artist_name": "Miles Davis",
                "lon": 1.2,
                "lat": 2.3,
                "logcount": 4.5,
            },
        ],
    }


def test_panel_artist_with_repr(db):
    result = panel_fetch(ARTIST, 10, repr_limit=3, db=db)

    assert result == {
        "entity_type": "artist",
        "rowid": 10,
        "artist_name": "Miles Davis",
        "lon": 5.5,
        "lat": 6.6,
        "logcount": 7.3,
        "ntrack": 128,
        "nalbum": 42,
        "nrepr": 2,
        "artist_genre": "jazz",
        "reprs": [
            {
                "entity_type": "album",
                "rowid": 20,
                "album_name_norm": "kind of blue",
                "artist_name": "Miles Davis",
                "lon": 3.3,
                "lat": 4.4,
                "logcount": 6.1,
            },
            {
                "entity_type": "album",
                "rowid": 21,
                "album_name_norm": "nefertiti",
                "artist_name": "Miles Davis",
                "lon": 3.4,
                "lat": 4.5,
                "logcount": 5.2,
            },
        ],
    }


def test_panel_label_with_repr(db):
    result = panel_fetch(LABEL, 30, repr_limit=3, db=db)

    assert result == {
        "entity_type": "label",
        "rowid": 30,
        "label": "Columbia",
        "lon": 7.7,
        "lat": 8.8,
        "logcount": 5.0,
        "ntrack": 500,
        "nalbum": 100,
        "nartist": 50,
        "nrepr": 1,
        "reprs": [
            {
                "entity_type": "artist",
                "rowid": 10,
                "artist_name": "Miles Davis",
                "lon": 5.5,
                "lat": 6.6,
                "logcount": 7.3,
            },
        ],
    }


def test_panel_missing_row_returns_none(db):
    assert panel_fetch(TRACK, 999, repr_limit=3, db=db) is None


def test_panel_track_ignores_repr_limit(db):
    result = panel_fetch(TRACK, 1, repr_limit=1, db=db)

    assert result is not None
    assert result["reprs"] == []


def test_panel_respects_repr_limit(db):
    result = panel_fetch(ALBUM, 20, repr_limit=1, db=db)

    assert result is not None
    assert len(result["reprs"]) == 1
    assert result["reprs"][0]["rowid"] == 1


def test_panel_empty_repr_when_nrepr_zero(db):
    assert panel_fetch(ALBUM, 23, repr_limit=3, db=db) == {
        "entity_type": "album",
        "rowid": 23,
        "album_name_norm": "no repr album",
        "artist_rowid": 11,
        "artist_name": "Herbie Hancock",
        "artist_logcount": 6.8,
        "label_rowid": 31,
        "label": "Blue Note",
        "label_logcount": 5.5,
        "lon": 3.6,
        "lat": 4.7,
        "artist_lon": 5.6,
        "artist_lat": 6.7,
        "label_lon": 7.8,
        "label_lat": 8.9,
        "logcount": 4.8,
        "nrepr": 0,
        "total_tracks": 4,
        "release_date": "1970-01-01",
        "album_type": "album",
        "reprs": [],
    }
