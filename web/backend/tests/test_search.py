import pytest

from src.search import search_map
from src.search import search_mid2eid


def test_search_map_track():
    hit = {
        "id": "track_1",
        "track_name": "Blue in Green",
        "artist_name": "Miles Davis",
        "lon": 1.1,
        "lat": 2.2,
        "logcount": 4.7,
    }
    result = search_map(hit)
    assert result == {
        "entity_type": "track",
        "track_rowid": 1,
        "track_name": "Blue in Green",
        "artist_name": "Miles Davis",
        "lon": 1.1,
        "lat": 2.2,
        "logcount": 4.7,
    }


def test_search_map_album():
    hit = {
        "id": "album_2",
        "album_name_norm": "Kind of Blue",
        "artist_name": "Miles Davis",
        "lon": 3.3,
        "lat": 4.4,
        "logcount": 6.1,
    }
    result = search_map(hit)
    assert result == {
        "entity_type": "album",
        "album_rowid": 2,
        "album_name_norm": "Kind of Blue",
        "artist_name": "Miles Davis",
        "lon": 3.3,
        "lat": 4.4,
        "logcount": 6.1,
    }


def test_search_map_artist():
    hit = {
        "id": "artist_3",
        "artist_name": "Miles Davis",
        "lon": 5.5,
        "lat": 6.6,
        "logcount": 7.3,
    }
    result = search_map(hit)
    assert result == {
        "entity_type": "artist",
        "artist_rowid": 3,
        "artist_name": "Miles Davis",
        "lon": 5.5,
        "lat": 6.6,
        "logcount": 7.3,
    }


def test_search_map_label():
    hit = {
        "id": "label_4",
        "label": "Columbia",
        "lon": 7.7,
        "lat": 8.8,
        "logcount": 5.0,
    }
    result = search_map(hit)
    assert result == {
        "entity_type": "label",
        "label_rowid": 4,
        "label": "Columbia",
        "lon": 7.7,
        "lat": 8.8,
        "logcount": 5.0,
    }


def test_search_mid2eid_invalid():
    with pytest.raises(ValueError):
        search_mid2eid("invalid_123")
