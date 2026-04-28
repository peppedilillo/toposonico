import pytest

from src.search import dedup
from src.search import search_map
from src.search import search_mid2eid


def track_hit(**overrides):
    hit = {
        "entity_type": "track",
        "rowid": 1,
        "track_name_norm": "Blue in Green",
        "artist_name": "Miles Davis",
        "lon": 1.1,
        "lat": 2.2,
        "logcount": 4.7,
    }
    return hit | overrides


def album_hit(**overrides):
    hit = {
        "entity_type": "album",
        "rowid": 2,
        "album_name_norm": "Kind of Blue",
        "artist_name": "Miles Davis",
        "lon": 3.3,
        "lat": 4.4,
        "logcount": 6.1,
    }
    return hit | overrides


def artist_hit(**overrides):
    hit = {
        "entity_type": "artist",
        "rowid": 3,
        "artist_name": "Miles Davis",
        "lon": 5.5,
        "lat": 6.6,
        "logcount": 7.3,
    }
    return hit | overrides


def label_hit(**overrides):
    hit = {
        "entity_type": "label",
        "rowid": 4,
        "label": "Columbia",
        "lon": 7.7,
        "lat": 8.8,
        "logcount": 5.0,
    }
    return hit | overrides


def test_search_map_track():
    hit = {
        "id": "track_1",
        "track_name_norm": "Blue in Green",
        "artist_name": "Miles Davis",
        "lon": 1.1,
        "lat": 2.2,
        "logcount": 4.7,
    }
    result = search_map(hit)
    assert result == {
        "entity_type": "track",
        "rowid": 1,
        "track_name_norm": "Blue in Green",
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
        "rowid": 2,
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
        "rowid": 3,
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
        "rowid": 4,
        "label": "Columbia",
        "lon": 7.7,
        "lat": 8.8,
        "logcount": 5.0,
    }


def test_search_mid2eid_invalid():
    with pytest.raises(ValueError):
        search_mid2eid("invalid_123")


def test_dedup_keeps_first_hit_for_each_duplicate_key():
    first_track = track_hit(rowid=1, lon=1.1, lat=2.2, logcount=4.7)
    duplicate_track = track_hit(rowid=2, lon=9.9, lat=8.8, logcount=3.1)
    other_track = track_hit(rowid=3, track_name_norm="So What")

    assert dedup([first_track, duplicate_track, other_track]) == [
        first_track,
        other_track,
    ]


def test_dedup_keys_by_entity_type():
    track = track_hit(track_name_norm="Kind of Blue")
    album = album_hit(album_name_norm="Kind of Blue")
    artist = artist_hit()
    label = label_hit()

    assert dedup([track, album, artist, label, label_hit(rowid=5)]) == [
        track,
        album,
        artist,
        label,
    ]


def test_dedup_strips_whitespace_and_ignores_case():
    first_track = track_hit(track_name_norm="Straße", artist_name="CAN")
    duplicate_track = track_hit(
        rowid=2,
        track_name_norm=" strasse ",
        artist_name=" can ",
    )
    other_track = track_hit(rowid=3, artist_name="Canal")

    assert dedup([first_track, duplicate_track, other_track]) == [
        first_track,
        other_track,
    ]


def test_dedup_rejects_unknown_entity_type():
    with pytest.raises(AssertionError):
        dedup([track_hit(entity_type="unknown")])
