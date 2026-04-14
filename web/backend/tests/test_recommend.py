from fastapi import FastAPI
import httpx
import pytest

from src.recommend import recommend_fetch
from src.recommend import router as recommend_router
from src.utils import ALBUM
from src.utils import ARTIST
from src.utils import LABEL
from src.utils import TRACK


def test_recommend_track(db, faiss_indexes):
    results = recommend_fetch(TRACK, 1, limit=3, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert results is not None
    assert len(results) == 3
    assert all(r["track_rowid"] != 1 for r in results)
    assert all(
        set(r.keys())
        == {
            "track_rowid",
            "track_name_norm",
            "artist_name",
            "lon",
            "lat",
            "logcount",
            "simscore",
        }
        for r in results
    )
    assert all(isinstance(r["simscore"], float) for r in results)


def test_recommend_album(db, faiss_indexes):
    results = recommend_fetch(ALBUM, 20, limit=2, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert results is not None
    assert len(results) == 2
    assert all(r["album_rowid"] != 20 for r in results)


def test_recommend_artist(db, faiss_indexes):
    results = recommend_fetch(ARTIST, 10, limit=1, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert results == [
        {
            "artist_rowid": 11,
            "artist_name": "Herbie Hancock",
            "lon": 5.6,
            "lat": 6.7,
            "logcount": 6.8,
            "artist_genre": "jazz",
            "simscore": results[0]["simscore"],
        }
    ]
    assert isinstance(results[0]["simscore"], float)


def test_recommend_label(db, faiss_indexes):
    results = recommend_fetch(LABEL, 30, limit=1, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert results == [
        {
            "label_rowid": 31,
            "label": "Blue Note",
            "lon": 7.8,
            "lat": 8.9,
            "logcount": 5.5,
            "simscore": results[0]["simscore"],
        }
    ]
    assert isinstance(results[0]["simscore"], float)


def test_recommend_track_diverse(db, faiss_indexes):
    results = recommend_fetch(TRACK, 1, limit=3, diverse=True, popfloor=0, db=db, indexes=faiss_indexes)

    assert results is not None
    artist_names = [r["artist_name"] for r in results]
    assert len(artist_names) == len(set(artist_names))


def test_recommend_album_diverse(db, faiss_indexes):
    results = recommend_fetch(ALBUM, 20, limit=2, diverse=True, popfloor=0, db=db, indexes=faiss_indexes)

    assert results is not None
    artist_names = [r["artist_name"] for r in results]
    assert len(artist_names) == len(set(artist_names))


def test_recommend_diverse_noop_for_artist(db, faiss_indexes):
    diverse = recommend_fetch(ARTIST, 10, limit=1, diverse=True, popfloor=0, db=db, indexes=faiss_indexes)
    plain = recommend_fetch(ARTIST, 10, limit=1, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert diverse == plain


def test_recommend_logcount_filter_disabled_for_low_pivot(db, faiss_indexes):
    baseline = recommend_fetch(TRACK, 1, limit=3, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)
    filtered = recommend_fetch(TRACK, 1, limit=3, diverse=False, popfloor=-1, db=db, indexes=faiss_indexes)

    assert filtered == baseline


def test_recommend_diverse_filters_and_logcount_can_underfill(db, faiss_indexes):
    results = recommend_fetch(ALBUM, 20, limit=2, diverse=True, popfloor=5.4, db=db, indexes=faiss_indexes)

    assert results is not None
    assert len(results) == 1
    assert all(r["logcount"] > 5.4 for r in results)
    assert results[0]["album_rowid"] == 22


def test_recommend_logcount_filter_applies_to_artist(db, faiss_indexes):
    results = recommend_fetch(ARTIST, 10, limit=1, diverse=False, popfloor=6.8, db=db, indexes=faiss_indexes)

    assert results == []


def test_recommend_logcount_filter_applies_to_label(db, faiss_indexes):
    results = recommend_fetch(LABEL, 30, limit=1, diverse=False, popfloor=5.5, db=db, indexes=faiss_indexes)

    assert results == []


def test_recommend_diverse_false_keeps_same_artist_candidates(db, faiss_indexes):
    results = recommend_fetch(ALBUM, 20, limit=2, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert results is not None
    assert [r["album_rowid"] for r in results] == [21, 22]


def test_recommend_track_popfloor_filters_low_count_rows(db, faiss_indexes):
    results = recommend_fetch(TRACK, 1, limit=3, diverse=False, popfloor=4, db=db, indexes=faiss_indexes)

    assert results is not None
    assert len(results) == 2
    assert all(r["logcount"] > 4 for r in results)


def test_recommend_missing_row(db, faiss_indexes):
    result = recommend_fetch(TRACK, 999, limit=3, diverse=False, popfloor=0, db=db, indexes=faiss_indexes)

    assert result is None


@pytest.mark.anyio
async def test_recommend_endpoint_rejects_negative_popfloor(monkeypatch, db, faiss_indexes):
    app = FastAPI()
    app.include_router(recommend_router)
    monkeypatch.setattr("src.recommend.get_db", lambda: db)
    monkeypatch.setattr("src.recommend.get_faiss_indexes", lambda: faiss_indexes)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get(
            "/api/recommend",
            params={"rowid": 1, "entity_name": "track", "popfloor": -1},
        )

    assert response.status_code == 422
