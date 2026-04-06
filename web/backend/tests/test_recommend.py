import numpy as np
import pytest

from src import recommend as recommend_module


class FakeIndex:
    def __init__(self, ids: list[int]):
        self.ids = ids

    def search(self, emb, fetch_k):
        assert emb.shape == (1, 2)
        return None, np.array([self.ids[:fetch_k]], dtype=np.int64)


class FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeDb:
    def execute(self, query, params):
        if "SELECT embedding" in query:
            emb = np.array([1.0, 2.0], dtype=np.float32).tobytes()
            return FakeCursor((emb,))
        if "artist_rowid" in query:
            # Return artist rows in a different order than neighbor_ids.
            return FakeResult([(13, 103), (11, 101), (12, 102)])
        if "SELECT track_rowid, track_name, artist_name, lon, lat" in query:
            # Return recommendation rows in a different order than neighbor_ids.
            return FakeResult(
                [
                    (12, "track 12", "artist 102", 12.0, 22.0),
                    (11, "track 11", "artist 101", 11.0, 21.0),
                    (13, "track 13", "artist 103", 13.0, 23.0),
                ]
            )
        raise AssertionError(query)


@pytest.mark.anyio
async def test_recommend_preserves_faiss_order(monkeypatch):
    monkeypatch.setattr(recommend_module, "faiss_track_index", FakeIndex([10, 13, 11, 12]))
    monkeypatch.setattr(recommend_module, "sick_db", FakeDb())

    result = await recommend_module.recommend(rowid=10, entity_name="track", limit=3, diverse=True)

    assert [row["track_rowid"] for row in result] == [13, 11, 12]
