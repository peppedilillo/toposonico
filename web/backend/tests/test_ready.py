from fastapi import FastAPI
import httpx
import pytest

from src.ready import router as ready_router
from src.shared import FaissIndexes


class FakeFaissIndex:
    def __init__(self, ntotal: int = 1):
        self.ntotal = ntotal


class FakeMeiliIndex:
    def get_stats(self):
        return {"numberOfDocuments": 1}


@pytest.mark.anyio
async def test_ready_endpoint_ok(monkeypatch, db):
    app = FastAPI()
    app.include_router(ready_router)
    indexes = FaissIndexes(
        track=FakeFaissIndex(),
        album=FakeFaissIndex(),
        artist=FakeFaissIndex(),
        label=FakeFaissIndex(),
    )
    monkeypatch.setattr("src.ready.check_config", lambda: None)
    monkeypatch.setattr("src.ready.get_db", lambda: db)
    monkeypatch.setattr("src.ready.get_faiss_indexes", lambda: indexes)
    monkeypatch.setattr("src.ready.get_meili_index", lambda: FakeMeiliIndex())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/api/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_ready_endpoint_failure_returns_503(monkeypatch, db):
    app = FastAPI()
    app.include_router(ready_router)
    indexes = FaissIndexes(
        track=FakeFaissIndex(0),
        album=FakeFaissIndex(),
        artist=FakeFaissIndex(),
        label=FakeFaissIndex(),
    )
    monkeypatch.setattr("src.ready.check_config", lambda: None)
    monkeypatch.setattr("src.ready.get_db", lambda: db)
    monkeypatch.setattr("src.ready.get_faiss_indexes", lambda: indexes)
    monkeypatch.setattr("src.ready.get_meili_index", lambda: FakeMeiliIndex())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/api/ready")

    assert response.status_code == 503
    assert response.json() == {"detail": "Readiness check failed"}
