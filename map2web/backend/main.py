import os

import meilisearch
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
)

_meili_url = os.environ.get("MEILI_URL", "http://localhost:7700")
_meili_key = os.environ.get("MEILI_MASTER_KEY")
_index_name = os.environ.get("MEILI_INDEX_NAME")
if _index_name is None:
    raise RuntimeError("MEILI_INDEX_NAME is not set. source config.env before starting.")

_client = meilisearch.Client(_meili_url, _meili_key)
_index = _client.index(_index_name)


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    result = _index.search(q, {"limit": limit})
    return result["hits"]
