import os
import random
import sqlite3

import meilisearch
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
)

_meili_url  = os.environ.get("MEILI_URL", "http://localhost:7700")
_meili_key  = os.environ.get("MEILI_MASTER_KEY")
_index_name = os.environ.get("MEILI_INDEX_NAME")
if _index_name is None:
    raise RuntimeError("MEILI_INDEX_NAME is not set. source config.env before starting.")

_client = meilisearch.Client(_meili_url, _meili_key)
_index  = _client.index(_index_name)

_db_path = os.environ.get("T2M_DB")
if _db_path is None:
    raise RuntimeError("T2M_DB is not set. source config.env before starting.")
_db = sqlite3.connect(f"file:{_db_path}?mode=ro", uri=True, check_same_thread=False)

_ENTITY_TABLE = {
    "track":  ("tracks",  "track_rowid"),
    "album":  ("albums",  "album_rowid"),
    "artist": ("artists", "artist_rowid"),
    "label":  ("labels",  "label_id"),
}

_KNN_TABLE = {
    "track":  ("track_knn",  "track_rowid",  "tracks",  "track_rowid",  "neighbor_rowid"),
    "album":  ("album_knn",  "album_rowid",  "albums",  "album_rowid",  "neighbor_rowid"),
    "artist": ("artist_knn", "artist_rowid", "artists", "artist_rowid", "neighbor_rowid"),
    "label":  ("label_knn",  "label_id",     "labels",  "label_id",     "neighbor_id"),
}

# K neighbours per entity — uniform across all rows (fixed at KNN build time)
_KNN_K = {
    entity: _db.execute(f"SELECT MAX(rank) + 1 FROM {knn_table}").fetchone()[0] or 0
    for entity, (knn_table, *_) in _KNN_TABLE.items()
}
assert all(k > 0 for k in _KNN_K.values()), f"Empty KNN table(s): {_KNN_K}"


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    result = _index.search(q, {"limit": limit})
    return result["hits"]


@app.get("/api/info")
async def info(q: int, entity: str):
    if entity not in _ENTITY_TABLE:
        raise HTTPException(status_code=400, detail=f"Unknown entity: {entity!r}")
    table, pk = _ENTITY_TABLE[entity]
    cur = _db.execute(f"SELECT * FROM {table} WHERE {pk} = ?", (q,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"{entity} {q} not found")
    return dict(zip([d[0] for d in cur.description], row))


@app.get("/api/recroll")
async def recroll(q: int, entity: str):
    if entity not in _KNN_TABLE:
        raise HTTPException(status_code=400, detail=f"Unknown entity: {entity!r}")
    knn_table, knn_pk, entity_table, entity_pk, neighbor_col = _KNN_TABLE[entity]

    k = _KNN_K[entity]
    top_k = max(1, k // 5)
    if random.randint(1, 10) <= 8:
        rank_filter = f"k.rank < {top_k}"
    else:
        rank_filter = f"k.rank >= {top_k}"

    sql = f"""
        SELECT e.*
        FROM {knn_table} k
        JOIN {entity_table} e ON e.{entity_pk} = k.{neighbor_col}
        WHERE k.{knn_pk} = ? AND {rank_filter}
        ORDER BY RANDOM()
        LIMIT 1
    """
    cur = _db.execute(sql, (q,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No neighbours for {entity} {q}")
    return dict(zip([d[0] for d in cur.description], row))


@app.get("/api/recs")
async def recs(q: int, entity: str, limit: int = Query(5, ge=1)):
    if entity not in _KNN_TABLE:
        raise HTTPException(status_code=400, detail=f"Unknown entity: {entity!r}")
    knn_table, knn_pk, entity_table, entity_pk, neighbor_col = _KNN_TABLE[entity]
    sql = f"""
        SELECT k.{neighbor_col}, k.score, e.*
        FROM {knn_table} k
        JOIN {entity_table} e ON e.{entity_pk} = k.{neighbor_col}
        WHERE k.{knn_pk} = ?
        ORDER BY k.rank
        LIMIT ?
    """
    cur  = _db.execute(sql, (q, limit))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]


@app.get("/api/knn")
async def knn(q: int, entity: str, limit: int = Query(20, ge=1, le=100)):
    if entity not in _KNN_TABLE:
        raise HTTPException(status_code=400, detail=f"Unknown entity: {entity!r}")
    knn_table, knn_pk, entity_table, entity_pk, neighbor_col = _KNN_TABLE[entity]
    sql = f"""
        SELECT k.{neighbor_col}, k.score, e.*
        FROM {knn_table} k
        JOIN {entity_table} e ON e.{entity_pk} = k.{neighbor_col}
        WHERE k.{knn_pk} = ?
        ORDER BY k.rank
        LIMIT ?
    """
    cur  = _db.execute(sql, (q, limit))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]
