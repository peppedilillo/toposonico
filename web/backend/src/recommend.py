import sqlite3
from typing import TypedDict

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
import numpy as np

from src.shared import FaissIndexes
from src.shared import get_db
from src.shared import get_faiss_indexes
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import Entity
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity


router = APIRouter()


class TrackRecommend(TypedDict):
    track_rowid: int
    track_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float
    simscore: float


class AlbumRecommend(TypedDict):
    album_rowid: int
    album_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float
    simscore: float


class ArtistRecommend(TypedDict):
    artist_rowid: int
    artist_name: str
    lon: float
    lat: float
    logcount: float
    simscore: float
    artist_genre: str | None


class LabelRecommend(TypedDict):
    label_rowid: int
    label: str
    lon: float
    lat: float
    logcount: float
    simscore: float


Recommend = TrackRecommend | AlbumRecommend | ArtistRecommend | LabelRecommend


def recommend_fetch(
    entity: Entity,
    rowid: int,
    limit: int,
    diverse: bool,
    db: sqlite3.Connection,
    indexes: FaissIndexes,
) -> list[Recommend] | None:
    match entity:
        case TrackEntity():
            rec_cls = TrackRecommend
            index = indexes.track
            query = """
                SELECT
                    track_rowid,
                    track_name_norm,
                    artist_name,
                    lon,
                    lat,
                    logcount
                FROM tracks
                WHERE track_rowid IN ({placeholders})
            """
        case AlbumEntity():
            rec_cls = AlbumRecommend
            index = indexes.album
            query = """
                SELECT
                    album_rowid,
                    album_name_norm,
                    artist_name,
                    lon,
                    lat,
                    logcount
                FROM albums
                WHERE album_rowid IN ({placeholders})
            """
        case ArtistEntity():
            rec_cls = ArtistRecommend
            index = indexes.artist
            query = """
                SELECT
                    artist_rowid,
                    artist_name,
                    lon,
                    lat,
                    logcount,
                    artist_genre 
                FROM artists
                WHERE artist_rowid IN ({placeholders})
            """
        case LabelEntity():
            rec_cls = LabelRecommend
            index = indexes.label
            query = """
                SELECT
                    label_rowid,
                    label,
                    lon,
                    lat,
                    logcount
                FROM labels
                WHERE label_rowid IN ({placeholders})
            """
    emb = db.execute(
        f"SELECT embedding FROM {entity.embedding} WHERE {entity.key} = ?",
        (rowid,),
    ).fetchone()
    if emb is None:
        return None

    emb = np.frombuffer(emb[0], dtype=np.float32).reshape(1, -1)
    diversifiable = diverse and isinstance(entity, (TrackEntity, AlbumEntity))
    fetch_k = 10 * limit + 1 if diversifiable else limit + 1
    sims, ids = index.search(emb, fetch_k)  # noqa
    sim_map = {int(nid): float(sim) for sim, nid in zip(sims[0], ids[0]) if nid != -1 and nid != rowid}
    neighbor_ids = list(sim_map)
    if not neighbor_ids:
        return []

    if diversifiable:
        placeholders = ", ".join("?" * len(neighbor_ids))
        artist_rows = db.execute(
            f"SELECT {entity.key}, artist_rowid FROM {entity.table} " f"WHERE {entity.key} IN ({placeholders})",
            neighbor_ids,
        ).fetchall()
        artist_map = {row[0]: row[1] for row in artist_rows}
        seen_artists: set[int] = set()
        diverse_ids: list[int] = []
        for nid in neighbor_ids:
            aid = artist_map.get(nid)
            if aid is not None and aid not in seen_artists:
                seen_artists.add(aid)
                diverse_ids.append(nid)
                if len(diverse_ids) == limit:
                    break
        neighbor_ids = diverse_ids
    else:
        neighbor_ids = neighbor_ids[:limit]
    if not neighbor_ids:
        # this shall IN PRINCIPLE not happen, so it will happen.
        return []

    placeholders = ", ".join("?" * len(neighbor_ids))
    rec_rows = db.execute(
        query.format(placeholders=placeholders),
        neighbor_ids,
    ).fetchall()
    # returns recommends in neighbour order
    rec_map = {}
    for rec in rec_rows:
        rec_data = dict(rec)
        rec_data["simscore"] = sim_map[rec[0]]
        rec_map[rec[0]] = rec_cls(**rec_data)
    return [rec_map[nid] for nid in neighbor_ids if nid in rec_map]


@router.get("/api/recommend")
async def recommend(
    rowid: int,
    entity_name: str,
    limit: int = Query(10, ge=1, le=50),
    diverse: bool = True,
) -> list[Recommend]:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail="Entity not found")
    entity = NAME2ENTITY[entity_name]
    results = recommend_fetch(entity, rowid, limit, diverse, get_db(), get_faiss_indexes())
    if results is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return results
