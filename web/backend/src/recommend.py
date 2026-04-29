import sqlite3
from typing import Literal, TypedDict

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
    entity_type: Literal["track"]
    rowid: int
    track_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float
    simscore: float


class AlbumRecommend(TypedDict):
    entity_type: Literal["album"]
    rowid: int
    album_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float
    simscore: float


class ArtistRecommend(TypedDict):
    entity_type: Literal["artist"]
    rowid: int
    artist_name: str
    lon: float
    lat: float
    logcount: float
    simscore: float
    artist_genre: str | None


class LabelRecommend(TypedDict):
    entity_type: Literal["label"]
    rowid: int
    label: str
    lon: float
    lat: float
    logcount: float
    simscore: float


Recommend = TrackRecommend | AlbumRecommend | ArtistRecommend | LabelRecommend


class RecommendMeta(TypedDict, total=False):
    rowid: int
    logcount: float
    artist_rowid: int


def recommend_fetch(
    entity: Entity,
    rowid: int,
    limit: int,
    diverse: bool,
    popfloor: float,
    db: sqlite3.Connection,
    indexes: FaissIndexes,
) -> list[Recommend] | None:
    match entity:
        case TrackEntity():
            rec_cls = TrackRecommend
            index = indexes.track
            meta_query = """
                SELECT
                    track_rowid AS rowid,
                    logcount,
                    artist_rowid
                FROM tracks
                WHERE track_rowid IN ({placeholders})
            """
            payload_query = """
                SELECT
                    'track' AS entity_type,
                    track_rowid AS rowid,
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
            meta_query = """
                SELECT
                    album_rowid AS rowid,
                    logcount,
                    artist_rowid
                FROM albums
                WHERE album_rowid IN ({placeholders})
            """
            payload_query = """
                SELECT
                    'album' AS entity_type,
                    album_rowid AS rowid,
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
            meta_query = """
                SELECT
                    artist_rowid AS rowid,
                    logcount
                FROM artists
                WHERE artist_rowid IN ({placeholders})
            """
            payload_query = """
                SELECT
                    'artist' AS entity_type,
                    artist_rowid AS rowid,
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
            meta_query = """
                SELECT
                    label_rowid AS rowid,
                    logcount
                FROM labels
                WHERE label_rowid IN ({placeholders})
            """
            payload_query = """
                SELECT
                    'label' AS entity_type,
                    label_rowid AS rowid,
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
    # oversample when post-search filtering is active so we can still surface
    # enough valid rows after applying popularity and diversity constraints.
    fetch_k = 20 * limit + 1 if diversifiable or popfloor > 0.0 else limit + 1
    fetch_k = min(fetch_k, index.ntotal)
    sims, ids = index.search(emb, fetch_k)  # noqa
    sim_map = {int(nid): float(sim) for sim, nid in zip(sims[0], ids[0]) if nid != -1 and nid != rowid}
    neighbor_ids = list(sim_map)
    if not neighbor_ids:
        return []

    placeholders = ", ".join("?" * len(neighbor_ids))
    meta_rows = db.execute(
        meta_query.format(placeholders=placeholders),
        neighbor_ids,
    ).fetchall()

    meta_map: dict[int, RecommendMeta] = {int(row["rowid"]): RecommendMeta(**dict(row)) for row in meta_rows}
    selected_ids: list[int] = []
    seen_artists: set[int] = set()
    for nid in neighbor_ids:
        meta = meta_map.get(nid)
        if meta is None or meta["logcount"] <= popfloor:
            continue
        if diversifiable:
            artist_rowid = meta["artist_rowid"]
            if artist_rowid in seen_artists:
                continue
            seen_artists.add(artist_rowid)
        selected_ids.append(nid)
        if len(selected_ids) == limit:
            break

    if not selected_ids:
        return []

    placeholders = ", ".join("?" * len(selected_ids))
    rec_rows = db.execute(
        payload_query.format(placeholders=placeholders),
        selected_ids,
    ).fetchall()

    rec_map = {}
    for rec in rec_rows:
        rec_data = dict(rec)
        rec_data["simscore"] = sim_map[rec_data["rowid"]]
        rec_map[rec_data["rowid"]] = rec_cls(**rec_data)
    return [rec_map[nid] for nid in selected_ids if nid in rec_map]


@router.get("/api/recommend")
async def recommend(
    rowid: int,
    entity_name: str,
    limit: int = Query(10, ge=1, le=50),
    diverse: bool = True,
    popfloor: int = Query(0, ge=0),
) -> list[Recommend]:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail="Entity not found")
    entity = NAME2ENTITY[entity_name]
    results = recommend_fetch(entity, rowid, limit, diverse, popfloor, get_db(), get_faiss_indexes())
    if results is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return results
