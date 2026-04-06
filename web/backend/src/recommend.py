from typing import TypedDict

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
import numpy as np

from src.shared import faiss_album_index
from src.shared import faiss_artist_index
from src.shared import faiss_label_index
from src.shared import faiss_track_index
from src.shared import sick_db
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import cols
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity

router = APIRouter()


class TrackRecommend(TypedDict):
    track_rowid: int
    track_name: str
    artist_name: str
    lon: float
    lat: float


class AlbumRecommend(TypedDict):
    album_rowid: int
    album_name_norm: str
    artist_name: str
    lon: float
    lat: float


class ArtistRecommend(TypedDict):
    artist_rowid: int
    artist_name: str
    lon: float
    lat: float


class LabelRecommend(TypedDict):
    label_rowid: int
    label: str
    lon: float
    lat: float


Recommend = TrackRecommend | AlbumRecommend | ArtistRecommend | LabelRecommend


@router.get("/api/recommend")
async def recommend(
    rowid: int,
    entity_name: str,
    limit: int = Query(10, ge=1, le=10),
    diverse: bool = True,
) -> list[Recommend]:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail="Entity not found")
    entity = NAME2ENTITY[entity_name]
    match entity:
        case TrackEntity():
            rec_cls = TrackRecommend
            index = faiss_track_index
        case AlbumEntity():
            rec_cls = AlbumRecommend
            index = faiss_album_index
        case ArtistEntity():
            rec_cls = ArtistRecommend
            index = faiss_artist_index
        case LabelEntity():
            rec_cls = LabelRecommend
            index = faiss_label_index

    row = sick_db.execute(
        f"SELECT embedding FROM {entity.embedding} WHERE {entity.key} = ?",
        (rowid,),
    ).fetchone()

    emb = np.frombuffer(row[0], dtype=np.float32).reshape(1, -1)
    diversifiable = diverse and isinstance(entity, (TrackEntity, AlbumEntity))
    fetch_k = 10 * limit + 1 if diversifiable else limit + 1
    _, ids = index.search(emb, fetch_k)  # noqa
    neighbor_ids = [int(i) for i in ids[0] if i != -1 and i != rowid]

    if diversifiable:
        placeholders = ", ".join("?" * len(neighbor_ids))
        artist_rows = sick_db.execute(
            f"SELECT {entity.key}, artist_rowid FROM {entity.table} "
            f"WHERE {entity.key} IN ({placeholders})",
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
        return []

    rec_cols = cols(rec_cls)  # noqa
    placeholders = ", ".join("?" * len(neighbor_ids))
    rec_rows = sick_db.execute(
        f"SELECT {', '.join(rec_cols)} FROM {entity.table} WHERE {entity.key} IN ({placeholders})",
        neighbor_ids,
    ).fetchall()
    # returns recommends in neighbour order
    rec_map = {
        rec[0]: rec_cls(**dict(zip(rec_cols, rec)))
        for rec in rec_rows
    }
    return [rec_map[nid] for nid in neighbor_ids if nid in rec_map]
