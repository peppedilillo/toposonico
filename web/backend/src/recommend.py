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
    album_name: str
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
    limit: int = Query(10, ge=1, le=20),
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
    _, ids = index.search(emb, limit + 1)  # noqa
    neighbor_ids = [int(i) for i in ids[0] if i != -1 and i != rowid][:limit]
    if not neighbor_ids:
        return []

    rec_cols = cols(rec_cls)  # noqa
    placeholders = ", ".join("?" * len(neighbor_ids))
    rec_rows = sick_db.execute(
        f"SELECT {', '.join(rec_cols)} FROM {entity.table} WHERE {entity.key} IN ({placeholders})",
        neighbor_ids,
    ).fetchall()
    return [rec_cls(**dict(zip(rec_cols, rec))) for rec in rec_rows]  # noqa
