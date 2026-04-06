from typing import TypedDict

from fastapi import APIRouter, HTTPException, Query

from src.shared import sick_db
from src.utils import NAME2ENTITY, TrackEntity, AlbumEntity, ArtistEntity, LabelEntity, cols

router = APIRouter()


class TrackRepr(TypedDict):
    track_rowid: int
    track_name: str
    artist_name: str
    lon: float
    lat: float

class AlbumRepr(TypedDict):
    album_rowid: int
    album_name_norm: str
    artist_name: str
    lon: float
    lat: float

class LabelRepr(TypedDict):
    label_rowid: int
    label: str
    lon: float
    lat: float


Repr = TrackRepr | AlbumRepr | LabelRepr


@router.get("/api/repr")
async def repr(
    rowid: int,
    entity_name: str,
    limit: Query(10, ge=1, le=3),
) -> Repr:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail="Entity not found")
    entity = NAME2ENTITY[entity_name]
    match entity:
        case TrackEntity():
            raise NotImplementedError("Track have no representative child.")
        case AlbumEntity():
            repr_cls = TrackRepr
        case ArtistEntity():
            repr_cls = AlbumRepr
        case LabelEntity():
            repr_cls = LabelRepr

    repr_cols = cols(repr_cls)  # noqa
    repr_rows = sick_db.execute(
        f"SELECT {', '.join(repr_cols)} "
        f"FROM {entity.repr} AS r "
        f"JOIN {entity.repr_join} AS rj ON r.track_id = rj.track_id "
        f"WHERE r.album_id = 'YOUR_ALBUM_ID';"
    ).fetchall()
    return [repr_cls(**dict(zip(repr_cols, repr))) for repr in repr_rows]