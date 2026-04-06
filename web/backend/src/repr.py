from typing import TypedDict
import sqlite3

from fastapi import APIRouter, HTTPException, Query

from src.shared import get_db
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import cols
from src.utils import Entity
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity

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


class ArtistRepr(TypedDict):
    artist_rowid: int
    artist_name: str
    lon: float
    lat: float


Repr = TrackRepr | AlbumRepr | ArtistRepr


def repr_fetch(entity: Entity, rowid: int, limit: int, db: sqlite3.Connection,) -> list[Repr]:
    match entity:
        case TrackEntity():
            return []
        case AlbumEntity():
            child_repr_cls = TrackRepr
        case ArtistEntity():
            child_repr_cls = AlbumRepr
        case LabelEntity():
            child_repr_cls = ArtistRepr

    child = entity.repr_entity
    repr_cols = cols(child_repr_cls)
    repr_select = ", ".join(f"c.{col}" for col in repr_cols)
    repr_rows = db.execute(
        f"SELECT {repr_select} "
        f"FROM {entity.repr} AS r "
        f"JOIN {child.table} AS c ON r.{child.key} = c.{child.key} "
        f"WHERE r.{entity.key} = ? "
        f"ORDER BY c.{child.key} ASC "
        f"LIMIT ?",
        (rowid, limit),
    ).fetchall()
    return [child_repr_cls(**dict(zip(repr_cols, row))) for row in repr_rows]


@router.get("/api/repr")
async def repr(
    rowid: int,
    entity_name: str,
    limit: int = Query(3, ge=1, le=3),
) -> list[Repr]:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail="Entity not found")
    entity = NAME2ENTITY[entity_name]
    return repr_fetch(entity, rowid, limit, get_db())
