import sqlite3
from typing import Literal, TypedDict

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from src.shared import get_db
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import Entity
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity

router = APIRouter()


class TrackRepr(TypedDict):
    entity_type: Literal["track"]
    rowid: int
    track_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float


class AlbumRepr(TypedDict):
    entity_type: Literal["album"]
    rowid: int
    album_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float


class ArtistRepr(TypedDict):
    entity_type: Literal["artist"]
    rowid: int
    artist_name: str
    lon: float
    lat: float
    logcount: float


Repr = TrackRepr | AlbumRepr | ArtistRepr


def repr_fetch(
    entity: Entity,
    rowid: int,
    limit: int,
    db: sqlite3.Connection,
) -> list[Repr]:
    match entity:
        case TrackEntity():
            return []
        case AlbumEntity():
            child_repr_cls = TrackRepr
            query = """
                SELECT
                    'track' AS entity_type,
                    c.track_rowid AS rowid,
                    c.track_name_norm,
                    c.artist_name,
                    c.lon,
                    c.lat,
                    c.logcount
                FROM album_repr_tracks AS r
                JOIN tracks AS c ON r.track_rowid = c.track_rowid
                WHERE r.album_rowid = ?
                ORDER BY r.rank ASC
                LIMIT ?
            """
        case ArtistEntity():
            child_repr_cls = AlbumRepr
            query = """
                SELECT
                    'album' AS entity_type,
                    c.album_rowid AS rowid,
                    c.album_name_norm,
                    c.artist_name,
                    c.lon,
                    c.lat,
                    c.logcount
                FROM artist_repr_albums AS r
                JOIN albums AS c ON r.album_rowid = c.album_rowid
                WHERE r.artist_rowid = ?
                ORDER BY r.rank ASC
                LIMIT ?
            """
        case LabelEntity():
            child_repr_cls = ArtistRepr
            query = """
                SELECT
                    'artist' AS entity_type,
                    c.artist_rowid AS rowid,
                    c.artist_name,
                    c.lon,
                    c.lat,
                    c.logcount
                FROM label_repr_artists AS r
                JOIN artists AS c ON r.artist_rowid = c.artist_rowid
                WHERE r.label_rowid = ?
                ORDER BY r.rank ASC
                LIMIT ?
            """
    repr_rows = db.execute(query, (rowid, limit)).fetchall()
    return [child_repr_cls(**dict(row)) for row in repr_rows]


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
