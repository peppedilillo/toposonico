import sqlite3
from typing import TypedDict

from fastapi import APIRouter
from fastapi import HTTPException

from src.shared import get_db
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import cols
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity
from src.utils import Entity

router = APIRouter()


class TrackInfo(TypedDict):
    track_rowid: int
    track_name: str
    artist_rowid: int
    artist_name: str
    album_rowid: int
    album_name: str
    label_rowid: int
    label: str
    lon: float
    lat: float
    album_lon: float
    album_lat: float
    artist_lon: float
    artist_lat: float
    label_lon: float
    label_lat: float
    logcount: float
    release_date: str | None


class AlbumInfo(TypedDict):
    album_rowid: int
    album_name_norm: str
    artist_rowid: int
    artist_name: str
    label_rowid: int
    label: str
    lon: float
    lat: float
    artist_lon: float
    artist_lat: float
    label_lon: float
    label_lat: float
    logcount: float
    total_tracks: int | None
    release_date: str | None
    album_type: str | None


class ArtistInfo(TypedDict):
    artist_rowid: int
    artist_name: str
    lon: float
    lat: float
    logcount: float
    # TODO: make non null after db schema fix
    nalbum: int | None
    artist_genre: str | None


class LabelInfo(TypedDict):
    label_rowid: int
    label: str
    lon: float
    lat: float
    logcount: float
    # TODO: make non null after db schema fix
    nalbum: int | None
    nartist: int | None


Info = TrackInfo | LabelInfo | AlbumInfo | ArtistInfo


def info_fetch(entity: Entity, rowid: int, db: sqlite3.Connection) -> Info | None:
    match entity:
        case TrackEntity():
            info_cls = TrackInfo
        case AlbumEntity():
            info_cls = AlbumInfo
        case ArtistEntity():
            info_cls = ArtistInfo
        case LabelEntity():
            info_cls = LabelInfo
    keys = cols(info_cls)
    row = db.execute(
        f"SELECT {', '.join(keys)} FROM {entity.table} WHERE {entity.key} = ?",
        (rowid,),
    ).fetchone()
    if row is None:
        return None
    return info_cls(**dict(zip(keys, row)))


@router.get("/api/info")
async def info(rowid: int, entity_name: str) -> Info:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_name}' not supported.")
    entity = NAME2ENTITY[entity_name]
    entity_info = info_fetch(entity, rowid, get_db())
    if entity_info is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return entity_info
