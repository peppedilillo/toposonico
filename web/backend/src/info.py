import sqlite3
from typing import Literal, TypedDict

from fastapi import APIRouter
from fastapi import HTTPException

from src.shared import get_db
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import Entity
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity

router = APIRouter()


class TrackInfo(TypedDict):
    entity_type: Literal["track"]
    rowid: int
    track_name_norm: str
    artist_rowid: int
    artist_name: str
    artist_logcount: float
    album_rowid: int
    album_name: str
    album_name_norm: str
    album_logcount: float
    label_rowid: int
    label: str
    label_logcount: float
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
    entity_type: Literal["album"]
    rowid: int
    album_name_norm: str
    artist_rowid: int
    artist_name: str
    artist_logcount: float
    label_rowid: int
    label: str
    label_logcount: float
    lon: float
    lat: float
    artist_lon: float
    artist_lat: float
    label_lon: float
    label_lat: float
    logcount: float
    nrepr: int
    total_tracks: int | None
    release_date: str | None
    album_type: str | None


class ArtistInfo(TypedDict):
    entity_type: Literal["artist"]
    rowid: int
    artist_name: str
    lon: float
    lat: float
    logcount: float
    ntrack: int
    nalbum: int
    nrepr: int
    artist_genre: str | None


class LabelInfo(TypedDict):
    entity_type: Literal["label"]
    rowid: int
    label: str
    lon: float
    lat: float
    logcount: float
    ntrack: int
    nalbum: int
    nartist: int
    nrepr: int


Info = TrackInfo | LabelInfo | AlbumInfo | ArtistInfo


def info_fetch(entity: Entity, rowid: int, db: sqlite3.Connection) -> Info | None:
    match entity:
        case TrackEntity():
            info_cls = TrackInfo
            query = """
                SELECT
                    'track' AS entity_type,
                    track_rowid AS rowid,
                    track_name_norm,
                    artist_rowid,
                    artist_name,
                    artist_logcount,
                    album_rowid,
                    album_name,
                    album_name_norm,
                    album_logcount,
                    label_rowid,
                    label,
                    label_logcount,
                    lon,
                    lat,
                    album_lon,
                    album_lat,
                    artist_lon,
                    artist_lat,
                    label_lon,
                    label_lat,
                    logcount,
                    release_date
                FROM tracks
                WHERE track_rowid = ?
            """
        case AlbumEntity():
            info_cls = AlbumInfo
            query = """
                SELECT
                    'album' AS entity_type,
                    album_rowid AS rowid,
                    album_name_norm,
                    artist_rowid,
                    artist_name,
                    artist_logcount,
                    label_rowid,
                    label,
                    label_logcount,
                    lon,
                    lat,
                    artist_lon,
                    artist_lat,
                    label_lon,
                    label_lat,
                    logcount,
                    nrepr,
                    total_tracks,
                    release_date,
                    album_type
                FROM albums
                WHERE album_rowid = ?
            """
        case ArtistEntity():
            info_cls = ArtistInfo
            query = """
                SELECT
                    'artist' AS entity_type,
                    artist_rowid AS rowid,
                    artist_name,
                    lon,
                    lat,
                    logcount,
                    ntrack,
                    nalbum,
                    nrepr,
                    artist_genre
                FROM artists
                WHERE artist_rowid = ?
            """
        case LabelEntity():
            info_cls = LabelInfo
            query = """
                SELECT
                    'label' AS entity_type,
                    label_rowid AS rowid,
                    label,
                    lon,
                    lat,
                    logcount,
                    ntrack,
                    nalbum,
                    nartist,
                    nrepr
                FROM labels
                WHERE label_rowid = ?
            """
    row = db.execute(query, (rowid,)).fetchone()
    if row is None:
        return None
    return info_cls(**dict(row))


@router.get("/api/info")
async def info(rowid: int, entity_name: str) -> Info:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_name}' not supported.")
    entity = NAME2ENTITY[entity_name]
    entity_info = info_fetch(entity, rowid, get_db())
    if entity_info is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return entity_info
