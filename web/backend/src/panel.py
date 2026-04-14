import sqlite3
from typing import TypedDict

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query

from src.info import AlbumInfo
from src.info import ArtistInfo
from src.info import info_fetch
from src.info import LabelInfo
from src.info import TrackInfo
from src.repr import AlbumRepr
from src.repr import ArtistRepr
from src.repr import repr_fetch
from src.repr import TrackRepr
from src.shared import get_db
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import Entity
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity

router = APIRouter()


class TrackPanel(TrackInfo):
    reprs: list[None]


class AlbumPanel(AlbumInfo):
    reprs: list[TrackRepr]


class ArtistPanel(ArtistInfo):
    reprs: list[AlbumRepr]


class LabelPanel(LabelInfo):
    reprs: list[ArtistRepr]


Panel = TrackPanel | AlbumPanel | ArtistPanel | LabelPanel


def panel_fetch(entity: Entity, rowid: int, repr_limit: int, db: sqlite3.Connection) -> Panel | None:
    info = info_fetch(entity, rowid, db)
    if info is None:
        return None

    match entity:
        case TrackEntity():
            return TrackPanel(**info, reprs=[])
        case AlbumEntity():
            if info["nrepr"] == 0:
                return AlbumPanel(**info, reprs=[])
            return AlbumPanel(**info, reprs=repr_fetch(entity, rowid, repr_limit, db))
        case ArtistEntity():
            if info["nrepr"] == 0:
                return ArtistPanel(**info, reprs=[])
            return ArtistPanel(**info, reprs=repr_fetch(entity, rowid, repr_limit, db))
        case LabelEntity():
            if info["nrepr"] == 0:
                return LabelPanel(**info, reprs=[])
            return LabelPanel(**info, reprs=repr_fetch(entity, rowid, repr_limit, db))


@router.get("/api/panel")
async def panel(
    rowid: int,
    entity_name: str,
    repr_limit: int = Query(3, ge=1, le=3),
) -> Panel:
    if entity_name not in NAME2ENTITY:
        raise HTTPException(status_code=404, detail="Entity not found")
    entity = NAME2ENTITY[entity_name]
    result = panel_fetch(entity, rowid, repr_limit, get_db())
    if result is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return result
