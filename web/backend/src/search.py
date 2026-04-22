import re
from typing import Literal, TypedDict

from fastapi import APIRouter
from fastapi import Query

from src.shared import get_meili_index
from src.utils import AlbumEntity
from src.utils import ArtistEntity
from src.utils import ENTITIES
from src.utils import Entity
from src.utils import LabelEntity
from src.utils import NAME2ENTITY
from src.utils import TrackEntity

router = APIRouter()

SEARCH_ID_RE = re.compile(rf"^({'|'.join(re.escape(e.name) for e in ENTITIES)})_(\d+)$")


class TrackHit(TypedDict):
    entity_type: Literal["track"]
    rowid: int
    track_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float


class AlbumHit(TypedDict):
    entity_type: Literal["album"]
    rowid: int
    album_name_norm: str
    artist_name: str
    lon: float
    lat: float
    logcount: float


class ArtistHit(TypedDict):
    entity_type: Literal["artist"]
    rowid: int
    artist_name: str
    lon: float
    lat: float
    logcount: float


class LabelHit(TypedDict):
    entity_type: Literal["label"]
    rowid: int
    label: str
    lon: float
    lat: float
    logcount: float


def search_mid2eid(search_id: str) -> tuple[Entity, int]:
    """From meilisearch primary key extracts 2-tuple (Entity, rowid)."""
    m = SEARCH_ID_RE.match(search_id)
    if not m:
        raise ValueError(f"Invalid search id: {search_id!r}")
    return NAME2ENTITY[m.group(1)], int(m.group(2))


def search_map(hit: dict) -> TrackHit | AlbumHit | ArtistHit | LabelHit:
    """Maps a meilisearch doc hit dictionary to a table subset."""
    entity, rowid = search_mid2eid(hit.pop("id"))
    match entity:
        case TrackEntity():
            return TrackHit(
                entity_type="track",
                rowid=rowid,
                track_name_norm=hit["track_name_norm"],
                artist_name=hit["artist_name"],
                lon=hit["lon"],
                lat=hit["lat"],
                logcount=hit["logcount"],
            )
        case AlbumEntity():
            return AlbumHit(
                entity_type="album",
                rowid=rowid,
                album_name_norm=hit["album_name_norm"],
                artist_name=hit["artist_name"],
                lon=hit["lon"],
                lat=hit["lat"],
                logcount=hit["logcount"],
            )
        case ArtistEntity():
            return ArtistHit(
                entity_type="artist",
                rowid=rowid,
                artist_name=hit["artist_name"],
                lon=hit["lon"],
                lat=hit["lat"],
                logcount=hit["logcount"],
            )
        case LabelEntity():
            return LabelHit(
                entity_type="label",
                rowid=rowid,
                label=hit["label"],
                lon=hit["lon"],
                lat=hit["lat"],
                logcount=hit["logcount"],
            )


SearchHit = TrackHit | AlbumHit | ArtistHit | LabelHit


@router.get("/api/search")
async def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
) -> list[SearchHit]:
    """Search for up to `limit` entities matching `q` query."""
    return [search_map(hit) for hit in get_meili_index().search(q, {"limit": limit})["hits"]]
