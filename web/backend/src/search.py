import re
from typing import Literal, TypedDict, assert_never

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


def dedup_normalize(value: str) -> str:
    return value.strip().casefold()


def dedup_key(hit: SearchHit) -> tuple[str, str] | tuple[str, str, str]:
    match hit["entity_type"]:
        case "track":
            return (
                "track",
                dedup_normalize(hit["track_name_norm"]),
                dedup_normalize(hit["artist_name"]),
            )
        case "album":
            return (
                "album",
                dedup_normalize(hit["album_name_norm"]),
                dedup_normalize(hit["artist_name"]),
            )
        case "artist":
            return ("artist", dedup_normalize(hit["artist_name"]))
        case "label":
            return ("label", dedup_normalize(hit["label"]))
        case entity_type:
            assert_never(entity_type)


def dedup(hits: list[SearchHit]) -> list[SearchHit]:
    seen = set()
    out: list[SearchHit] = []
    for hit in hits:
        key = dedup_key(hit)
        if key not in seen:
            seen.add(key)
            out.append(hit)
    return out


@router.get("/api/search")
async def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
) -> list[SearchHit]:
    """Search for up to `limit` entities matching `q` query."""
    return dedup(
        [search_map(hit) for hit in get_meili_index().search(q, {"limit": limit})["hits"]]
    )
