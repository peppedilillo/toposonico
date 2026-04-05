import os
import re
import sqlite3
from typing import TypedDict

import meilisearch
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.utils import ENTITIES, Entity, KEY2ENTITY, TrackEntity, AlbumEntity, ArtistEntity, LabelEntity

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
)


def get_config_str(var: str) -> str:
    var = os.environ.get(var)
    if var is None:
        raise ValueError("")
    return var


MEILI_URL = get_config_str("MEILI_URL")
MEILI_UID = get_config_str("MEILI_UID")
MEILI_KEY = get_config_str("MEILI_KEY")
SICK_DB = get_config_str("SICK_DB")

meili_client = meilisearch.Client(MEILI_URL, MEILI_KEY)
meili_index = meili_client.index(MEILI_UID)
sick_db = sqlite3.connect(f"file:{SICK_DB}?mode=ro", uri=True, check_same_thread=False)


SEARCH_ID_RE = re.compile(
    rf"^({'|'.join(re.escape(e.key) for e in ENTITIES)})_(\d+)$"
)


class TrackHit(TypedDict):
    entity_type: str
    track_rowid: int
    track_name: str
    artist_name: str
    logcount: float


class AlbumHit(TypedDict):
    entity_type: str
    album_rowid: int
    album_name: str
    artist_name: str
    logcount: float


class ArtistHit(TypedDict):
    entity_type: str
    artist_rowid: int
    artist_name: str
    logcount: float


class LabelHit(TypedDict):
    entity_type: str
    label_rowid: int
    label: str
    logcount: float


def search_mid2eid(search_id: str) -> tuple[Entity, int]:
    """From meilisearch primary key extracts 2-tuple (Entity, rowid)."""
    m = SEARCH_ID_RE.match(search_id)
    if not m:
        raise ValueError(f"Invalid search id: {search_id!r}")
    return KEY2ENTITY[m.group(1)], int(m.group(2))


def search_map(hit: dict) -> TrackHit | AlbumHit | ArtistHit | LabelHit:
    """Maps a meilisearch doc hit dictionary to a table subset."""
    entity, rowid = search_mid2eid(hit.pop("id"))
    match entity:
        case TrackEntity():
            return TrackHit(
                entity_type=entity.name,
                track_rowid=rowid,
                track_name=hit["track_name"],
                artist_name=hit["artist_name"],
                logcount=hit["logcount"],
            )
        case AlbumEntity():
            return AlbumHit(
                entity_type=entity.name,
                album_rowid=rowid,
                album_name=hit["album_name"],
                artist_name=hit["artist_name"],
                logcount=hit["logcount"],
            )
        case ArtistEntity():
            return ArtistHit(
                entity_type=entity.name,
                artist_rowid=rowid,
                artist_name=hit["artist_name"],
                logcount=hit["logcount"],
            )
        case LabelEntity():
            return LabelHit(
                entity_type=entity.name,
                label_rowid=rowid,
                label=hit["label"],
                logcount=hit["logcount"],
            )


@app.get("/api/search")
async def search(
        q: str = Query(..., min_length=1),
        limit: int = Query(10, ge=1, le=20),
) -> list[TrackHit | AlbumHit | ArtistHit | LabelHit]:
    """Search for up to `limit` entities matching `q` query."""
    return [search_map(hit) for hit in meili_index.search(q, {"limit": limit})["hits"]]
