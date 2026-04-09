from functools import cache
import os
import sqlite3
from sqlite3 import connect
from typing import NamedTuple

import faiss
import meilisearch

REQUIRED_ENV_VARS = (
    "SICK_DB",
    "SICK_FAISS_TRACK",
    "SICK_FAISS_ALBUM",
    "SICK_FAISS_ARTIST",
    "SICK_FAISS_LABEL",
    "MEILI_URL",
    "MEILI_KEY",
    "MEILI_UID",
)


def get_config_str(var: str) -> str:
    val = os.environ.get(var)
    if val is None:
        raise ValueError(f"Missing required environment variable: {var}")
    return val


def check_config() -> None:
    missing = [v for v in REQUIRED_ENV_VARS if v not in os.environ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


@cache
def get_db() -> sqlite3.Connection:
    DB = get_config_str("SICK_DB")
    db = connect(f"file:{DB}?mode=ro", uri=True, check_same_thread=False)
    db.row_factory = sqlite3.Row
    return db


class FaissIndexes(NamedTuple):
    track: faiss.Index
    album: faiss.Index
    artist: faiss.Index
    label: faiss.Index


def get_faiss_track_index() -> faiss.Index:
    index = faiss.read_index(get_config_str("SICK_FAISS_TRACK"))
    ivf = faiss.extract_index_ivf(index)
    quantizer = faiss.downcast_index(ivf.quantizer)
    quantizer.hnsw.efSearch = 64
    return index


@cache
def get_faiss_indexes() -> FaissIndexes:
    return FaissIndexes(
        track=get_faiss_track_index(),
        album=faiss.read_index(get_config_str("SICK_FAISS_ALBUM")),
        artist=faiss.read_index(get_config_str("SICK_FAISS_ARTIST")),
        label=faiss.read_index(get_config_str("SICK_FAISS_LABEL")),
    )


@cache
def get_meili_index() -> meilisearch.index.Index:
    url = get_config_str("MEILI_URL")
    key = get_config_str("MEILI_KEY")
    uid = get_config_str("MEILI_UID")
    client = meilisearch.Client(url, key)
    return client.index(uid)
