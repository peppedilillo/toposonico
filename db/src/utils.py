import os
from pathlib import Path
from typing import NamedTuple
import tomllib

import numpy as np
import pandas as pd

ENTITIES = ("track", "artist", "album", "label")


class EntityPaths(NamedTuple):
    track: Path
    artist: Path
    album: Path
    label: Path


class EntityIndex(NamedTuple):
    track: np.ndarray
    artist: np.ndarray
    album: np.ndarray
    label: np.ndarray


class EntityTable(NamedTuple):
    track: pd.DataFrame
    artist: pd.DataFrame
    album: pd.DataFrame
    label: pd.DataFrame


class EntityKey(NamedTuple):
    track: str = "track_rowid"
    artist: str = "artist_rowid"
    album: str = "album_rowid"
    label: str = "label_rowid"


MANIFEST_REQUIRED_SECTIONS = ("source", "embedding", "lookup", "umap")
ENTITY_KEYS = EntityKey()


def _get_config_int(var: str) -> int:
    """Read an integer from an environment variable; raise if unset."""
    n = os.environ.get(var)
    if n is None:
        raise EnvironmentError(f"No {var} environment variable set. Have you run `source config.env`?")
    return int(n)


def _get_config_float(var: str) -> float:
    """Read a float from an environment variable; raise if unset."""
    n = os.environ.get(var)
    if n is None:
        raise EnvironmentError(f"No {var} environment variable set. Have you run `source config.env`?")
    return float(n)


def read_manifest(
    manifest_path: str | Path | None = None,
    required_sections: tuple[str, ...] | None = MANIFEST_REQUIRED_SECTIONS,
) -> dict[str, EntityPaths | dict[str, Path]]:
    """Reads and validates a manifest TOML, returning a nested Path dict.

    Entity sections (embedding, lookup, umap) are returned as EntityPaths;
    non-entity sections (source) are returned as plain dicts.
    """
    if manifest_path is None:
        manifest_path = os.environ.get("SICK_MANIFEST")
    manifest_path = Path(manifest_path)
    if not manifest_path.is_file():
        raise ValueError("Manifest file not found. Set $SICK_MANIFEST or provide path.")

    entity_keys = {"embedding", "lookup", "umap"}
    with open(manifest_path, "rb") as f:
        m = tomllib.load(f)
    result: dict[str, EntityPaths | dict[str, Path]] = {}
    for section in m:
        paths = {k: Path(v) for k, v in m[section].items()}
        if section in entity_keys:
            result[section] = EntityPaths(**paths)
        else:
            result[section] = paths
    check_manifest(result, required_sections=required_sections)
    return result


def check_manifest(
    m: dict[str, EntityPaths | dict[str, Path]],
    required_sections: tuple[str, ...] | None = MANIFEST_REQUIRED_SECTIONS,
) -> None:
    """Raise ValueError if manifest sections, entity keys, or paths are invalid."""
    if required_sections is None:
        required_sections = MANIFEST_REQUIRED_SECTIONS
    rs = set(required_sections)
    missing_sections = rs - set(m)
    if missing_sections:
        raise ValueError(f"Missing manifest sections: {', '.join(sorted(missing_sections))}.")
    entity_sections = rs & {"embedding", "lookup", "umap"}
    for section in entity_sections:
        if not isinstance(m[section], EntityPaths):
            raise ValueError(f"Section `{section}` must have exactly: {', '.join(ENTITIES)}.")
    all_paths = []
    for section in rs:
        val = m[section]
        if isinstance(val, EntityPaths):
            all_paths.extend(val)
        else:
            all_paths.extend(val.values())
    missing = [p for p in all_paths if not p.is_file()]
    if missing:
        raise ValueError(
            "Manifest paths not found:\n" + "\n".join(f"  {p}" for p in missing)
        )


def get_geo_paths() -> EntityPaths:
    """Return per-entity geomap parquet paths rooted at $SICK_GEO_DIR."""
    outdir = os.environ.get("SICK_OUT_DIR")
    if outdir is None:
        raise ValueError("$SICK_OUT_DIR not set")
    d = Path(outdir) / "geo"
    d.mkdir(parents=True, exist_ok=True)
    return EntityPaths(**{e: d / f"geo_{e}.parquet" for e in ENTITIES})


def get_index_filter_db_paths() -> EntityPaths:
    """Return per-entity filter index paths."""
    outdir = os.environ.get("SICK_OUT_DIR")
    if outdir is None:
        raise ValueError("$SICK_OUT_DIR not set")
    d = Path(outdir) / "index" / "filter" / "db"
    d.mkdir(parents=True, exist_ok=True)
    return EntityPaths(**{e: d / f"index_filter_{e}.npy" for e in ENTITIES})


def get_index_filter_sim_paths() -> EntityPaths:
    """Return per-entity filter index paths rooted at $SICK_INDEX_FILTER_DIR."""
    outdir = os.environ.get("SICK_OUT_DIR")
    if outdir is None:
        raise ValueError("$SICK_OUT_DIR not set")
    d = Path(outdir) / "index" / "filter" / "sim"
    d.mkdir(parents=True, exist_ok=True)
    return EntityPaths(**{e: d / f"index_filter_{e}.npy" for e in ENTITIES})


def get_index_faiss_paths() -> EntityPaths:
    def _path(var: str) -> Path:
        v = os.environ.get(var)
        if v is None:
            raise ValueError(f"${var} not set")
        p = Path(v)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    return EntityPaths(
        track=_path("SICK_INDEX_FAISS_TRACK"),
        artist=_path("SICK_INDEX_FAISS_ARTIST"),
        album=_path("SICK_INDEX_FAISS_ALBUM"),
        label=_path("SICK_INDEX_FAISS_LABEL"),
    )
