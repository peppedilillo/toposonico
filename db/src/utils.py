import os
from pathlib import Path
import tomllib
from typing import NamedTuple

import pandas as pd

ENTITIES = ("track", "artist", "album", "label")


class EntityPaths(NamedTuple):
    track: Path
    artist: Path
    album: Path
    label: Path


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


MANIFEST_REQUIRED_SECTIONS = ("embedding", "lookup", "umap")
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
    manifest_path: str | Path,
    required_sections: tuple[str, ...] | None = MANIFEST_REQUIRED_SECTIONS,
) -> dict[str, EntityPaths | dict[str, Path]]:
    """Reads and validates a manifest TOML, returning a nested Path dict.

    Entity sections (embedding, lookup, umap) are returned as EntityPaths;
    non-entity sections (source) are returned as plain dicts.
    """
    manifest_path = Path(manifest_path)
    if not manifest_path.is_file():
        raise ValueError("Manifest file not found.")

    entity_keys = {"embedding", "lookup", "umap"}
    with open(manifest_path, "rb") as f:
        m = tomllib.load(f)

    result = {}
    if "source" in m:
        result["source"] = {k: Path(v) for k, v in m["source"].items()}
    for section in entity_keys:
        paths = {k: Path(v) for k, v in m[section].items()}
        result[section] = EntityPaths(**paths)
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
    if "source" in rs:
        all_paths.extend(m["source"].values())
    for section in rs - {"source"}:
        all_paths.extend(m[section])
    missing = [p for p in all_paths if not p.is_file()]
    if missing:
        raise ValueError("Manifest paths not found:\n" + "\n".join(f"  {p}" for p in missing))


def get_geo_paths() -> EntityPaths:
    """Return per-entity geomap parquet paths rooted at $SICK_GEO_DIR."""
    outdir = os.environ.get("SICK_OUT_DIR")
    if outdir is None:
        raise ValueError("$SICK_OUT_DIR not set")
    d = Path(outdir) / "geo"
    d.mkdir(parents=True, exist_ok=True)
    return EntityPaths(**{e: d / f"geo_{e}.parquet" for e in ENTITIES})


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


def get_geojson_paths() -> EntityPaths:
    def _path(var: str) -> Path:
        v = os.environ.get(var)
        if v is None:
            raise ValueError(f"${var} not set")
        p = Path(v)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    return EntityPaths(
        track=_path("SICK_GEOJSON_TRACK"),
        artist=_path("SICK_GEOJSON_ARTIST"),
        album=_path("SICK_GEOJSON_ALBUM"),
        label=_path("SICK_GEOJSON_LABEL"),
    )
