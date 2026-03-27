import os
from pathlib import Path
import tomllib


ENTITIES = {"track", "artist", "album", "label"}

manifest = dict[str, dict[str, Path]]

def read_manifest(manifest_path: str | Path) -> manifest:
    """Reads and validates a manifest TOML, returning a nested Path dict."""
    with open(manifest_path, "rb") as f:
        m = tomllib.load(f)
    m = {section: {k: Path(f) for k, f in m[section].items()} for section in m}
    check_manifest(m)
    return m


def check_manifest(m: manifest) -> None:
    """Raise ValueError if manifest sections, entity keys, or paths are invalid."""
    expected_sections = {"source", "embeddings", "lookups", "umap"}
    if set(m) != expected_sections:
        raise ValueError(f"Wrong manifest sections.")
    for section in ("embeddings", "lookups", "umap"):
        if set(m[section]) != ENTITIES:
            raise ValueError(f"Unexpected entities in section `{section}`.")
    missing = [p for s in m for p in m[s].values() if not p.is_file()]
    if missing:
        raise ValueError("Manifest paths not found:\n" + "\n".join(f"  {p}" for p in missing))


def get_auxpaths() -> dict[str, dict[str, Path]]:
    """Return per-entity paths for all auxiliary parquet files, rooted at $SICK_OUT_DIR."""
    outdir = os.environ.get("SICK_OUT_DIR")
    if outdir is None:
        raise ValueError("$SICK_OUT_DIR not set")
    d = Path(outdir)
    geo_dir = d / "geo"
    knn_dir = d / "knn"
    return {
        "geo":        {e: geo_dir / f"geo_{e}.parquet"        for e in ENTITIES},
        "knn":        {e: knn_dir / f"knn_{e}.parquet"        for e in ENTITIES},
        "knn_scores": {e: knn_dir / f"knn_scores_{e}.parquet" for e in ENTITIES},
    }
