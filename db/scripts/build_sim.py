"""Build FAISS similarity indexes for all entity types.

Reads recable entity rowids from the DB, loads embedding parquets from the ml
manifest, filters to recable entities, then trains and saves one FAISS index
per entity.

Usage:
    source config.env && uv run python scripts/build_sim.py
    uv run python scripts/build_sim.py --manifest path/to/manifest.toml --db path/to/sick.db
"""

import argparse
import os
import sqlite3
import time

import numpy as np

from src.sim import album_spec
from src.sim import artist_spec
from src.sim import label_spec
from src.sim import load_filtered_embeddings
from src.sim import save_index
from src.sim import track_spec
from src.sim import train_index
from src.utils import ENTITY_KEYS as EKEYS
from src.utils import get_index_faiss_paths
from src.utils import read_manifest


def query_recable_rowids(conn: sqlite3.Connection, table: str, key_col: str) -> np.ndarray:
    """Query recable entity rowids from the DB, returned as a sorted int64 array."""
    rows = conn.execute(f"SELECT {key_col} FROM {table} WHERE recable = 1 ORDER BY {key_col}").fetchall()
    return np.array([r[0] for r in rows], dtype=np.int64)


def build_entity(spec, embedding_path, rowid_col, out_path):
    print(f"  Loading embeddings from {embedding_path}")
    xb = load_filtered_embeddings(embedding_path, spec, rowid_col)
    print(f"  {xb.shape[0]:,} vectors, dim={xb.shape[1]}")
    print(f"  Factory: {spec.factory_string}")

    t = time.time()
    index = train_index(spec, xb)
    elapsed = time.time() - t
    print(f"  Trained + added in {elapsed:.1f}s")

    save_index(index, out_path)
    print(f"  Saved to {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Build FAISS similarity indexes for all entity types.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--manifest",
        default=os.environ.get("SICK_MANIFEST"),
        metavar="PATH",
        help="Path to ml manifest TOML. $SICK_MANIFEST",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("SICK_DB"),
        metavar="PATH",
        help="Path to sick.db. $SICK_DB",
    )
    args = parser.parse_args()

    if args.manifest is None:
        raise ValueError("--manifest / $SICK_MANIFEST not set")
    if args.db is None:
        raise ValueError("--db / $SICK_DB not set")

    manifest = read_manifest(args.manifest)
    emb = manifest["embedding"]
    faiss_paths = get_index_faiss_paths()

    conn = sqlite3.connect(args.db)

    print("track")
    spec = track_spec(query_recable_rowids(conn, "tracks", EKEYS.track))
    build_entity(spec, emb.track, EKEYS.track, faiss_paths.track)

    print("album")
    spec = album_spec(query_recable_rowids(conn, "albums", EKEYS.album))
    build_entity(spec, emb.album, EKEYS.album, faiss_paths.album)

    print("artist")
    spec = artist_spec(query_recable_rowids(conn, "artists", EKEYS.artist))
    build_entity(spec, emb.artist, EKEYS.artist, faiss_paths.artist)

    print("label")
    spec = label_spec(query_recable_rowids(conn, "labels", EKEYS.label))
    build_entity(spec, emb.label, EKEYS.label, faiss_paths.label)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
