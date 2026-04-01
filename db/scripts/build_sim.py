"""Build FAISS similarity indexes for all entity types.

Reads embedding parquets from the ml manifest, filters them using pre-built
SIM filter indexes (.npy), then trains and saves one FAISS index per entity.

Usage:
    source config.env && uv run python scripts/build_sim.py
    uv run python scripts/build_sim.py --manifest path/to/manifest.toml
    uv run python scripts/build_sim.py --overwrite
"""
import argparse
import os
import time

import numpy as np

from src.sim import (
    track_spec, album_spec, artist_spec, label_spec,
    load_filtered_embeddings, train_index, save_index,
)
from src.utils import (
    read_manifest, get_index_filter_sim_paths, get_index_faiss_paths,
    ENTITY_KEYS as EKEYS,
)


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
    args = parser.parse_args()

    if args.manifest is None:
        raise ValueError("--manifest / $SICK_MANIFEST not set")

    manifest = read_manifest(args.manifest)
    emb = manifest["embedding"]
    filter_paths = get_index_filter_sim_paths()
    faiss_paths = get_index_faiss_paths()

    print("track")
    spec = track_spec(np.load(filter_paths.track))
    build_entity(spec, emb.track, EKEYS.track, faiss_paths.track)

    print("album")
    spec = album_spec(np.load(filter_paths.album))
    build_entity(spec, emb.album, EKEYS.album, faiss_paths.album)

    print("artist")
    spec = artist_spec(np.load(filter_paths.artist))
    build_entity(spec, emb.artist, EKEYS.artist, faiss_paths.artist)

    print("label")
    spec = label_spec(np.load(filter_paths.label))
    build_entity(spec, emb.label, EKEYS.label, faiss_paths.label)

    print("Done.")


if __name__ == "__main__":
    main()
