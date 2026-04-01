"""
Builds "concentric" indexes for tracks to include in db and similarity indexes.
Contract: a track in a similarity index should always be in db index too.
"""
import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd

from src import filters as f
from src.utils import read_manifest, get_index_filter_sim_paths, get_index_filter_db_paths, EntityPaths, EntityTable, EntityIndex, _get_config_int, _get_config_float
from src.utils import ENTITY_KEYS as EKEYS


INDEX_FILTER_DB_LABEL_MIN_NARTIST = _get_config_int("SICK_INDEX_FILTER_DB_LABEL_MIN_NARTIST")
INDEX_FILTER_DB_ARTIST_MIN_NTRACK = _get_config_int("SICK_INDEX_FILTER_DB_ARTIST_MIN_NTRACK")
INDEX_FILTER_DB_ALBUM_MIN_TOTAL_TRACKS = _get_config_int("SICK_INDEX_FILTER_DB_ALBUM_MIN_TOTAL_TRACKS")
INDEX_FILTER_DB_TRACK_MIN_LOGCOUNT = _get_config_float("SICK_INDEX_FILTER_DB_TRACK_MIN_LOGCOUNT")
INDEX_FILTER_TRACK_MIN_LOGCOUNT = _get_config_float("SICK_INDEX_FILTER_SIM_TRACK_MIN_LOGCOUNT")


def save_indexes(indexes: EntityIndex, paths: EntityPaths) -> None:
    for entity, path, index in (
        ("track", paths.track, indexes.track),
        ("album", paths.album, indexes.album),
        ("artist", paths.artist, indexes.artist),
        ("label", paths.label, indexes.label),
    ):
        np.save(path, index)
        print(f"  {entity:<8s} {len(index):>10,} rows  →  {path}")


def lookup2index(lookups: EntityTable) -> EntityIndex:
    return EntityIndex(
        track=lookups.track[EKEYS.track].to_numpy(),
        album=lookups.album[EKEYS.album].to_numpy(),
        artist=lookups.artist[EKEYS.artist].to_numpy(),
        label=lookups.label[EKEYS.label].to_numpy(),
    )


def build_indexes(manifest_path: Path | str):
    print("Reading lookup tables from manifest...")
    lookup_paths = read_manifest(manifest_path)["lookup"]
    lookups = EntityTable(
        track=pd.read_parquet(lookup_paths.track),
        album=pd.read_parquet(lookup_paths.album),
        artist=pd.read_parquet(lookup_paths.artist),
        label=pd.read_parquet(lookup_paths.label),
    )

    print("Building DB filter indexes...")
    lookups = f.filter_cascade(
        lookups.track, lambda df: f.filter_track(df, INDEX_FILTER_DB_TRACK_MIN_LOGCOUNT),
        lookups.album, lambda df: f.filter_album(df, INDEX_FILTER_DB_ALBUM_MIN_TOTAL_TRACKS),
        lookups.artist, lambda df: f.filter_artist(df, INDEX_FILTER_DB_ARTIST_MIN_NTRACK),
        lookups.label, lambda df: f.filter_label(df, INDEX_FILTER_DB_LABEL_MIN_NARTIST),
    )
    indexes_db = lookup2index(lookups)

    print("Building SIM filter indexes...")
    lookups = f.filter_separate(
        lookups.track, lambda df: f.filter_track(df, INDEX_FILTER_TRACK_MIN_LOGCOUNT),
        lookups.album, lambda x: x,
        lookups.artist, lambda x: x,
        lookups.label, lambda x: x,
    )
    indexes_sim = lookup2index(lookups)

    print("Checking SIM ⊆ DB contract...")
    for index_sim, index_db in (
        (indexes_sim.track, indexes_db.track),
        (indexes_sim.album, indexes_db.album),
        (indexes_sim.artist, indexes_db.artist),
        (indexes_sim.label, indexes_db.label),
    ):
        assert np.all(np.isin(index_sim, index_db))

    print("Saving DB filter indexes...")
    save_indexes(indexes_db, get_index_filter_db_paths())
    print("Saving SIM filter indexes...")
    save_indexes(indexes_sim, get_index_filter_sim_paths())
    print()
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build per-entity filter indexes from manifest lookups.",
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

    build_indexes(args.manifest)
