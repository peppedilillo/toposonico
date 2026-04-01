import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from src import filters as f
from src.utils import ENTITY_KEYS as EKEYS, EntityIndex, EntityPaths, EntityTable


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_filter_index.py"


def _make_lookup_tables() -> EntityTable:
    return EntityTable(
        track=pd.DataFrame(
            {
                EKEYS.track: [101, 102, 103, 104, 105],
                EKEYS.album: [201, 201, 202, 203, 204],
                "logcount": [1.2, 2.5, 1.5, 1.3, 0.5],
            }
        ),
        album=pd.DataFrame(
            {
                EKEYS.album: [201, 202, 203, 204],
                EKEYS.artist: [301, 302, 303, 304],
                EKEYS.label: [401, 401, 402, 403],
                "total_tracks": [3, 1, 4, 5],
            }
        ),
        artist=pd.DataFrame(
            {
                EKEYS.artist: [301, 302, 303, 304],
                "ntrack": [5, 3, 1, 4],
            }
        ),
        label=pd.DataFrame(
            {
                EKEYS.label: [401, 402, 403],
                "nartist": [2, 1, 1],
            }
        ),
    )


def _write_manifest(tmp_path: Path, lookups: EntityTable) -> Path:
    lookup_dir = tmp_path / "lookup"
    lookup_dir.mkdir()
    for entity_name in ("track", "album", "artist", "label"):
        getattr(lookups, entity_name).to_parquet(lookup_dir / f"{entity_name}.parquet")

    placeholder_dir = tmp_path / "placeholders"
    placeholder_dir.mkdir()
    placeholder_paths = {}
    for stem in (
        "source_tracks",
        "embedding_track",
        "embedding_artist",
        "embedding_album",
        "embedding_label",
        "umap_track",
        "umap_artist",
        "umap_album",
        "umap_label",
    ):
        path = placeholder_dir / f"{stem}.bin"
        path.write_text("placeholder", encoding="utf-8")
        placeholder_paths[stem] = path

    manifest_path = tmp_path / "manifest.toml"
    manifest_path.write_text(
        "\n".join(
            (
                "[source]",
                f'tracks = "{placeholder_paths["source_tracks"]}"',
                "",
                "[embedding]",
                f'track = "{placeholder_paths["embedding_track"]}"',
                f'artist = "{placeholder_paths["embedding_artist"]}"',
                f'album = "{placeholder_paths["embedding_album"]}"',
                f'label = "{placeholder_paths["embedding_label"]}"',
                "",
                "[lookup]",
                f'track = "{lookup_dir / "track.parquet"}"',
                f'artist = "{lookup_dir / "artist.parquet"}"',
                f'album = "{lookup_dir / "album.parquet"}"',
                f'label = "{lookup_dir / "label.parquet"}"',
                "",
                "[umap]",
                f'track = "{placeholder_paths["umap_track"]}"',
                f'artist = "{placeholder_paths["umap_artist"]}"',
                f'album = "{placeholder_paths["umap_album"]}"',
                f'label = "{placeholder_paths["umap_label"]}"',
                "",
            )
        ),
        encoding="utf-8",
    )
    return manifest_path


def _load_build_filter_index_module(monkeypatch, tmp_path: Path):
    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.setenv("SICK_INDEX_FILTER_DB_LABEL_MIN_NARTIST", "2")
    monkeypatch.setenv("SICK_INDEX_FILTER_DB_ARTIST_MIN_NTRACK", "2")
    monkeypatch.setenv("SICK_INDEX_FILTER_DB_ALBUM_MIN_TOTAL_TRACKS", "2")
    monkeypatch.setenv("SICK_INDEX_FILTER_DB_TRACK_MIN_LOGCOUNT", "1.0")
    monkeypatch.setenv("SICK_INDEX_FILTER_DB_TRACK_MIN_LOGCOUNT", "2.0")
    monkeypatch.setenv("SICK_INDEX_FILTER_SIM_TRACK_MIN_LOGCOUNT", str(tmp_path / "filter"))

    module_name = "tests_build_filter_index_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_indexes_writes_expected_db_and_sim_indexes(tmp_path, monkeypatch):
    module = _load_build_filter_index_module(monkeypatch, tmp_path)
    lookups = _make_lookup_tables()
    manifest_path = _write_manifest(tmp_path, lookups)

    module.build_indexes(manifest_path)

    db_dir = tmp_path / "filter" / "db"
    sim_dir = tmp_path / "filter" / "sim"

    expected_db = {
        "track": np.array([101, 102, 103, 104]),
        "album": np.array([201, 203]),
        "artist": np.array([301]),
        "label": np.array([401]),
    }
    expected_sim = {
        "track": np.array([102]),
        "album": np.array([201, 203]),
        "artist": np.array([301]),
        "label": np.array([401]),
    }

    for entity_name, expected in expected_db.items():
        db_path = db_dir / f"index_filter_{entity_name}.npy"
        sim_path = sim_dir / f"index_filter_{entity_name}.npy"
        assert db_path.is_file()
        assert sim_path.is_file()
        np.testing.assert_array_equal(np.load(db_path), expected)
        np.testing.assert_array_equal(np.load(sim_path), expected_sim[entity_name])
        assert np.all(np.isin(expected_sim[entity_name], expected))


def test_lookup2index_preserves_rowid_columns(tmp_path, monkeypatch):
    module = _load_build_filter_index_module(monkeypatch, tmp_path)
    lookups = _make_lookup_tables()

    indexes = module.lookup2index(lookups)

    np.testing.assert_array_equal(indexes.track, np.array([101, 102, 103, 104, 105]))
    np.testing.assert_array_equal(indexes.album, np.array([201, 202, 203, 204]))
    np.testing.assert_array_equal(indexes.artist, np.array([301, 302, 303, 304]))
    np.testing.assert_array_equal(indexes.label, np.array([401, 402, 403]))


def test_save_indexes_writes_each_entity_file(tmp_path, monkeypatch):
    module = _load_build_filter_index_module(monkeypatch, tmp_path)
    indexes = EntityIndex(
        track=np.array([1, 2]),
        artist=np.array([3]),
        album=np.array([4, 5]),
        label=np.array([6]),
    )
    paths = EntityPaths(
        track=tmp_path / "track.npy",
        artist=tmp_path / "artist.npy",
        album=tmp_path / "album.npy",
        label=tmp_path / "label.npy",
    )

    module.save_indexes(indexes, paths)

    np.testing.assert_array_equal(np.load(paths.track), np.array([1, 2]))
    np.testing.assert_array_equal(np.load(paths.artist), np.array([3]))
    np.testing.assert_array_equal(np.load(paths.album), np.array([4, 5]))
    np.testing.assert_array_equal(np.load(paths.label), np.array([6]))


def test_filter_cascade_prunes_downstream_entities_from_track_selection():
    lookups = _make_lookup_tables()

    filtered = f.filter_cascade(
        lookups.track,
        lambda df: f.filter_track(df, 1.0),
        lookups.album,
        lambda df: f.filter_album(df, 2),
        lookups.artist,
        lambda df: f.filter_artist(df, 2),
        lookups.label,
        lambda df: f.filter_label(df, 2),
    )

    np.testing.assert_array_equal(filtered.track[EKEYS.track], np.array([101, 102, 103, 104]))
    np.testing.assert_array_equal(filtered.album[EKEYS.album], np.array([201, 203]))
    np.testing.assert_array_equal(filtered.artist[EKEYS.artist], np.array([301]))
    np.testing.assert_array_equal(filtered.label[EKEYS.label], np.array([401]))


def test_filter_separate_only_filters_requested_level():
    lookups = _make_lookup_tables()

    filtered = f.filter_separate(
        lookups.track,
        lambda df: f.filter_track(df, 2.0),
        lookups.album,
        lambda df: df,
        lookups.artist,
        lambda df: df,
        lookups.label,
        lambda df: df,
    )

    np.testing.assert_array_equal(filtered.track[EKEYS.track], np.array([102]))
    pd.testing.assert_frame_equal(filtered.album, lookups.album)
    pd.testing.assert_frame_equal(filtered.artist, lookups.artist)
    pd.testing.assert_frame_equal(filtered.label, lookups.label)
