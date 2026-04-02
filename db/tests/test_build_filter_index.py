import importlib.util
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from src import filters as f
from src.utils import ENTITY_KEYS as EKEYS
from src.utils import EntityIndex
from src.utils import EntityPaths
from src.utils import EntityTable

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_filter_index.py"


def _make_lookup_tables() -> EntityTable:
    return EntityTable(
        track=pd.DataFrame(
            {
                EKEYS.track: [101, 102, 103, 104, 105],
                EKEYS.album: [201, 201, 202, 203, 204],
                "id_isrc": ["isrc-101", "isrc-102", "isrc-103", "isrc-104", "isrc-105"],
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


def test_filter_track_keeps_highest_logcount_for_valid_isrc_duplicates():
    tracks = pd.DataFrame(
        {
            EKEYS.track: [101, 102, 103],
            "id_isrc": ["dup", "dup", "unique"],
            "logcount": [1.0, 3.0, 2.0],
        }
    )

    filtered = f.filter_track(tracks, 0.0)

    np.testing.assert_array_equal(filtered[EKEYS.track], np.array([102, 103]))


def test_filter_track_preserves_rows_with_nan_isrc():
    tracks = pd.DataFrame(
        {
            EKEYS.track: [101, 102, 103],
            "id_isrc": [np.nan, np.nan, "valid"],
            "logcount": [1.0, 3.0, 2.0],
        }
    )

    filtered = f.filter_track(tracks, 0.0)

    np.testing.assert_array_equal(filtered[EKEYS.track], np.array([101, 102, 103]))


def test_filter_track_preserves_rows_with_empty_or_whitespace_isrc():
    tracks = pd.DataFrame(
        {
            EKEYS.track: [101, 102, 103],
            "id_isrc": ["", "   ", "valid"],
            "logcount": [1.0, 3.0, 2.0],
        }
    )

    filtered = f.filter_track(tracks, 0.0)

    np.testing.assert_array_equal(filtered[EKEYS.track], np.array([101, 102, 103]))


def test_filter_track_mixed_valid_and_invalid_isrc_behavior():
    tracks = pd.DataFrame(
        {
            EKEYS.track: [101, 102, 103, 104, 105],
            "id_isrc": ["dup", "dup", np.nan, "", "unique"],
            "logcount": [1.0, 4.0, 2.0, 3.0, 5.0],
        }
    )

    filtered = f.filter_track(tracks, 0.0)

    np.testing.assert_array_equal(filtered[EKEYS.track], np.array([102, 103, 104, 105]))
