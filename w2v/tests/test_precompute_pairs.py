import numpy as np
import pandas as pd

from src.data import precompute_pairs


def _pairs(track_ids: list[int], w: int) -> list[tuple[int, int]]:
    """Wrap a single playlist into the DataFrame format expected by precompute_pairs
    and return pairs as a plain list of (center, context) tuples."""
    df = pd.DataFrame({
        "playlist_rowid": np.zeros(len(track_ids), dtype=np.int32),
        "track_id": np.array(track_ids, dtype=np.int64),
    })
    t = precompute_pairs(df, w)
    return list(zip(t[0].tolist(), t[1].tolist()))


def test_window_1_length_3():
    assert _pairs([1, 2, 3], 1) == [(1, 3), (2, 1), (3, 2), (1, 2), (2, 3), (3, 1)]


def test_window_1_length_4():
    assert _pairs([1, 2, 3, 4], 1) == [(1, 4), (2, 1), (3, 2), (4, 3), (1, 2), (2, 3), (3, 4), (4, 1)]


def test_window_2_length_5():
    assert _pairs([1, 2, 3, 4, 5], 2) == [
        (1, 4), (2, 5), (3, 1), (4, 2), (5, 3),
        (1, 5), (2, 1), (3, 2), (4, 3), (5, 4),
        (1, 2), (2, 3), (3, 4), (4, 5), (5, 1),
        (1, 3), (2, 4), (3, 5), (4, 1), (5, 2),
    ]


def test_window_larger_than_playlist():
    """Window larger than playlist length should behave like w=1 for a 2-track playlist."""
    assert _pairs([1, 2], 5) == [(1, 2), (2, 1)]


def test_window_capped_at_playlist_length():
    """w=100 on a 4-track playlist should produce the same pairs as w=2."""
    assert _pairs([1, 2, 3, 4], 100) == _pairs([1, 2, 3, 4], 2)


def test_single_track_playlist_produces_no_pairs():
    assert _pairs([1], 1) == []


def test_multi_playlist_independence():
    """Pairs from two playlists in the same DataFrame must equal the union of
    each playlist's pairs — no pair should cross playlist boundaries."""
    df = pd.DataFrame({
        "playlist_rowid": np.array([0, 0, 0, 1, 1, 1], dtype=np.int32),
        "track_id": np.array([1, 2, 3, 4, 5, 6], dtype=np.int64),
    })
    t = precompute_pairs(df, w=1)
    combined = set(zip(t[0].tolist(), t[1].tolist()))

    expected = set(_pairs([1, 2, 3], 1)) | set(_pairs([4, 5, 6], 1))
    assert combined == expected
