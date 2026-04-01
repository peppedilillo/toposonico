from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.build_geomap import read_and_filter, umap2geo
from src.utils import ENTITY_KEYS as EKEYS
from src.utils import EntityPaths, EntityTable


def _umap_table(
    track_xy: list[tuple[float, float]] | None = None,
    artist_xy: list[tuple[float, float]] | None = None,
    album_xy: list[tuple[float, float]] | None = None,
    label_xy: list[tuple[float, float]] | None = None,
) -> EntityTable:
    """Build a minimal EntityTable with known UMAP coordinates.

    Defaults place one point per entity at the four corners of the unit square,
    so the global bbox is [0, 1] × [0, 1] with no padding.
    """
    def frame(key_col, rowids, xys):
        xs, ys = zip(*xys)
        return pd.DataFrame({key_col: rowids, "umap_x": list(xs), "umap_y": list(ys)})

    track_xy = track_xy or [(0.0, 0.0), (1.0, 1.0)]
    artist_xy = artist_xy or [(0.5, 0.5)]
    album_xy = album_xy or [(0.25, 0.75)]
    label_xy = label_xy or [(0.75, 0.25)]

    return EntityTable(
        track=frame(EKEYS.track, list(range(len(track_xy))), track_xy),
        artist=frame(EKEYS.artist, list(range(len(artist_xy))), artist_xy),
        album=frame(EKEYS.album, list(range(len(album_xy))), album_xy),
        label=frame(EKEYS.label, list(range(len(label_xy))), label_xy),
    )


def _write_umap_parquets(tmp_path: Path, table: EntityTable) -> EntityPaths:
    paths = {}
    for entity in ("track", "artist", "album", "label"):
        p = tmp_path / f"umap_{entity}.parquet"
        getattr(table, entity).to_parquet(p, index=False)
        paths[entity] = p
    return EntityPaths(**paths)


def _write_filter_indices(tmp_path: Path, table: EntityTable) -> EntityPaths:
    """Write .npy filter arrays containing all rowids from each entity frame."""
    paths = {}
    for entity, key_col in (
        ("track", EKEYS.track),
        ("artist", EKEYS.artist),
        ("album", EKEYS.album),
        ("label", EKEYS.label),
    ):
        p = tmp_path / f"filter_{entity}.npy"
        rowids = getattr(table, entity)[key_col].to_numpy(dtype=np.int64)
        np.save(p, rowids)
        paths[entity] = p
    return EntityPaths(**paths)


def test_umap2geo_lon_lat_within_declared_range():
    table = _umap_table()
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    for entity in ("track", "artist", "album", "label"):
        df = getattr(result, entity)
        assert df["lon"].between(-10.0, 10.0).all(), f"{entity}: lon out of range"
        assert df["lat"].between(-10.0, 10.0).all(), f"{entity}: lat out of range"


def test_umap2geo_extreme_points_reach_bounds():
    # track has the global x/y extremes (0 and 1); with no padding they should
    # land exactly at ±max
    table = _umap_table()
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    track = result.track
    assert pytest.approx(track["lon"].min(), abs=1e-4) == -10.0
    assert pytest.approx(track["lon"].max(), abs=1e-4) == +10.0
    assert pytest.approx(track["lat"].min(), abs=1e-4) == -10.0
    assert pytest.approx(track["lat"].max(), abs=1e-4) == +10.0


def test_umap2geo_shared_bbox_keeps_entities_aligned():
    # track lives in [0, 1]; artist lives in [5, 6] — without shared bbox they
    # would fill the same lon/lat range.  With a shared bbox artist is in the
    # right half of the map and track is in the left half.
    table = _umap_table(
        track_xy=[(0.0, 0.5), (1.0, 0.5)],
        artist_xy=[(5.0, 0.5), (6.0, 0.5)],
        album_xy=[(3.0, 0.5)],
        label_xy=[(3.0, 0.5)],
    )
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    track_lon_max = result.track["lon"].max()
    artist_lon_min = result.artist["lon"].min()
    assert artist_lon_min > track_lon_max, "artist should be east of track in shared space"


def test_umap2geo_padding_moves_extremes_inward():
    # Without padding, the extreme track points hit ±max exactly.
    # With padding > 0 the bbox grows, so those same points map to interior values.
    table = _umap_table()
    no_pad = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)
    padded = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.5)

    assert abs(padded.track["lon"].min()) < abs(no_pad.track["lon"].min())
    assert abs(padded.track["lon"].max()) < abs(no_pad.track["lon"].max())


def test_umap2geo_negative_max_lon_flips_x_axis():
    # Two track points at x=0 and x=1.  Positive max_lon → left-to-right order.
    # Negative max_lon → reversed order (same as main() passes -hwidth).
    table = _umap_table(track_xy=[(0.0, 0.5), (1.0, 0.5)])
    pos = umap2geo(table, max_lon=+10.0, max_lat=10.0, padding=0.0)
    neg = umap2geo(table, max_lon=-10.0, max_lat=10.0, padding=0.0)

    lon_pos = pos.track["lon"].tolist()
    lon_neg = neg.track["lon"].tolist()

    assert lon_pos[0] < lon_pos[1], "positive max_lon: x=0 should have smaller lon"
    assert lon_neg[0] > lon_neg[1], "negative max_lon: x=0 should have larger lon (flipped)"


def test_umap2geo_output_has_correct_columns_and_dtype():
    table = _umap_table()
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    for entity, key_col in (
        ("track", EKEYS.track),
        ("artist", EKEYS.artist),
        ("album", EKEYS.album),
        ("label", EKEYS.label),
    ):
        df = getattr(result, entity)
        assert list(df.columns) == [key_col, "lon", "lat"]
        assert df["lon"].dtype == np.float32
        assert df["lat"].dtype == np.float32


def test_umap2geo_preserves_key_column_values():
    table = _umap_table()
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    np.testing.assert_array_equal(result.track[EKEYS.track], table.track[EKEYS.track])
    np.testing.assert_array_equal(result.artist[EKEYS.artist], table.artist[EKEYS.artist])


def test_read_and_filter_restricts_rows_to_filter_index(tmp_path):
    table = _umap_table(track_xy=[(float(i), float(i)) for i in range(5)])
    # Manually override track rowids so we can filter a specific subset
    table.track[EKEYS.track] = [10, 20, 30, 40, 50]
    umaps = _write_umap_parquets(tmp_path, table)

    # Filter: keep only track rowids 20 and 40
    filter_dir = tmp_path / "filters"
    filter_dir.mkdir()
    filter_track = filter_dir / "filter_track.npy"
    np.save(filter_track, np.array([20, 40], dtype=np.int64))

    # Other entities: keep all
    full_filters = _write_filter_indices(tmp_path, table)
    filters = EntityPaths(
        track=filter_track,
        artist=full_filters.artist,
        album=full_filters.album,
        label=full_filters.label,
    )

    result = read_and_filter(umaps, filters)

    assert len(result.track) == 2
    np.testing.assert_array_equal(sorted(result.track[EKEYS.track]), [20, 40])


def test_read_and_filter_preserves_umap_values(tmp_path):
    table = _umap_table(track_xy=[(1.0, 2.0), (3.0, 4.0)])
    table.track[EKEYS.track] = [100, 200]
    umaps = _write_umap_parquets(tmp_path, table)
    filters = _write_filter_indices(tmp_path, table)

    result = read_and_filter(umaps, filters)

    row = result.track.set_index(EKEYS.track).loc[100]
    assert row["umap_x"] == pytest.approx(1.0)
    assert row["umap_y"] == pytest.approx(2.0)
