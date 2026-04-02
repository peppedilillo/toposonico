from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.build_geomap import read_umaps
from scripts.build_geomap import umap2geo
from src.utils import ENTITY_KEYS as EKEYS
from src.utils import EntityPaths
from src.utils import EntityTable


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


def test_umap2geo_lon_lat_within_declared_range():
    table = _umap_table()
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    for entity in ("track", "artist", "album", "label"):
        df = getattr(result, entity)
        assert df["lon"].between(-10.0, 10.0).all(), f"{entity}: lon out of range"
        assert df["lat"].between(-10.0, 10.0).all(), f"{entity}: lat out of range"


def test_umap2geo_extreme_points_reach_bounds():
    table = _umap_table()
    result = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)

    track = result.track
    assert pytest.approx(track["lon"].min(), abs=1e-4) == -10.0
    assert pytest.approx(track["lon"].max(), abs=1e-4) == +10.0
    assert pytest.approx(track["lat"].min(), abs=1e-4) == -10.0
    assert pytest.approx(track["lat"].max(), abs=1e-4) == +10.0


def test_umap2geo_shared_bbox_keeps_entities_aligned():
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
    table = _umap_table()
    no_pad = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.0)
    padded = umap2geo(table, max_lon=10.0, max_lat=10.0, padding=0.5)

    assert abs(padded.track["lon"].min()) < abs(no_pad.track["lon"].min())
    assert abs(padded.track["lon"].max()) < abs(no_pad.track["lon"].max())


def test_umap2geo_negative_max_lon_flips_x_axis():
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


def test_read_umaps_preserves_umap_values(tmp_path):
    table = _umap_table(track_xy=[(1.0, 2.0), (3.0, 4.0)])
    table.track[EKEYS.track] = [100, 200]
    umaps = _write_umap_parquets(tmp_path, table)

    result = read_umaps(umaps)

    row = result.track.set_index(EKEYS.track).loc[100]
    assert row["umap_x"] == pytest.approx(1.0)
    assert row["umap_y"] == pytest.approx(2.0)
