"""Topology utilities: aggregations and derived representations over track embeddings."""
from typing import Sequence

import pandas as pd


def artist_embeddings(
    emb_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    min_tracks: int = 1,
) -> pd.DataFrame:
    """Mean-pool track embeddings to artist level.

    Returns a DataFrame with columns [artist_rowid, e0, …, e{D-1}], one row per
    artist that has at least `min_tracks` entries in `lookup_df`.
    """
    emb_cols = [c for c in emb_df.columns if c != "track_rowid"]
    df = emb_df.merge(
        lookup_df[["track_rowid", "artist_rowid"]], on="track_rowid", how="inner"
    )
    assert df["artist_rowid"].notna().all(), "Unexpected null artist_rowid after merge"
    df["artist_rowid"] = df["artist_rowid"].astype("int64")
    agg = df.groupby("artist_rowid", sort=False).agg(
        **{c: (c, "mean") for c in emb_cols},
        track_count=("track_rowid", "count"),
    )
    agg = agg[agg["track_count"] >= min_tracks]
    result = agg[emb_cols].reset_index()
    result["artist_rowid"] = result["artist_rowid"].astype("int64")
    for c in emb_cols:
        result[c] = result[c].astype("float32")
    return result


def album_embeddings(
    emb_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    min_tracks: int = 1,
) -> pd.DataFrame:
    """Mean-pool track embeddings to album level.

    Returns a DataFrame with columns [album_rowid, e0, …, e{D-1}], one row per
    album that has at least `min_tracks` entries in `lookup_df`.
    """
    emb_cols = [c for c in emb_df.columns if c != "track_rowid"]
    df = emb_df.merge(
        lookup_df[["track_rowid", "album_rowid"]], on="track_rowid", how="inner"
    )
    assert df["album_rowid"].notna().all(), "Unexpected null album_rowid after merge"
    df["album_rowid"] = df["album_rowid"].astype("int64")
    agg = df.groupby("album_rowid", sort=False).agg(
        **{c: (c, "mean") for c in emb_cols},
        track_count=("track_rowid", "count"),
    )
    agg = agg[agg["track_count"] >= min_tracks]
    result = agg[emb_cols].reset_index()
    result["album_rowid"] = result["album_rowid"].astype("int64")
    for c in emb_cols:
        result[c] = result[c].astype("float32")
    return result


def label_embeddings(
    emb_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    min_tracks: int = 1,
) -> pd.DataFrame:
    """Mean-pool track embeddings to label level.

    Returns a DataFrame with columns [label, e0, …, e{D-1}], one row per
    label that has at least `min_tracks` entries in `lookup_df`.
    """
    emb_cols = [c for c in emb_df.columns if c != "track_rowid"]
    df = emb_df.merge(
        lookup_df[["track_rowid", "label"]], on="track_rowid", how="inner"
    )
    df = df[df["label"].notna() & (df["label"] != "")]
    agg = df.groupby("label", sort=False).agg(
        **{c: (c, "mean") for c in emb_cols},
        track_count=("track_rowid", "count"),
    )
    agg = agg[agg["track_count"] >= min_tracks]
    result = agg[emb_cols].reset_index()
    for c in emb_cols:
        result[c] = result[c].astype("float32")
    return result


def umap2geo(
    umap_frames: Sequence[tuple[pd.DataFrame, str]],
    max_lon: float = 45.0,
    max_lat: float = 45.0,
    padding: float = 0.02,
) -> list[pd.DataFrame]:
    """Map UMAP coordinates to fake lon/lat using a shared bounding box.

    Computes a single global bbox across all input frames (so entity types stay
    spatially aligned), applies fractional padding, then normalises each frame to
    [-max_lon, +max_lon] × [-max_lat, +max_lat].

    Args:
        umap_frames: sequence of (df, key_col) pairs. Each df must have columns
            ``umap_x`` and ``umap_y`` plus the key column.
        max_lon: half-width in degrees for the x axis (default 45).
        max_lat: half-width in degrees for the y axis (default 45).
        padding: fractional padding added to each side of the global bbox (default 0.02).

    Returns:
        List of DataFrames, one per input, with columns [key_col, lon, lat] (float32).
    """
    import numpy as np

    x_min = min(df.umap_x.min() for df, _ in umap_frames)
    x_max = max(df.umap_x.max() for df, _ in umap_frames)
    y_min = min(df.umap_y.min() for df, _ in umap_frames)
    y_max = max(df.umap_y.max() for df, _ in umap_frames)

    x_pad = (x_max - x_min) * padding
    y_pad = (y_max - y_min) * padding
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    results = []
    for df, key_col in umap_frames:
        x_norm = (df.umap_x - x_min) / (x_max - x_min)
        y_norm = (df.umap_y - y_min) / (y_max - y_min)
        results.append(pd.DataFrame({
            key_col: df[key_col],
            "lon": (x_norm * 2 * max_lon - max_lon).round(6).astype(np.float32),
            "lat": (y_norm * 2 * max_lat - max_lat).round(6).astype(np.float32),
        }))
    return results
