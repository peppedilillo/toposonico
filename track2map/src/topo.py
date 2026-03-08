"""Topology utilities: aggregations and derived representations over track embeddings."""

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
