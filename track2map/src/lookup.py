import pandas as pd


def artist_lookup(df: pd.DataFrame, mintracks=20) -> pd.DataFrame:
    """Aggregate track_lookup rows to one row per artist.

    df must contain a logcounts (float32) column (from track_lookup.parquet).

    Returns columns: artist_rowid (int64), artist_name, track_count (int32),
    logcounts (float32) — mean of per-track logcounts across the artist's tracks.
    """
    df = df[df.groupby("artist_rowid")["artist_rowid"].transform("count") > mintracks]
    out = (
        df.groupby(["artist_rowid", "artist_name"], as_index=False)
        .agg(track_count=("track_rowid", "count"), logcounts=("logcounts", "mean"))
    )
    out["artist_rowid"] = out["artist_rowid"].astype("int64")
    out["track_count"] = out["track_count"].astype("int32")
    out["logcounts"] = out["logcounts"].astype("float32")
    return out


def album_lookup(df: pd.DataFrame, mintracks: int=5) -> pd.DataFrame:
    """Aggregate track_lookup rows to one row per album, with primary artist.

    df must contain a logcounts (float32) column (from track_lookup.parquet).

    Returns columns: album_rowid (int64), album_name, artist_rowid (int64),
    artist_name, track_count (int32), logcounts (float32) — mean of per-track
    logcounts across the album's tracks.
    """
    df = df[df.groupby("album_rowid")["album_rowid"].transform("count") > mintracks]
    out = (
        df.groupby(["album_rowid", "album_name"], as_index=False)
        .agg(track_count=("track_rowid", "count"), logcounts=("logcounts", "mean"))
    )
    primary_artist = (
        df.groupby("album_rowid")[["artist_rowid", "artist_name"]]
        .first()
        .reset_index()
    )
    out = out.merge(primary_artist, on="album_rowid", how="left")
    out["album_rowid"] = out["album_rowid"].astype("int64")
    out["track_count"] = out["track_count"].astype("int32")
    out["logcounts"] = out["logcounts"].astype("float32")
    return out


def label_lookup(df: pd.DataFrame, mintracks=100) -> pd.DataFrame:
    """Aggregate track_lookup rows to one row per label (excludes null/empty).

    df must contain a logcounts (float32) column (from track_lookup.parquet).

    Returns columns: label, track_count (int32), logcounts (float32) — mean of
    per-track logcounts across the label's tracks.
    """
    df = df[df["label"].notna() & (df["label"] != "")]
    df = df[df.groupby("label")["label"].transform("count") > mintracks]
    out = (
        df.groupby("label", as_index=False)
        .agg(track_count=("track_rowid", "count"), logcounts=("logcounts", "mean"))
    )
    out["track_count"] = out["track_count"].astype("int32")
    out["logcounts"] = out["logcounts"].astype("float32")
    return out
