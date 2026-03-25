import os

import pandas as pd


def _get_config_parameter(var: str) -> int:
    n = os.environ.get(var)
    if n is None:
        raise EnvironmentError(
            f"No {var} environment variable set. " f"Have you run `source config.env`?"
        )
    return int(n)


class Artists:
    MINTRACKS = _get_config_parameter("T2M_ARTIST_MINTRACK")

    @staticmethod
    def lookup(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate track_lookup rows to one row per artist.

        df must contain a logcounts (float32) column (from track_lookup.parquet).

        Returns columns: artist_rowid (int64), artist_name, logcounts (float32)
        mean of per-track logcounts across the artist's tracks.
        """

        df = df[
            df.groupby("artist_rowid")["artist_rowid"].transform("count")
            > Artists.MINTRACKS
        ]
        out = df.groupby(["artist_rowid", "artist_name"], as_index=False).agg(
            logcounts=("logcounts", "mean")
        )
        out["artist_rowid"] = out["artist_rowid"].astype("int64")
        out["logcounts"] = out["logcounts"].astype("float32")
        return out

    @staticmethod
    def embeddings(
        emb_df: pd.DataFrame,
        lookup_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Mean-pool track embeddings to artist level.

        Returns a DataFrame with columns [artist_rowid, e0, …, e{D-1}], one row per
        artist with at least Artists.MINTRACKS tracks in `lookup_df`.
        """
        emb_cols = [c for c in emb_df.columns if c != "track_rowid"]
        df = emb_df.merge(
            lookup_df[["track_rowid", "artist_rowid"]], on="track_rowid", how="inner"
        )
        assert (
            df["artist_rowid"].notna().all()
        ), "Unexpected null artist_rowid after merge"
        df["artist_rowid"] = df["artist_rowid"].astype("int64")
        agg = df.groupby("artist_rowid", sort=False).agg(
            **{c: (c, "mean") for c in emb_cols},
            track_count=("track_rowid", "count"),
        )
        agg = agg[agg["track_count"] >= Artists.MINTRACKS]
        result = agg[emb_cols].reset_index()
        result["artist_rowid"] = result["artist_rowid"].astype("int64")
        for c in emb_cols:
            result[c] = result[c].astype("float32")
        return result


class Albums:
    MINTRACKS = _get_config_parameter("T2M_ALBUM_MINTRACK")

    @staticmethod
    def lookup(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate track_lookup rows to one row per album, with primary artist.

        df must contain a logcounts (float32) column (from track_lookup.parquet).

        Returns columns: album_rowid (int64), album_name, artist_rowid (int64),
        artist_name, logcounts (float32) — mean of per-track
        logcounts across the album's tracks.
        """
        df = df[
            df.groupby("album_rowid")["album_rowid"].transform("count")
            > Albums.MINTRACKS
        ]
        out = df.groupby(["album_rowid", "album_name"], as_index=False).agg(
            logcounts=("logcounts", "mean")
        )
        primary_artist = (
            df.groupby("album_rowid")[["artist_rowid", "artist_name"]]
            .first()
            .reset_index()
        )
        out = out.merge(primary_artist, on="album_rowid", how="left")
        out["album_rowid"] = out["album_rowid"].astype("int64")
        out["logcounts"] = out["logcounts"].astype("float32")
        return out

    @staticmethod
    def embeddings(
        emb_df: pd.DataFrame,
        lookup_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Mean-pool track embeddings to album level.

        Returns a DataFrame with columns [album_rowid, e0, …, e{D-1}], one row per
        album with at least Albums.MINTRACKS tracks in `lookup_df`.
        """
        emb_cols = [c for c in emb_df.columns if c != "track_rowid"]
        df = emb_df.merge(
            lookup_df[["track_rowid", "album_rowid"]], on="track_rowid", how="inner"
        )
        assert (
            df["album_rowid"].notna().all()
        ), "Unexpected null album_rowid after merge"
        df["album_rowid"] = df["album_rowid"].astype("int64")
        agg = df.groupby("album_rowid", sort=False).agg(
            **{c: (c, "mean") for c in emb_cols},
            track_count=("track_rowid", "count"),
        )
        agg = agg[agg["track_count"] >= Albums.MINTRACKS]
        result = agg[emb_cols].reset_index()
        result["album_rowid"] = result["album_rowid"].astype("int64")
        for c in emb_cols:
            result[c] = result[c].astype("float32")
        return result


class Labels:
    MINTRACKS = _get_config_parameter("T2M_LABEL_MINTRACK")

    @staticmethod
    def lookup(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate track_lookup rows to one row per label (excludes null/empty).

        df must contain a logcounts (float32) column (from track_lookup.parquet).

        Returns columns: label_rowid (int32), label, logcounts (float32).
        label_rowid is a stable sequential int assigned in alphabetical label order.
        """
        df = df[df["label"].notna() & (df["label"] != "")]
        df = df[df.groupby("label")["label"].transform("count") > Labels.MINTRACKS]
        out = df.groupby("label", as_index=False).agg(logcounts=("logcounts", "mean"))
        out["logcounts"] = out["logcounts"].astype("float32")
        out.insert(0, "label_rowid", pd.RangeIndex(len(out), dtype="int32"))
        return out

    @staticmethod
    def embeddings(
        emb_df: pd.DataFrame,
        lookup_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Mean-pool track embeddings to label level.

        Returns a DataFrame with columns [label, e0, …, e{D-1}], one row per
        label with at least Labels.MINTRACKS tracks in `lookup_df`.
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
        agg = agg[agg["track_count"] >= Labels.MINTRACKS]
        result = agg[emb_cols].reset_index()
        for c in emb_cols:
            result[c] = result[c].astype("float32")
        return result
