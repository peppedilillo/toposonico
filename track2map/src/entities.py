import pandas as pd


class Artists:
    @staticmethod
    def lookup(df: pd.DataFrame, mintracks: int = 10) -> pd.DataFrame:
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

    @staticmethod
    def embeddings(
            emb_df: pd.DataFrame,
            lookup_df: pd.DataFrame,
            min_tracks: int = 10,
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


class Albums:
    @staticmethod
    def lookup(df: pd.DataFrame, mintracks: int = 5) -> pd.DataFrame:
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

    @staticmethod
    def embeddings(
            emb_df: pd.DataFrame,
            lookup_df: pd.DataFrame,
            min_tracks: int = 5,
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


class Labels:
    @staticmethod
    def lookup(df: pd.DataFrame, mintracks: int = 100) -> pd.DataFrame:
        """Aggregate track_lookup rows to one row per label (excludes null/empty).

        df must contain a logcounts (float32) column (from track_lookup.parquet).

        Returns columns: label_rowid (int32), label, track_count (int32), logcounts (float32).
        label_rowid is a stable sequential int assigned in alphabetical label order.
        """
        df = df[df["label"].notna() & (df["label"] != "")]
        df = df[df.groupby("label")["label"].transform("count") > mintracks]
        out = (
            df.groupby("label", as_index=False)
            .agg(track_count=("track_rowid", "count"), logcounts=("logcounts", "mean"))
        )
        out["track_count"] = out["track_count"].astype("int32")
        out["logcounts"] = out["logcounts"].astype("float32")
        out.insert(0, "label_rowid", pd.RangeIndex(len(out), dtype="int32"))
        return out

    @staticmethod
    def embeddings(
            emb_df: pd.DataFrame,
            lookup_df: pd.DataFrame,
            min_tracks: int = 100,
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