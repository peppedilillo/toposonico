"""Entity helpers for extracting and aggregating embeddings from a model checkpoint.

All classes (Tracks, Artists, Albums, Labels) share the same interface:
  - valid_ids(t1_df, model_dict)  — track rowids that qualify for this entity type
  - lookup(t1_df, model_dict)     — entity rowid + logcounts
  - embeddings(t1_df, model_dict) — entity rowid + e0..e{D-1} (mean-pooled for non-track entities)
"""

import os

import numpy as np
import pandas as pd


def _get_config_int_parameter(var: str) -> int:
    """Read an integer from an environment variable; raise if unset."""
    n = os.environ.get(var)
    if n is None:
        raise EnvironmentError(f"No {var} environment variable set. " f"Have you run `source config.env`?")
    return int(n)


def extract_model_rowids(model_dict: dict) -> np.array:
    """Return checkpoint track rowids in the same order as the embedding table."""
    return model_dict["vocab"]["track_rowid"]


def extract_model_embeddings(model_dict: dict) -> np.array:
    """Return checkpoint input embeddings as a NumPy array."""
    return model_dict["model_state_dict"]["embeddings_in.weight"].numpy()


def extract_model_dim(model_dict: dict) -> int:
    """Return checkpoint embedding dimension."""
    return model_dict["hparams"]["embed_dim"]


class Tracks:
    """Track-level entity helpers scoped to checkpoint-supported tracks.

    Unlike the higher-level entity classes, tracks do not apply any minimum-track
    threshold. All methods operate on the intersection of `t1_df` and the
    checkpoint vocabulary.
    """

    @staticmethod
    def valid_ids(t1_df: pd.DataFrame, model_dict: dict) -> pd.Index:
        """Return exported `track_rowid` values present in `t1_df` and the checkpoint.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` values.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            `pd.Index` of checkpoint-supported, labeled `track_rowid` values in
            the same order as they appear in `t1_df`.
        """
        assert t1_df["track_rowid"].is_unique, "Expected unique track_rowid in t1_df"
        model_rowids = extract_model_rowids(model_dict)
        valid_tracks = t1_df[
            t1_df["track_rowid"].isin(model_rowids)
            &
            # label are non-null but few tracks have empty label string.
            # these tracks are assigned a null ID in the training vocab. we drop them.
            t1_df["label_rowid"].notna()
        ]["track_rowid"]
        return pd.Index(valid_tracks)

    @staticmethod
    def lookup(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Build the track lookup table for checkpoint-supported tracks.

        Args:
            t1_df: Enriched training vocab with `track_rowid` and
                `playlist_count` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            DataFrame with columns `track_rowid` (`int64`) and `logcounts`
            (`float32`), where `logcounts = log10(playlist_count)`.
        """
        valid_ids = Tracks.valid_ids(t1_df, model_dict)
        mask = t1_df["track_rowid"].isin(valid_ids)
        out = pd.DataFrame(
            {
                "track_rowid": t1_df.loc[mask, "track_rowid"].to_numpy(),
                "logcounts": np.log10(t1_df.loc[mask, "playlist_count"]).astype("float32"),
            }
        )
        out["track_rowid"] = out["track_rowid"].astype("int64")
        out["logcounts"] = out["logcounts"].astype("float32")
        return out

    @staticmethod
    def embeddings(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Return checkpoint track embeddings for tracks present in `t1_df`.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` values.
            model_dict: Training checkpoint containing `embeddings_in.weight`.

        Returns:
            DataFrame with columns `track_rowid` and `e0..e{D-1}`, one row per
            checkpoint-supported track in `t1_df`. Embedding columns are
            `float32`.
        """
        emb_cols = [f"e{i}" for i in range(extract_model_dim(model_dict))]
        emb_df = pd.DataFrame(
            extract_model_embeddings(model_dict).astype("float32", copy=False),
            index=extract_model_rowids(model_dict),
            columns=emb_cols,
        )
        valid_ids = Tracks.valid_ids(t1_df, model_dict)
        emb_df.index.name = "track_rowid"
        out = emb_df.loc[valid_ids].reset_index()
        out["track_rowid"] = out["track_rowid"].astype("int64")
        return out


class Artists:
    """Artist-level entity helpers built from checkpoint-supported tracks.

    Artists are included only when at least `SICK_ARTIST_MINTRACK` of their
    tracks are present in the checkpoint-supported subset of `t1_df`.
    """

    MINTRACK = _get_config_int_parameter("SICK_ARTIST_MINTRACK")

    @staticmethod
    def valid_ids(t1_df: pd.DataFrame, model_dict: dict) -> pd.Index:
        """Return track ids belonging to artists that meet the minimum threshold.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` and
                `artist_rowid` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            `pd.Index` of `track_rowid` values whose artist has at least
            `Artists.MINTRACK` checkpoint-supported tracks.
        """
        base_ids = Tracks.valid_ids(t1_df, model_dict)
        subset = t1_df[t1_df["track_rowid"].isin(base_ids)]
        counts = subset.groupby("artist_rowid")["artist_rowid"].transform("count")
        valid_tracks = subset[counts >= Artists.MINTRACK]["track_rowid"]
        return pd.Index(valid_tracks)

    @staticmethod
    def lookup(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Build the artist lookup table from checkpoint-supported tracks.

        Args:
            t1_df: Enriched training vocab with `track_rowid`, `artist_rowid`,
                and `playlist_count` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            DataFrame with columns `artist_rowid` (`int64`), `logcounts`
            (`float32`), `ntrack` (`int32`), and `nalbum` (`int32`).
            `logcounts` is the mean of per-track `log10(playlist_count)`
            across valid artist tracks. `ntrack` is the number of valid
            tracks per artist. `nalbum` is the number of distinct albums.
        """
        valid_ids = Artists.valid_ids(t1_df, model_dict)
        mask = t1_df["track_rowid"].isin(valid_ids)
        out = (
            np.log10(t1_df.loc[mask, "playlist_count"])
            .astype("float32")
            .groupby(t1_df.loc[mask, "artist_rowid"], sort=False)
            .mean()
            .reset_index(name="logcounts")
        ).merge(
            t1_df.loc[mask].value_counts("artist_rowid"),
            on="artist_rowid"
        ).rename(columns={"count": "ntrack"}
        ).merge(
            t1_df.loc[mask].groupby("artist_rowid", as_index=False).agg(
                nalbum=("album_rowid", pd.Series.nunique)
            )
        )
        out["artist_rowid"] = out["artist_rowid"].astype("int64")
        out["logcounts"] = out["logcounts"].astype("float32")
        out["ntrack"] = out["ntrack"].astype("int32")
        out["nalbum"] = out["nalbum"].astype("int32")
        return out

    @staticmethod
    def embeddings(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Mean-pool checkpoint track embeddings to the artist level.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` and
                `artist_rowid` columns.
            model_dict: Training checkpoint containing `embeddings_in.weight`.

        Returns:
            DataFrame with columns `artist_rowid` and `e0..e{D-1}`, one row per
            valid artist. Embeddings are the mean of checkpoint track embeddings and
            remain `float32`.
        """
        emb_cols = [f"e{i}" for i in range(extract_model_dim(model_dict))]
        emb_df = pd.DataFrame(
            extract_model_embeddings(model_dict).astype("float32", copy=False),
            index=extract_model_rowids(model_dict),
            columns=emb_cols,
        )
        valid_ids = Artists.valid_ids(t1_df, model_dict)
        emb_df.index.name = "track_rowid"
        emb_df = emb_df.loc[valid_ids]
        artist_ids = (
            t1_df.loc[t1_df["track_rowid"].isin(valid_ids), ["track_rowid", "artist_rowid"]]
            .set_index("track_rowid")["artist_rowid"]
            .reindex(emb_df.index)
        )
        emb_df["artist_rowid"] = artist_ids.to_numpy()
        out = emb_df.groupby("artist_rowid", as_index=False)[emb_cols].mean()
        out["artist_rowid"] = out["artist_rowid"].astype("int64")
        for c in emb_cols:
            out[c] = out[c].astype("float32")
        return out


class Albums:
    """Album-level entity helpers built from checkpoint-supported tracks.

    Albums are included only when at least `SICK_ALBUM_MINTRACK` of their
    tracks are present in the checkpoint-supported subset of `t1_df`.
    """

    MINTRACK = _get_config_int_parameter("SICK_ALBUM_MINTRACK")

    @staticmethod
    def valid_ids(t1_df: pd.DataFrame, model_dict: dict) -> pd.Index:
        """Return track ids belonging to albums that meet the minimum threshold.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` and
                `album_rowid` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            `pd.Index` of `track_rowid` values whose album has at least
            `Albums.MINTRACK` checkpoint-supported tracks.
        """
        base_ids = Tracks.valid_ids(t1_df, model_dict)
        subset = t1_df[t1_df["track_rowid"].isin(base_ids)]
        counts = subset.groupby("album_rowid")["album_rowid"].transform("count")
        valid_tracks = subset[counts >= Albums.MINTRACK]["track_rowid"]
        return pd.Index(valid_tracks)

    @staticmethod
    def lookup(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Build the album lookup table from checkpoint-supported tracks.

        Args:
            t1_df: Enriched training vocab with `track_rowid`, `album_rowid`,
                and `playlist_count` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            DataFrame with columns `album_rowid` (`int64`) and `logcounts`
            (`float32`). `logcounts` is the mean of per-track
            `log10(playlist_count)` across valid album tracks.
        """
        valid_ids = Albums.valid_ids(t1_df, model_dict)
        mask = t1_df["track_rowid"].isin(valid_ids)
        out = (
            np.log10(t1_df.loc[mask, "playlist_count"])
            .astype("float32")
            .groupby(t1_df.loc[mask, "album_rowid"], sort=False)
            .mean()
            .reset_index(name="logcounts")
        )
        out["album_rowid"] = out["album_rowid"].astype("int64")
        out["logcounts"] = out["logcounts"].astype("float32")
        return out

    @staticmethod
    def embeddings(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Mean-pool checkpoint track embeddings to the album level.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` and
                `album_rowid` columns.
            model_dict: Training checkpoint containing `embeddings_in.weight`.

        Returns:
            DataFrame with columns `album_rowid` and `e0..e{D-1}`, one row per
            valid album. Embeddings are the mean of checkpoint track embeddings and
            remain `float32`.
        """
        emb_cols = [f"e{i}" for i in range(extract_model_dim(model_dict))]
        emb_df = pd.DataFrame(
            extract_model_embeddings(model_dict).astype("float32", copy=False),
            index=extract_model_rowids(model_dict),
            columns=emb_cols,
        )
        valid_ids = Albums.valid_ids(t1_df, model_dict)
        emb_df.index.name = "track_rowid"
        emb_df = emb_df.loc[valid_ids]
        album_ids = (
            t1_df.loc[t1_df["track_rowid"].isin(valid_ids), ["track_rowid", "album_rowid"]]
            .set_index("track_rowid")["album_rowid"]
            .reindex(emb_df.index)
        )
        emb_df["album_rowid"] = album_ids.to_numpy()
        out = emb_df.groupby("album_rowid", as_index=False)[emb_cols].mean()
        out["album_rowid"] = out["album_rowid"].astype("int64")
        for c in emb_cols:
            out[c] = out[c].astype("float32")
        return out


class Labels:
    """Label-level entity helpers built from checkpoint-supported tracks.

    Labels are included only when at least `SICK_LABEL_MINTRACK` of their
    tracks are present in the checkpoint-supported subset of `t1_df`.
    """

    MINTRACK = _get_config_int_parameter("SICK_LABEL_MINTRACK")

    @staticmethod
    def valid_ids(t1_df: pd.DataFrame, model_dict: dict) -> pd.Index:
        """Return track ids belonging to labels that meet the minimum threshold.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` and
                `label_rowid` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            `pd.Index` of `track_rowid` values whose label has at least
            `Labels.MINTRACK` checkpoint-supported tracks.
        """
        base_ids = Tracks.valid_ids(t1_df, model_dict)
        subset = t1_df[t1_df["track_rowid"].isin(base_ids)]
        counts = subset.groupby("label_rowid")["label_rowid"].transform("count")
        valid_tracks = subset[counts >= Labels.MINTRACK]["track_rowid"]
        return pd.Index(valid_tracks)

    @staticmethod
    def lookup(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Build the label lookup table from checkpoint-supported tracks.

        Args:
            t1_df: Enriched training vocab with `track_rowid`, `label_rowid`,
                and `playlist_count` columns.
            model_dict: Training checkpoint containing the model vocab.

        Returns:
            DataFrame with columns `label_rowid` (`int32`), `logcounts`
            (`float32`), and `ntrack` (`int32`). `logcounts` is the mean of
            per-track `log10(playlist_count)` across valid label tracks.
            `ntrack` is the number of valid tracks per label.
        """
        valid_ids = Labels.valid_ids(t1_df, model_dict)
        mask = t1_df["track_rowid"].isin(valid_ids)
        out = (
            # adds mean of log10(track counts)
            np.log10(t1_df.loc[mask, "playlist_count"])
            .astype("float32")
            .groupby(t1_df.loc[mask, "label_rowid"], sort=False)
            .mean()
            .reset_index(name="logcounts")
        ).merge(
            t1_df.loc[mask].value_counts("label_rowid"),
            on="label_rowid"
        ).rename(columns={"count": "ntrack"}
        ).merge(
            # adds number of albums released by each label
            t1_df.loc[mask].groupby("label_rowid", as_index=False).agg(
                nalbum=("album_rowid", pd.Series.nunique)
            )
        ).merge(
            # adds number of artists in roster
            t1_df.loc[mask].groupby("label_rowid", as_index=False).agg(
                nartist=("artist_rowid", pd.Series.nunique)
            )
        )
        out["label_rowid"] = out["label_rowid"].astype("int32")
        out["logcounts"] = out["logcounts"].astype("float32")
        out["ntrack"] = out["ntrack"].astype("int32")
        out["nalbum"] = out["nalbum"].astype("int32")
        out["nartist"] = out["nartist"].astype("int32")
        return out

    @staticmethod
    def embeddings(t1_df: pd.DataFrame, model_dict: dict) -> pd.DataFrame:
        """Mean-pool checkpoint track embeddings to the label level.

        Args:
            t1_df: Enriched training vocab with unique `track_rowid` and
                `label_rowid` columns.
            model_dict: Training checkpoint containing `embeddings_in.weight`.

        Returns:
            DataFrame with columns `label_rowid` and `e0..e{D-1}`, one row per
            valid label. Embeddings are the mean of checkpoint track embeddings and
            remain `float32`.
        """
        emb_cols = [f"e{i}" for i in range(extract_model_dim(model_dict))]
        emb_df = pd.DataFrame(
            extract_model_embeddings(model_dict).astype("float32", copy=False),
            index=extract_model_rowids(model_dict),
            columns=emb_cols,
        )
        valid_ids = Labels.valid_ids(t1_df, model_dict)
        emb_df.index.name = "track_rowid"
        emb_df = emb_df.loc[valid_ids]
        label_ids = (
            t1_df.loc[t1_df["track_rowid"].isin(valid_ids), ["track_rowid", "label_rowid"]]
            .set_index("track_rowid")["label_rowid"]
            .reindex(emb_df.index)
        )
        emb_df["label_rowid"] = label_ids.to_numpy()
        out = emb_df.groupby("label_rowid", as_index=False)[emb_cols].mean()
        out["label_rowid"] = out["label_rowid"].astype("int32")
        for c in emb_cols:
            out[c] = out[c].astype("float32")
        return out
