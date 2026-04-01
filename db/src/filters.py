from typing import Callable

import pandas as pd

from src.utils import ENTITY_KEYS as EKEYS, EntityTable


def filter_label(lookup_label: pd.DataFrame, min_nartist: int) -> pd.DataFrame:
    """Filters out labels with small roster."""
    return lookup_label[
        lookup_label["nartist"] >= min_nartist
    ].reset_index(drop=True)


def filter_artist(lookup_artist: pd.DataFrame, min_ntrack: int) -> pd.DataFrame:
    """Filters out artists with very small catalog."""
    return lookup_artist[
        lookup_artist["ntrack"] >= min_ntrack
    ].reset_index(drop=True)


def filter_album(lookup_album: pd.DataFrame, min_total_tracks: int) -> pd.DataFrame:
    """Filters album with small total tracks number."""
    return lookup_album[
        lookup_album["total_tracks"] >= min_total_tracks
    ].reset_index(drop=True)


def filter_track(lookup_track: pd.DataFrame, min_logcount: float) -> pd.DataFrame:
    """Filter tracks under playlist appearances threshold."""
    return (
        lookup_track
        .sort_values("logcount", ascending=False)
        .drop_duplicates("id_isrc", keep="first")
        .sort_values("track_rowid")
        .query("logcount >= @min_logcount")
        .reset_index(drop=True)
    )


def filter_cascade(
    track: pd.DataFrame, filter_track: Callable[[pd.DataFrame], pd.DataFrame],
    album: pd.DataFrame, filter_album: Callable[[pd.DataFrame], pd.DataFrame],
    artist: pd.DataFrame, filter_artist: Callable[[pd.DataFrame], pd.DataFrame],
    label: pd.DataFrame, filter_label: Callable[[pd.DataFrame], pd.DataFrame],
) -> EntityTable:
    """Apply per-entity filters with cascading referential pruning.

    The cascade flows top-down: tracks are filtered first, then each downstream
    entity is restricted to rowids still referenced by the tier above it before
    its own size filter is applied.
    """
    track = filter_track(track)
    album_rowids = track[EKEYS.album].unique()
    album = album[album[EKEYS.album].isin(album_rowids)]
    album = filter_album(album)
    artist_rowids = album[EKEYS.artist].unique()
    artist = artist[artist[EKEYS.artist].isin(artist_rowids)]
    artist = filter_artist(artist)
    label_rowids = album[EKEYS.label].unique()
    label = label[label[EKEYS.label].isin(label_rowids)]
    label = filter_label(label)
    return EntityTable(
        track=track,
        artist=artist,
        album=album,
        label=label,
    )

def filter_separate(
    track: pd.DataFrame, filter_track: Callable[[pd.DataFrame], pd.DataFrame],
    album: pd.DataFrame, filter_album: Callable[[pd.DataFrame], pd.DataFrame],
    artist: pd.DataFrame, filter_artist: Callable[[pd.DataFrame], pd.DataFrame],
    label: pd.DataFrame, filter_label: Callable[[pd.DataFrame], pd.DataFrame],
) -> EntityTable:
    """
    Applies filters separately to eahc lookup.
    """
    return EntityTable(
        track=filter_track(track),
        artist=filter_artist(artist),
        album=filter_album(album),
        label=filter_label(label),
    )
