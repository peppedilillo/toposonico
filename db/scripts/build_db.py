#!/usr/bin/env python3
"""Build the SQLite database from parquet inputs.

Loads lookup parquets (via manifest), geo parquets, and embedding parquets to
produce a single SQLite DB with:
- Denormalized entity metadata with canonical IDs for dedup and navigation
- Per-entity searchable/recable flags for downstream index construction
- Per-entity embedding tables (L2-normalized BLOBs) for FAISS queries
- Ranked representative children for hierarchy navigation

All entity metadata comes from the ml lookups — no source Spotify DB needed.

Usage:
    source config.env && uv run python scripts/build_db.py [options]

Examples:
    source config.env && uv run python scripts/build_db.py
    uv run python scripts/build_db.py --manifest ml/outs/manifest.toml --db outs/sick.db
"""

import argparse
import os
from pathlib import Path
import re
import sqlite3
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.utils import _get_config_float
from src.utils import _get_config_int
from src.utils import ENTITY_KEYS as EKEYS
from src.utils import EntityPaths
from src.utils import get_geo_paths
from src.utils import read_manifest

BATCH_SIZE = 500_000

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

CREATE TABLE tracks (
    track_rowid           INTEGER PRIMARY KEY,
    track_canonical_rowid INTEGER NOT NULL REFERENCES tracks(track_rowid),
    track_name            TEXT    NOT NULL,
    track_name_norm       TEXT    NOT NULL,
    logcount              REAL    NOT NULL,
    searchable            INTEGER NOT NULL DEFAULT 0,
    recable               INTEGER NOT NULL DEFAULT 0,
    lon                   REAL    NOT NULL,
    lat                   REAL    NOT NULL,
    artist_rowid          INTEGER NOT NULL REFERENCES artists(artist_rowid),
    artist_name           TEXT    NOT NULL,
    artist_lon            REAL    NOT NULL,
    artist_lat            REAL    NOT NULL,
    album_rowid           INTEGER NOT NULL REFERENCES albums(album_rowid),
    album_name            TEXT    NOT NULL,
    album_lon             REAL    NOT NULL,
    album_lat             REAL    NOT NULL,
    label_rowid           INTEGER NOT NULL REFERENCES labels(label_rowid),
    label                 TEXT    NOT NULL,
    label_lon             REAL    NOT NULL,
    label_lat             REAL    NOT NULL,
    track_popularity      INTEGER,
    release_date          TEXT,
    id_isrc               TEXT
);

CREATE TABLE albums (
    album_rowid              INTEGER PRIMARY KEY,
    album_canonical_rowid    INTEGER NOT NULL REFERENCES albums(album_rowid),
    album_name               TEXT    NOT NULL,
    album_name_norm          TEXT    NOT NULL,
    logcount                 REAL    NOT NULL,
    searchable               INTEGER NOT NULL DEFAULT 0,
    recable                  INTEGER NOT NULL DEFAULT 0,
    nrepr                    INTEGER NOT NULL DEFAULT 0,
    lon                      REAL    NOT NULL,
    lat                      REAL    NOT NULL,
    artist_rowid             INTEGER NOT NULL REFERENCES artists(artist_rowid),
    artist_name              TEXT    NOT NULL,
    artist_lon               REAL    NOT NULL,
    artist_lat               REAL    NOT NULL,
    label                    TEXT    NOT NULL,
    label_rowid              INTEGER NOT NULL REFERENCES labels(label_rowid),
    label_lon                REAL    NOT NULL,
    label_lat                REAL    NOT NULL,
    album_type               TEXT,
    total_tracks             INTEGER,
    release_date             TEXT,
    release_date_precision   TEXT
);

CREATE TABLE artists (
    artist_rowid           INTEGER PRIMARY KEY,
    artist_canonical_rowid INTEGER NOT NULL REFERENCES artists(artist_rowid),
    artist_name            TEXT    NOT NULL,
    lon                    REAL    NOT NULL,
    lat                    REAL    NOT NULL,
    logcount               REAL    NOT NULL,
    ntrack                 INTEGER NOT NULL,
    nalbum                 INTEGER NOT NULL,
    searchable             INTEGER NOT NULL DEFAULT 0,
    recable                INTEGER NOT NULL DEFAULT 0,
    nrepr                  INTEGER NOT NULL DEFAULT 0,
    artist_genre           TEXT
);

CREATE TABLE labels (
    label_rowid           INTEGER PRIMARY KEY,
    label_canonical_rowid INTEGER NOT NULL REFERENCES labels(label_rowid),
    label                 TEXT    NOT NULL UNIQUE,
    logcount              REAL    NOT NULL,
    ntrack                INTEGER NOT NULL,
    nalbum                INTEGER NOT NULL,
    nartist               INTEGER NOT NULL,
    searchable            INTEGER NOT NULL DEFAULT 0,
    recable               INTEGER NOT NULL DEFAULT 0,
    nrepr                 INTEGER NOT NULL DEFAULT 0,
    lon                   REAL    NOT NULL,
    lat                   REAL    NOT NULL
);

CREATE TABLE track_embedding (
    track_rowid INTEGER PRIMARY KEY REFERENCES tracks(track_rowid),
    embedding   BLOB    NOT NULL
);

CREATE TABLE album_embedding (
    album_rowid INTEGER PRIMARY KEY REFERENCES albums(album_rowid),
    embedding   BLOB    NOT NULL
);

CREATE TABLE artist_embedding (
    artist_rowid INTEGER PRIMARY KEY REFERENCES artists(artist_rowid),
    embedding    BLOB    NOT NULL
);

CREATE TABLE label_embedding (
    label_rowid INTEGER PRIMARY KEY REFERENCES labels(label_rowid),
    embedding   BLOB    NOT NULL
);

CREATE TABLE album_repr_tracks (
    album_rowid INTEGER NOT NULL REFERENCES albums(album_rowid),
    rank        INTEGER NOT NULL,
    track_rowid INTEGER NOT NULL REFERENCES tracks(track_rowid),
    score       REAL    NOT NULL,
    PRIMARY KEY (album_rowid, rank)
);

CREATE TABLE artist_repr_albums (
    artist_rowid INTEGER NOT NULL REFERENCES artists(artist_rowid),
    rank         INTEGER NOT NULL,
    album_rowid  INTEGER NOT NULL REFERENCES albums(album_rowid),
    score        REAL    NOT NULL,
    PRIMARY KEY (artist_rowid, rank)
);

CREATE TABLE label_repr_artists (
    label_rowid  INTEGER NOT NULL REFERENCES labels(label_rowid),
    rank         INTEGER NOT NULL,
    artist_rowid INTEGER NOT NULL REFERENCES artists(artist_rowid),
    score        REAL    NOT NULL,
    PRIMARY KEY (label_rowid, rank)
);
"""

POST_DDL = """
CREATE INDEX idx_tracks_artist ON tracks(artist_rowid);
CREATE INDEX idx_tracks_album  ON tracks(album_rowid);
CREATE INDEX idx_tracks_label  ON tracks(label_rowid);
CREATE INDEX idx_albums_artist ON albums(artist_rowid);
"""


ALBUM_TITLE_VARIANT_MARKERS = ("remaster", "edition", "version", "deluxe", "expanded", "anniversary")


def normalize_title(name: str) -> str:
    """Strip a small set of trailing edition markers from titles."""
    s = " ".join(name.split())
    s = re.sub(r"\.{3,}", "..", s)
    # Match only a trailing variant suffix after at least one title character:
    # spaced hyphen suffixes, bracket suffixes, or parenthesized suffixes.
    # The lookbehind keeps bracket-led titles from normalizing to an empty string.
    match = re.search(r"(?<=.)(\s+-\s*[^()]*$|\s*\[.*\]\s*$|\s*\([^()]*\)\s*$)", s)
    if not match:
        return s

    marker = match.group(0).lower()
    if any(token in marker for token in ALBUM_TITLE_VARIANT_MARKERS):
        return s[: match.start()].rstrip()
    return s


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str], entity: str) -> None:
    """Fail fast if schema-required columns contain null/empty values."""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{entity}: missing required columns: {', '.join(missing)}")

    invalid_parts = []
    for col in required_cols:
        series = df[col]
        mask = series.isna()
        if pd.api.types.is_string_dtype(series):
            mask = mask | series.astype(str).str.strip().eq("")
        invalid_count = int(mask.sum())
        if invalid_count:
            invalid_parts.append(f"{col}={invalid_count}")

    if invalid_parts:
        raise ValueError(
            f"{entity}: required columns contain null/empty values: {', '.join(invalid_parts)}"
        )


def get_album_canonical_updates(albums: pd.DataFrame) -> pd.DataFrame:
    """Return canonical album assignments for an albums DataFrame.

    Albums are grouped by artist, album type, and case-insensitive normalized
    title. Within each group, the entry with the smallest album_rowid is
    elected as canonical.
    """
    albums["album_type_norm"] = albums["album_type"].fillna("")
    albums["album_name_lower"] = albums["album_name_norm"].str.lower()
    group_keys = ["artist_rowid", "album_type_norm", "album_name_lower"]
    canonical = (
        albums.sort_values(group_keys + ["album_rowid"])
        .drop_duplicates(group_keys, keep="first")[group_keys + ["album_rowid"]]
        .rename(columns={"album_rowid": "album_canonical_rowid"})
    )
    return albums.merge(
        canonical,
        on=group_keys,
        how="left",
    )[["album_canonical_rowid", "album_rowid"]]


def canonicalize_albums(conn: sqlite3.Connection) -> None:
    """Assign each album row to a canonical album row within its duplicate group."""
    albums = pd.read_sql(
        """
        SELECT
            album_rowid,
            artist_rowid,
            album_type,
            album_name,
            album_name_norm
        FROM albums
        """,
        conn,
    )
    updates = get_album_canonical_updates(albums)
    conn.executemany(
        "UPDATE albums SET album_canonical_rowid = ? WHERE album_rowid = ?",
        list(updates.itertuples(index=False, name=None)),
    )


def get_track_canonical_updates(tracks: pd.DataFrame) -> pd.DataFrame:
    """Return canonical track assignments for a tracks DataFrame.

    Tracks are grouped by canonical album and case-insensitive normalized
    title. Within each group the entry with the smallest track_rowid is
    elected as canonical.
    """
    if "album_canonical_rowid" not in tracks.columns:
        if "album_rowid" not in tracks.columns:
            raise KeyError("tracks must include album_canonical_rowid or album_rowid")
        tracks = tracks.rename(columns={"album_rowid": "album_canonical_rowid"})

    tracks["track_name_lower"] = tracks["track_name_norm"].str.lower()
    group_keys = ["album_canonical_rowid", "track_name_lower"]
    canonical = (
        tracks.sort_values(group_keys + ["track_rowid"])
        .drop_duplicates(group_keys, keep="first")[group_keys + ["track_rowid"]]
        .rename(columns={"track_rowid": "track_canonical_rowid"})
    )
    return tracks.merge(
        canonical,
        on=group_keys,
        how="left",
    )[["track_canonical_rowid", "track_rowid"]]


def canonicalize_tracks(conn: sqlite3.Connection) -> None:
    """Assign each track row to a canonical track row within its duplicate group.

    Groups by canonical album (not raw album_rowid), so tracks appearing in
    duplicate albums are correctly deduplicated.
    """
    tracks = pd.read_sql(
        "SELECT t.track_rowid, a.album_canonical_rowid, t.track_name_norm "
        "FROM tracks t JOIN albums a ON a.album_rowid = t.album_rowid",
        conn,
    )
    updates = get_track_canonical_updates(tracks)
    conn.executemany(
        "UPDATE tracks SET track_canonical_rowid = ? WHERE track_rowid = ?",
        list(updates.itertuples(index=False, name=None)),
    )


def compute_searchable_recable(
    conn: sqlite3.Connection,
    searchable_track_min_logcount: float,
    searchable_album_min_total_tracks: int,
    searchable_artist_min_ntrack: int,
    searchable_label_min_nartist: int,
    recable_track_min_logcount: float,
) -> None:
    """Set searchable and recable flags on all entity tables.

    An entity is searchable if it is self-canonical and meets per-entity
    threshold criteria. An entity is recable if it is searchable (and for
    tracks, meets an additional logcount threshold).
    """
    conn.execute(
        "UPDATE tracks SET searchable = 1 "
        "WHERE track_canonical_rowid = track_rowid AND logcount >= ?",
        (searchable_track_min_logcount,),
    )
    conn.execute(
        "UPDATE albums SET searchable = 1 " 
        "WHERE album_canonical_rowid = album_rowid AND total_tracks >= ?",
        (searchable_album_min_total_tracks,),
    )
    conn.execute(
        "UPDATE artists SET searchable = 1 " 
        "WHERE artist_canonical_rowid = artist_rowid AND ntrack >= ?",
        (searchable_artist_min_ntrack,),
    )
    conn.execute(
        "UPDATE labels SET searchable = 1 " 
        "WHERE label_canonical_rowid = label_rowid AND nartist >= ?",
        (searchable_label_min_nartist,),
    )

    conn.execute(
        "UPDATE tracks SET recable = 1 WHERE searchable = 1 AND logcount >= ?",
        (recable_track_min_logcount,),
    )
    conn.execute("UPDATE albums SET recable = 1 WHERE searchable = 1")
    conn.execute("UPDATE artists SET recable = 1 WHERE searchable = 1")
    conn.execute("UPDATE labels SET recable = 1 WHERE searchable = 1")

    for table in ("tracks", "albums", "artists", "labels"):
        total, searchable, recable = conn.execute(
            f"SELECT COUNT(*), SUM(searchable), SUM(recable) FROM {table}"
        ).fetchone()
        print(f"  [{table}] total={total:,}  searchable={searchable:,}  recable={recable:,}")


def build_labels(
    conn: sqlite3.Connection,
    lookup: pd.DataFrame,
    geo: pd.DataFrame,
) -> pd.DataFrame:
    """Build the labels table. Returns geo for downstream denormalization."""
    df = lookup.merge(geo, on=EKEYS.label)
    df["label_canonical_rowid"] = df[EKEYS.label]
    df[
        [
            EKEYS.label,
            "label_canonical_rowid",
            "label",
            "logcount",
            "ntrack",
            "nalbum",
            "nartist",
            "lon",
            "lat",
        ]
    ].to_sql("labels", conn, if_exists="append", index=False)
    print(f"  [labels]  {len(df):,} rows")
    return geo


def build_artists(
    conn: sqlite3.Connection,
    lookup: pd.DataFrame,
    geo: pd.DataFrame,
) -> pd.DataFrame:
    """Build the artists table. Returns geo for downstream denormalization."""
    df = lookup.merge(geo, on=EKEYS.artist)
    df["artist_canonical_rowid"] = df[EKEYS.artist]
    df[
        [
            EKEYS.artist,
            "artist_canonical_rowid",
            "artist_name",
            "artist_genre",
            "logcount",
            "ntrack",
            "nalbum",
            "lon",
            "lat",
        ]
    ].to_sql("artists", conn, if_exists="append", index=False)
    print(f"  [artists] {len(df):,} rows")
    return geo


def build_albums(
    conn: sqlite3.Connection,
    lookup: pd.DataFrame,
    geo: pd.DataFrame,
    artist_geo: pd.DataFrame,
    label_geo: pd.DataFrame,
) -> pd.DataFrame:
    """Build the albums table. Returns geo for downstream denormalization."""
    df = lookup.merge(geo, on=EKEYS.album)
    df = df.merge(
        artist_geo.rename(columns={"lon": "artist_lon", "lat": "artist_lat"}),
        on=EKEYS.artist,
        how="left",
    )
    df = df.merge(
        label_geo.rename(columns={"lon": "label_lon", "lat": "label_lat"}),
        on=EKEYS.label,
        how="left",
    )
    df["album_name_norm"] = df["album_name"].map(normalize_title)
    df["album_canonical_rowid"] = df[EKEYS.album]
    _validate_required_columns(
        df,
        [
            "artist_name",
            "artist_lon",
            "artist_lat",
            "label",
            "label_lon",
            "label_lat",
        ],
        "albums",
    )

    df[
        [
            EKEYS.album,
            "album_canonical_rowid",
            "album_name",
            "album_name_norm",
            "logcount",
            "lon",
            "lat",
            EKEYS.artist,
            "artist_name",
            "artist_lon",
            "artist_lat",
            "album_type",
            "label",
            "total_tracks",
            "release_date",
            "release_date_precision",
            EKEYS.label,
            "label_lon",
            "label_lat",
        ]
    ].to_sql("albums", conn, if_exists="append", index=False)
    print(f"  [albums]  {len(df):,} rows")
    return geo


def build_tracks(
    conn: sqlite3.Connection,
    lookup_path: Path,
    track_geo: pd.DataFrame,
    artist_geo: pd.DataFrame,
    album_geo: pd.DataFrame,
    label_geo: pd.DataFrame,
    batch_size: int,
) -> None:
    """Build the tracks table, streaming the lookup parquet in batches."""
    artist_geo_r = artist_geo.rename(columns={"lon": "artist_lon", "lat": "artist_lat"})
    album_geo_r = album_geo.rename(columns={"lon": "album_lon", "lat": "album_lat"})
    label_geo_r = label_geo.rename(columns={"lon": "label_lon", "lat": "label_lat"})

    out_cols = [
        EKEYS.track,
        "track_canonical_rowid",
        "track_name",
        "track_name_norm",
        "track_popularity",
        "logcount",
        "release_date",
        "id_isrc",
        "lon",
        "lat",
        EKEYS.artist,
        "artist_name",
        "artist_lon",
        "artist_lat",
        EKEYS.album,
        "album_name",
        "album_lon",
        "album_lat",
        EKEYS.label,
        "label",
        "label_lon",
        "label_lat",
    ]

    pf = pq.ParquetFile(lookup_path)
    total = 0
    t0 = time.time()

    for batch in pf.iter_batches(batch_size=batch_size):
        df = (
            batch.to_pandas()
            .merge(track_geo, on=EKEYS.track, how="inner")
            .merge(artist_geo_r, on=EKEYS.artist, how="left")
            .merge(album_geo_r, on=EKEYS.album, how="left")
            .merge(label_geo_r, on=EKEYS.label, how="left")
        )
        df["track_canonical_rowid"] = df[EKEYS.track]
        df["track_name_norm"] = df["track_name"].map(normalize_title)
        _validate_required_columns(
            df,
            [
                "artist_name",
                "artist_lon",
                "artist_lat",
                "album_name",
                "album_lon",
                "album_lat",
                "label",
                "label_lon",
                "label_lat",
            ],
            "tracks",
        )
        df[out_cols].to_sql("tracks", conn, if_exists="append", index=False)

        total += len(df)
        rate = total / (time.time() - t0)
        print(f"  [tracks]  {total:>9,}  ({rate:,.0f} rows/s)", end="\r")

    print(f"\n  [tracks]  {total:,} rows  ({time.time()-t0:.1f}s)")


def build_album_repr_tracks(conn: sqlite3.Connection, limit: int) -> None:
    """Store each album's top tracks by logcount."""
    t0 = time.time()
    conn.execute("DELETE FROM album_repr_tracks")
    conn.execute(
        """
        INSERT INTO album_repr_tracks (album_rowid, rank, track_rowid, score)
        WITH ranked AS (
            SELECT
                album_rowid,
                track_rowid,
                logcount AS score,
                ROW_NUMBER() OVER (
                    PARTITION BY album_rowid
                    ORDER BY logcount DESC, track_rowid ASC
                ) AS rn
            FROM tracks 
            WHERE searchable = 1 
        )
        SELECT album_rowid, rn - 1, track_rowid, score
        FROM ranked
        WHERE rn <= ?
        """,
        (limit,),
    )
    total = conn.execute("SELECT COUNT(*) FROM album_repr_tracks").fetchone()[0]
    print(f"  [album_repr_tracks]  {total:,} rows  ({time.time() - t0:.1f}s)")


def rank_artist_repr_albums(
    tracks: pd.DataFrame,
    albums: pd.DataFrame,
    limit: int,
) -> pd.DataFrame:
    """Rank representative albums per artist.

    Tracks are grouped under their canonical album. Scored by median
    child-track logcount. Prefers full albums over EPs over other release
    types. Expects pre-filtered (searchable) inputs.

    Returns a DataFrame with columns: artist_rowid, rank, album_rowid, score.
    """
    # the tracks we selected are self-canonical.
    # they could still belong to different versions of the same album.
    # we group them under the same album entry.
    # this is somewhat unfortunate as it may artificially inflate the album tracklist
    # and drift the median.
    # the alternative is not great anyway: we could group under album_rowid, ending up
    # losing important track entries from an album entity.
    album_scores = tracks.groupby("album_canonical_rowid")["logcount"].median().rename("score")
    candidates = albums.merge(album_scores, left_on="album_rowid", right_on="album_canonical_rowid")

    # EP will have less tracks and an absolute hit may throw an EP above a more relevant
    # album entry. singles and nan falls together at priority 2.
    type_map = {"album": 0, "ep": 1}
    candidates["type_priority"] = (
        # this is needed as album types can definitely be nan or empty.
        candidates["album_type"].fillna("").str.lower().map(type_map).fillna(2).astype(int)
    )

    best = candidates.groupby("artist_rowid")["type_priority"].min().rename("best")
    candidates = candidates.merge(best, on="artist_rowid")
    candidates = candidates[candidates["type_priority"] == candidates["best"]]

    # we sort by score, then logcount, then rowid (for tie-break) and keep the first `n`.
    candidates = candidates.sort_values(
        ["artist_rowid", "score", "logcount", "album_rowid"],
        ascending=[True, False, False, True],
    )
    candidates["rank"] = candidates.groupby("artist_rowid").cumcount()
    candidates = candidates[candidates["rank"] < limit]

    return candidates[["artist_rowid", "rank", "album_rowid", "score"]]


def build_artist_repr_albums(conn: sqlite3.Connection, limit: int) -> None:
    """Store representative albums for each artist."""
    t0 = time.time()
    conn.execute("DELETE FROM artist_repr_albums")

    tracks = pd.read_sql(
        "SELECT a.album_canonical_rowid, t.logcount "
        "FROM tracks t "
        "JOIN albums a ON a.album_rowid = t.album_rowid "
        "WHERE t.searchable = 1",
        conn,
    )
    albums = pd.read_sql(
        "SELECT album_rowid, artist_rowid, logcount, album_type "
        "FROM albums WHERE searchable = 1",
        conn,
    )

    result = rank_artist_repr_albums(tracks, albums, limit)
    if not result.empty:
        result.to_sql("artist_repr_albums", conn, if_exists="append", index=False)

    print(f"  [artist_repr_albums] {len(result):,} rows  ({time.time() - t0:.1f}s)")


def rank_label_repr_artists(
    tracks: pd.DataFrame,
    limit: int,
) -> pd.DataFrame:
    """Rank representative artists per label.

    Scored as sum(logcount) / sqrt(track_count), which softens catalog-size
    bias. Ties broken by rowid.
    Expects pre-filtered (searchable tracks of searchable artists) inputs.

    Returns a DataFrame with columns: label_rowid, rank, artist_rowid, score.
    """
    stats = tracks.groupby(["label_rowid", "artist_rowid"]).agg(
        total_logcount=("logcount", "sum"),
        track_count=("logcount", "count"),
    )
    stats["score"] = stats["total_logcount"] / np.sqrt(stats["track_count"])
    stats = stats.reset_index()

    stats = stats.sort_values(
        ["label_rowid", "score", "artist_rowid"],
        ascending=[True, False, True],
    )
    stats["rank"] = stats.groupby("label_rowid").cumcount()
    stats = stats[stats["rank"] < limit]

    return stats[["label_rowid", "rank", "artist_rowid", "score"]]


def build_label_repr_artists(conn: sqlite3.Connection, limit: int) -> None:
    """Store representative artists for each label."""
    t0 = time.time()
    conn.execute("DELETE FROM label_repr_artists")

    tracks = pd.read_sql(
        "SELECT t.label_rowid, t.artist_rowid, t.logcount "
        "FROM tracks t "
        "JOIN artists a ON a.artist_rowid = t.artist_rowid "
        "AND a.searchable = 1 "
        "WHERE t.searchable = 1",
        conn,
    )

    result = rank_label_repr_artists(tracks, limit)
    if not result.empty:
        result.to_sql("label_repr_artists", conn, if_exists="append", index=False)

    print(f"  [label_repr_artists] {len(result):,} rows  ({time.time() - t0:.1f}s)")


def build_representatives(conn: sqlite3.Connection, limit: int) -> None:
    """Build all ranked representative-child tables and set nrepr counts."""
    build_album_repr_tracks(conn, limit)
    build_artist_repr_albums(conn, limit)
    build_label_repr_artists(conn, limit)

    conn.execute(
        "UPDATE albums SET nrepr = "
        "(SELECT COUNT(*) FROM album_repr_tracks r "
        "WHERE r.album_rowid = albums.album_rowid)"
    )
    conn.execute(
        "UPDATE artists SET nrepr = "
        "(SELECT COUNT(*) FROM artist_repr_albums r "
        "WHERE r.artist_rowid = artists.artist_rowid)"
    )
    conn.execute(
        "UPDATE labels SET nrepr = "
        "(SELECT COUNT(*) FROM label_repr_artists r "
        "WHERE r.label_rowid = labels.label_rowid)"
    )
    conn.commit()

    for table in ("albums", "artists", "labels"):
        total, with_repr = conn.execute(
            f"SELECT COUNT(*), SUM(nrepr > 0) FROM {table}"
        ).fetchone()
        print(f"  [{table}] {with_repr:,}/{total:,} have repr children")


def build_embedding(
    conn: sqlite3.Connection,
    embedding_path: Path,
    key_col: str,
    table_name: str,
    batch_size: int,
) -> None:
    """Load one entity's embedding parquet, L2-normalize, write BLOBs."""
    pf = pq.ParquetFile(embedding_path)
    total = 0
    t0 = time.time()

    for batch in pf.iter_batches(batch_size=batch_size):
        df = batch.to_pandas()
        if df.empty:
            continue
        rowids = df[key_col].to_numpy()
        matrix = np.ascontiguousarray(df.filter(regex=r"^e\d+$").to_numpy(dtype=np.float32))
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        np.maximum(norms, 1e-12, out=norms)
        matrix /= norms

        rows = [(int(rid), emb.tobytes()) for rid, emb in zip(rowids, matrix)]
        conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?)", rows)

        total += len(rows)
        elapsed = time.time() - t0
        rate = total / elapsed if elapsed > 0 else 0
        print(f"  [{table_name}] {total:>9,}  ({rate:,.0f} rows/s)", end="\r")

    elapsed = time.time() - t0
    print(f"\n  [{table_name}] {total:,} rows  ({elapsed:.1f}s)")


def build_embeddings(
    conn: sqlite3.Connection,
    embedding_paths: EntityPaths,
    batch_size: int,
) -> None:
    """Build all four embedding tables."""
    for entity, emb_path, key_col in (
        ("track", embedding_paths.track, EKEYS.track),
        ("album", embedding_paths.album, EKEYS.album),
        ("artist", embedding_paths.artist, EKEYS.artist),
        ("label", embedding_paths.label, EKEYS.label),
    ):
        build_embedding(conn, emb_path, key_col, f"{entity}_embedding", batch_size)
    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Build SQLite DB from lookup, geo, and embedding parquets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--manifest",
        default=os.environ.get("SICK_MANIFEST"),
        help="Path to ml manifest TOML. $SICK_MANIFEST",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("SICK_DB"),
        help="Output SQLite path. $SICK_DB",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Rows per write batch (default: {BATCH_SIZE:,})",
    )
    args = parser.parse_args()

    if args.manifest is None:
        raise ValueError("--manifest / $SICK_MANIFEST not set")
    if args.db is None:
        raise ValueError("--db / $SICK_DB not set")

    manifest = read_manifest(args.manifest, required_sections=("embedding", "lookup"))
    lookup_paths = manifest["lookup"]
    embedding_paths = manifest["embedding"]

    geo_paths = get_geo_paths()

    searchable_track_min_logcount = _get_config_float("SICK_SEARCHABLE_TRACK_MIN_LOGCOUNT")
    searchable_album_min_total_tracks = _get_config_int("SICK_SEARCHABLE_ALBUM_MIN_TOTAL_TRACKS")
    searchable_artist_min_ntrack = _get_config_int("SICK_SEARCHABLE_ARTIST_MIN_NTRACK")
    searchable_label_min_nartist = _get_config_int("SICK_SEARCHABLE_LABEL_MIN_NARTIST")
    recable_track_min_logcount = _get_config_float("SICK_RECABLE_TRACK_MIN_LOGCOUNT")
    representative_count = _get_config_int("SICK_REPRESENTATIVE_N")

    db_path = Path(args.db)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"DB             : {db_path}")
    print(f"Lookup track   : {lookup_paths.track}")
    print(f"Lookup artist  : {lookup_paths.artist}")
    print(f"Lookup album   : {lookup_paths.album}")
    print(f"Lookup label   : {lookup_paths.label}")
    print()

    t_total = time.time()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(DDL)

    print("Loading lookups...")
    label_lookup = pd.read_parquet(lookup_paths.label)
    artist_lookup = pd.read_parquet(lookup_paths.artist)
    album_lookup = pd.read_parquet(lookup_paths.album)
    print(f"  labels={len(label_lookup):,}" f"  artists={len(artist_lookup):,}" f"  albums={len(album_lookup):,}")

    print("Loading geo...")
    track_geo = pd.read_parquet(geo_paths.track)
    artist_geo = pd.read_parquet(geo_paths.artist)
    album_geo = pd.read_parquet(geo_paths.album)
    label_geo = pd.read_parquet(geo_paths.label)
    print(
        f"  tracks={len(track_geo):,}"
        f"  artists={len(artist_geo):,}"
        f"  albums={len(album_geo):,}"
        f"  labels={len(label_geo):,}"
    )
    print()

    print("Building entity tables...")
    label_geo = build_labels(conn, label_lookup, label_geo)
    artist_geo = build_artists(conn, artist_lookup, artist_geo)
    album_geo = build_albums(conn, album_lookup, album_geo, artist_geo, label_geo)
    canonicalize_albums(conn)
    conn.commit()

    build_tracks(
        conn,
        lookup_paths.track,
        track_geo,
        artist_geo,
        album_geo,
        label_geo,
        args.batch_size,
    )
    canonicalize_tracks(conn)
    conn.commit()

    print("\nComputing searchable/recable flags...")
    compute_searchable_recable(
        conn,
        searchable_track_min_logcount,
        searchable_album_min_total_tracks,
        searchable_artist_min_ntrack,
        searchable_label_min_nartist,
        recable_track_min_logcount,
    )
    conn.commit()

    print("\nCreating indexes...")
    conn.executescript(POST_DDL)
    conn.commit()

    print(f"\nBuilding representative children (top {representative_count})...")
    build_representatives(conn, representative_count)

    print("\nBuilding embedding tables...")
    build_embeddings(conn, embedding_paths, args.batch_size)

    conn.close()

    size_mb = db_path.stat().st_size / 1024 / 1024
    print(f"\nDone in {time.time()-t_total:.1f}s  —  {db_path}  ({size_mb:,.0f} MB)")


if __name__ == "__main__":
    main()
