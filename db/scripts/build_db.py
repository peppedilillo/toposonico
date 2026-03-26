#!/usr/bin/env python3
"""Build the SQLite database from track2map output parquets.

Loads lookup, geo, and KNN parquets and writes a single SQLite DB:
  - tracks, albums, artists, labels  — with denormalized geo for cross-entity navigation
  - track_knn, album_knn, artist_knn, label_knn  — flat format, self-match excluded

Usage:
    source config.env && uv run python scripts/build_db.py [options]

Examples:
    source config.env && uv run python scripts/build_db.py
    source config.env && uv run python scripts/build_db.py --knn-k 20
"""

import argparse
import os
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.entities import Artists, Albums, Labels

BATCH_SIZE = 500_000
KNN_K_DEFAULT = 20

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

CREATE TABLE tracks (
    track_rowid      INTEGER PRIMARY KEY,
    track_name       TEXT    NOT NULL,
    track_popularity INTEGER,
    logcounts        REAL    NOT NULL,
    release_date     TEXT,
    lon              REAL    NOT NULL,
    lat              REAL    NOT NULL,
    artist_rowid     INTEGER NOT NULL,
    artist_name      TEXT,
    artist_lon       REAL,
    artist_lat       REAL,
    album_rowid      INTEGER NOT NULL,
    album_name       TEXT,
    album_lon        REAL,
    album_lat        REAL,
    label_id         INTEGER,
    label            TEXT,
    label_lon        REAL,
    label_lat        REAL
);

CREATE TABLE albums (
    album_rowid          INTEGER PRIMARY KEY,
    album_name           TEXT    NOT NULL,
    track_count          INTEGER NOT NULL,
    logcounts            REAL    NOT NULL,
    lon                  REAL    NOT NULL,
    lat                  REAL    NOT NULL,
    artist_rowid         INTEGER NOT NULL,
    artist_name          TEXT,
    artist_lon           REAL,
    artist_lat           REAL,
    album_type           TEXT,
    label                TEXT,
    popularity           INTEGER,
    total_tracks         INTEGER,
    release_date         TEXT,
    release_date_precision TEXT,
    label_id             INTEGER,
    label_lon            REAL,
    label_lat            REAL
);

CREATE TABLE artists (
    artist_rowid INTEGER PRIMARY KEY,
    artist_name  TEXT    NOT NULL,
    track_count  INTEGER NOT NULL,
    logcounts    REAL    NOT NULL,
    lon          REAL    NOT NULL,
    lat          REAL    NOT NULL,
    popularity   INTEGER,
    genre        TEXT
);

CREATE TABLE labels (
    label_id    INTEGER PRIMARY KEY,
    label       TEXT    NOT NULL UNIQUE,
    track_count INTEGER NOT NULL,
    logcounts   REAL    NOT NULL,
    lon         REAL    NOT NULL,
    lat         REAL    NOT NULL
);

CREATE TABLE track_knn (
    track_rowid    INTEGER NOT NULL,
    rank           INTEGER NOT NULL,
    neighbor_rowid INTEGER NOT NULL,
    score          REAL    NOT NULL,
    PRIMARY KEY (track_rowid, rank)
);

CREATE TABLE album_knn (
    album_rowid    INTEGER NOT NULL,
    rank           INTEGER NOT NULL,
    neighbor_rowid INTEGER NOT NULL,
    score          REAL    NOT NULL,
    PRIMARY KEY (album_rowid, rank)
);

CREATE TABLE artist_knn (
    artist_rowid   INTEGER NOT NULL,
    rank           INTEGER NOT NULL,
    neighbor_rowid INTEGER NOT NULL,
    score          REAL    NOT NULL,
    PRIMARY KEY (artist_rowid, rank)
);

CREATE TABLE label_knn (
    label_id     INTEGER NOT NULL,
    rank         INTEGER NOT NULL,
    neighbor_id  INTEGER NOT NULL,
    score        REAL    NOT NULL,
    PRIMARY KEY (label_id, rank)
);
"""

POST_DDL = """
CREATE INDEX idx_tracks_artist ON tracks(artist_rowid);
CREATE INDEX idx_tracks_album  ON tracks(album_rowid);
CREATE INDEX idx_tracks_label  ON tracks(label_id);
CREATE INDEX idx_albums_artist ON albums(artist_rowid);
"""


def _pivot_knn(key_vals, neighbor_mat, score_mat, knn_k):
    """Pivot wide KNN batch to flat rows, filtering self-matches, capped at knn_k per entity.

    key_vals:     (B,)      int64 — entity rowids for this batch
    neighbor_mat: (B, K+1)  int64 — neighbor rowids (may include self-match)
    score_mat:    (B, K+1)  float32 — cosine similarity scores

    Returns arrays: keys_f, ranks_f, neighbors_f, scores_f — each (M,)
    """
    _, K1 = neighbor_mat.shape

    keys_flat = np.repeat(key_vals, K1)  # (B*K1,)
    neighbors_flat = neighbor_mat.ravel()
    scores_flat = score_mat.ravel()

    # Filter self-matches
    valid = keys_flat != neighbors_flat
    keys_f = keys_flat[valid]
    neighbors_f = neighbors_flat[valid]
    scores_f = scores_flat[valid]

    if len(keys_f) == 0:
        return keys_f, np.empty(0, np.int32), neighbors_f, scores_f

    # Assign per-entity ranks (keys_f has contiguous runs from np.repeat)
    is_new = np.concatenate([[True], keys_f[1:] != keys_f[:-1]])
    run_starts = np.where(is_new)[0]
    run_lengths = np.diff(np.append(run_starts, len(keys_f)))
    run_start_per_el = np.repeat(run_starts, run_lengths)
    ranks_f = np.arange(len(keys_f), dtype=np.int32) - run_start_per_el.astype(np.int32)

    # Cap at knn_k neighbours per entity
    cap = ranks_f < knn_k
    return keys_f[cap], ranks_f[cap], neighbors_f[cap], scores_f[cap]


def _insert_knn(
    conn, table, pk_col, neighbor_col, keys_f, ranks_f, neighbors_f, scores_f
):
    conn.executemany(
        f"INSERT INTO {table} ({pk_col}, rank, {neighbor_col}, score) VALUES (?, ?, ?, ?)",
        zip(keys_f.tolist(), ranks_f.tolist(), neighbors_f.tolist(), scores_f.tolist()),
    )


def enrich_artists(df: pd.DataFrame, tracks_conn: sqlite3.Connection) -> pd.DataFrame:
    """Join artist popularity and first genre from T2M_TRACKS_DB.

    Merges on artist_rowid. Genre is NULL when no artist_genres row exists.
    """
    pop = pd.read_sql(
        "SELECT rowid AS artist_rowid, popularity FROM artists", tracks_conn
    )
    genres = pd.read_sql(
        "SELECT artist_rowid, MIN(genre) AS genre FROM artist_genres GROUP BY artist_rowid",
        tracks_conn,
    )
    df = df.merge(pop, on="artist_rowid", how="left")
    df = df.merge(genres, on="artist_rowid", how="left")
    return df


def enrich_albums(df: pd.DataFrame, tracks_conn: sqlite3.Connection) -> pd.DataFrame:
    """Join album metadata from T2M_TRACKS_DB.

    Adds: album_type, label (string), popularity, total_tracks, release_date,
    release_date_precision. The label string is used downstream to resolve label_id/lon/lat.
    Queries only the rowids present in df to avoid loading the full albums table.
    """
    rowids = df["album_rowid"].tolist()
    chunk_size = 5_000
    chunks = []
    for i in range(0, len(rowids), chunk_size):
        batch = rowids[i : i + chunk_size]
        placeholders = ",".join("?" * len(batch))
        chunks.append(
            pd.read_sql(
                f"SELECT rowid AS album_rowid, album_type, label, popularity,"
                f" total_tracks, release_date, release_date_precision"
                f" FROM albums WHERE rowid IN ({placeholders})",
                tracks_conn,
                params=batch,
            )
        )
    extra = pd.concat(chunks, ignore_index=True)
    return df.merge(extra, on="album_rowid", how="left")


def build_labels(conn, label_lookup: pd.DataFrame, geo_dir: Path) -> pd.DataFrame:
    """Build labels table; return the label DataFrame for downstream use."""
    geo = pd.read_parquet(geo_dir / "label_geo.parquet")  # columns: label, lon, lat
    df = label_lookup.rename(columns={"label_rowid": "label_id"})
    df = df.merge(geo, on="label", how="inner")
    df = df[["label_id", "label", "track_count", "logcounts", "lon", "lat"]]
    df.to_sql("labels", conn, if_exists="append", index=False)
    print(f"  [labels]  {len(df):,} rows")
    return df


def build_artists(
    conn,
    artist_lookup: pd.DataFrame,
    geo_dir: Path,
    tracks_conn: sqlite3.Connection | None,
) -> pd.DataFrame:
    """Build artists table; return artist_geo for albums/tracks enrichment."""
    artist_geo = pd.read_parquet(geo_dir / "artist_geo.parquet")
    df = artist_lookup.merge(artist_geo, on="artist_rowid", how="inner")
    if tracks_conn is not None:
        df = enrich_artists(df, tracks_conn)
    else:
        df["popularity"] = None
        df["genre"] = None
    df = df[
        ["artist_rowid", "artist_name", "track_count", "logcounts", "lon", "lat",
         "popularity", "genre"]
    ]
    df.to_sql("artists", conn, if_exists="append", index=False)
    print(f"  [artists] {len(df):,} rows")
    return artist_geo  # raw geo parquet (artist_rowid, lon, lat)


def build_albums(
    conn,
    album_lookup: pd.DataFrame,
    geo_dir: Path,
    artist_geo: pd.DataFrame,
    label_df: pd.DataFrame,
    tracks_conn: sqlite3.Connection | None,
) -> pd.DataFrame:
    """Build albums table; return album_geo for tracks enrichment."""
    album_geo = pd.read_parquet(geo_dir / "album_geo.parquet")

    df = album_lookup.merge(album_geo, on="album_rowid", how="inner")
    df = df.merge(
        artist_geo.rename(columns={"lon": "artist_lon", "lat": "artist_lat"}),
        on="artist_rowid",
        how="left",
    )

    if tracks_conn is not None:
        df = enrich_albums(df, tracks_conn)
    else:
        for col in ["album_type", "label", "popularity", "total_tracks",
                    "release_date", "release_date_precision"]:
            df[col] = None

    label_info = label_df[["label_id", "label", "lon", "lat"]].rename(
        columns={"lon": "label_lon", "lat": "label_lat"}
    )
    df = df.merge(label_info, on="label", how="left")
    df = df[
        [
            "album_rowid", "album_name", "track_count", "logcounts", "lon", "lat",
            "artist_rowid", "artist_name", "artist_lon", "artist_lat",
            "album_type", "label", "popularity", "total_tracks",
            "release_date", "release_date_precision",
            "label_id", "label_lon", "label_lat",
        ]
    ]
    df.to_sql("albums", conn, if_exists="append", index=False)
    print(f"  [albums]  {len(df):,} rows written")
    return album_geo  # raw geo parquet (album_rowid, lon, lat)


def build_tracks(
    conn, lookup_dir, geo_dir, artist_geo, album_geo, label_df, batch_size
):
    track_geo = pd.read_parquet(geo_dir / "track_geo.parquet")

    artist_geo_r = artist_geo.rename(columns={"lon": "artist_lon", "lat": "artist_lat"})
    album_geo_r = album_geo.rename(columns={"lon": "album_lon", "lat": "album_lat"})
    label_info = label_df[["label_id", "label", "lon", "lat"]].rename(
        columns={"lon": "label_lon", "lat": "label_lat"}
    )

    lookup_cols = [
        "track_rowid",
        "track_name",
        "track_popularity",
        "logcounts",
        "release_date",
        "artist_rowid",
        "artist_name",
        "album_rowid",
        "album_name",
        "label",
    ]
    pf = pq.ParquetFile(lookup_dir / "track_lookup.parquet")
    total = 0
    t0 = time.time()

    for batch in pf.iter_batches(batch_size=batch_size, columns=lookup_cols):
        df = (
            batch.to_pandas()
            .merge(track_geo, on="track_rowid", how="inner")
            .merge(artist_geo_r, on="artist_rowid", how="left")
            .merge(album_geo_r, on="album_rowid", how="left")
            .merge(label_info, on="label", how="left")
        )
        # Normalize empty label strings to NULL
        df["label"] = df["label"].replace("", None)

        df[
            [
                "track_rowid",
                "track_name",
                "track_popularity",
                "logcounts",
                "release_date",
                "lon",
                "lat",
                "artist_rowid",
                "artist_name",
                "artist_lon",
                "artist_lat",
                "album_rowid",
                "album_name",
                "album_lon",
                "album_lat",
                "label_id",
                "label",
                "label_lon",
                "label_lat",
            ]
        ].to_sql("tracks", conn, if_exists="append", index=False)

        total += len(df)
        rate = total / (time.time() - t0)
        print(f"  [tracks]  {total:>9,}  ({rate:,.0f} rows/s)", end="\r")

    print(f"\n  [tracks]  {total:,} rows  ({time.time()-t0:.1f}s)")


def build_knn_tables(conn, knn_dir, label_to_id, knn_k, batch_size):
    # Track, album, artist — int64 keys throughout
    for entity, pk_col, table in [
        ("track", "track_rowid", "track_knn"),
        ("album", "album_rowid", "album_knn"),
        ("artist", "artist_rowid", "artist_knn"),
    ]:
        knn_path = knn_dir / f"{entity}_knn.parquet"
        score_path = knn_dir / f"{entity}_knn_scores.parquet"
        if not knn_path.exists():
            print(f"  [{table}] skipped (not found: {knn_path})")
            continue

        schema = pq.read_schema(knn_path)
        k = sum(1 for n in schema.names if n.startswith("n"))  # k+1 neighbor cols
        n_cols = [f"n{i}" for i in range(k)]
        s_cols = [f"s{i}" for i in range(k)]

        total = 0
        t0 = time.time()
        for knn_b, score_b in zip(
            pq.ParquetFile(knn_path).iter_batches(batch_size=batch_size),
            pq.ParquetFile(score_path).iter_batches(batch_size=batch_size),
        ):
            kdf = knn_b.to_pandas()
            sdf = score_b.to_pandas()

            key_vals = kdf[pk_col].to_numpy(dtype=np.int64)
            neighbor_mat = kdf[n_cols].to_numpy(dtype=np.int64)
            score_mat = sdf[s_cols].to_numpy(dtype=np.float32)

            keys_f, ranks_f, neighbors_f, scores_f = _pivot_knn(
                key_vals, neighbor_mat, score_mat, knn_k
            )
            _insert_knn(
                conn,
                table,
                pk_col,
                "neighbor_rowid",
                keys_f,
                ranks_f,
                neighbors_f,
                scores_f,
            )

            total += len(keys_f)
            rate = total / max(time.time() - t0, 1e-6)
            print(f"  [{table}]  {total:>10,}  ({rate:,.0f} rows/s)", end="\r")

        conn.commit()
        print(f"\n  [{table}]  {total:,} rows  ({time.time()-t0:.1f}s)")

    # Labels — string keys, must map to label_id
    label_knn_path = knn_dir / "label_knn.parquet"
    label_score_path = knn_dir / "label_knn_scores.parquet"
    if not label_knn_path.exists():
        print("  [label_knn] skipped (not found)")
        return

    schema = pq.read_schema(label_knn_path)
    k = sum(1 for n in schema.names if n.startswith("n"))
    n_cols = [f"n{i}" for i in range(k)]
    s_cols = [f"s{i}" for i in range(k)]

    total = 0
    t0 = time.time()
    for knn_b, score_b in zip(
        pq.ParquetFile(label_knn_path).iter_batches(batch_size=batch_size),
        pq.ParquetFile(label_score_path).iter_batches(batch_size=batch_size),
    ):
        kdf = knn_b.to_pandas()
        sdf = score_b.to_pandas()

        # Map string keys → int64 label_id; unknown = -1
        key_strs = kdf["label"].values
        neighbor_mat_str = kdf[n_cols].values  # (B, k), dtype object (strings)
        score_mat = sdf[s_cols].to_numpy(dtype=np.float32)

        key_ids = np.array([label_to_id.get(s, -1) for s in key_strs], dtype=np.int64)
        neighbor_ids = np.array(
            [[label_to_id.get(n, -1) for n in row] for row in neighbor_mat_str],
            dtype=np.int64,
        )

        # Treat unmapped neighbors as self-match so _pivot_knn filters them
        neighbor_ids = np.where(
            neighbor_ids == -1, key_ids[:, np.newaxis], neighbor_ids
        )

        valid = key_ids != -1
        keys_f, ranks_f, neighbors_f, scores_f = _pivot_knn(
            key_ids[valid], neighbor_ids[valid], score_mat[valid], knn_k
        )
        _insert_knn(
            conn,
            "label_knn",
            "label_id",
            "neighbor_id",
            keys_f,
            ranks_f,
            neighbors_f,
            scores_f,
        )

        total += len(keys_f)
        rate = total / max(time.time() - t0, 1e-6)
        print(f"  [label_knn]  {total:>8,}  ({rate:,.0f} rows/s)", end="\r")

    conn.commit()
    print(f"\n  [label_knn]  {total:,} rows  ({time.time()-t0:.1f}s)")


def main():
    parser = argparse.ArgumentParser(
        description="Build SQLite DB from track2map parquet outputs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("T2M_DB"),
        help="Output SQLite path. $T2M_DB",
    )
    parser.add_argument(
        "--lookup-dir",
        default=os.environ.get("T2M_LOOKUP_DIR"),
        help="Dir with track_lookup.parquet. $T2M_LOOKUP_DIR",
    )
    parser.add_argument(
        "--geo-dir",
        default=os.environ.get("T2M_GEO_DIR"),
        help="Dir with *_geo.parquets. $T2M_GEO_DIR",
    )
    parser.add_argument(
        "--knn-dir",
        default=os.environ.get("T2M_KNN_DIR"),
        help="Dir with *_knn.parquets. $T2M_KNN_DIR",
    )
    parser.add_argument(
        "--tracks-db",
        default=os.environ.get("T2M_TRACKS_DB"),
        help="Path to spotify_clean.sqlite3 for artist/album enrichment. $T2M_TRACKS_DB (optional)",
    )
    parser.add_argument(
        "--knn-k",
        type=int,
        default=KNN_K_DEFAULT,
        help=f"Max neighbours to store per entity (default: {KNN_K_DEFAULT})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Rows per write batch (default: {BATCH_SIZE:,})",
    )
    args = parser.parse_args()

    for name, val in [
        ("--db", args.db),
        ("--lookup-dir", args.lookup_dir),
        ("--geo-dir", args.geo_dir),
        ("--knn-dir", args.knn_dir),
    ]:
        if val is None:
            raise ValueError(f"{name} not set (or corresponding env var)")

    lookup_dir = Path(args.lookup_dir)
    geo_dir = Path(args.geo_dir)
    knn_dir = Path(args.knn_dir)
    db_path = Path(args.db)

    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"DB         : {db_path}")
    print(f"Lookup dir : {lookup_dir}")
    print(f"Geo dir    : {geo_dir}")
    print(f"KNN dir    : {knn_dir}")
    print(f"Tracks DB  : {args.tracks_db or '(none — enrichment skipped)'}")
    print(f"knn_k      : {args.knn_k}")
    print()

    t_total = time.time()
    conn = sqlite3.connect(db_path)
    conn.executescript(DDL)

    tracks_conn = sqlite3.connect(args.tracks_db) if args.tracks_db else None

    print("Computing entity lookups from track_lookup.parquet...")
    lookup_cols = [
        "track_rowid", "artist_rowid", "artist_name",
        "album_rowid", "album_name", "label", "logcounts",
    ]
    track_lookup = pd.read_parquet(lookup_dir / "track_lookup.parquet", columns=lookup_cols)
    label_lookup = Labels.lookup(track_lookup)
    artist_lookup = Artists.lookup(track_lookup)
    album_lookup = Albums.lookup(track_lookup)
    print(
        f"  labels={len(label_lookup):,}  artists={len(artist_lookup):,}  albums={len(album_lookup):,}"
    )
    print()

    print("Building entity tables...")
    label_df = build_labels(conn, label_lookup, geo_dir)
    artist_geo = build_artists(conn, artist_lookup, geo_dir, tracks_conn)
    album_geo = build_albums(conn, album_lookup, geo_dir, artist_geo, label_df, tracks_conn)
    conn.commit()

    if tracks_conn is not None:
        tracks_conn.close()

    build_tracks(
        conn, lookup_dir, geo_dir, artist_geo, album_geo, label_df, args.batch_size
    )
    conn.commit()

    print("\nBuilding KNN tables...")
    label_to_id = dict(zip(label_df["label"], label_df["label_id"]))
    build_knn_tables(conn, knn_dir, label_to_id, args.knn_k, args.batch_size)

    print("\nCreating indexes...")
    conn.executescript(POST_DDL)
    conn.commit()
    conn.close()

    size_mb = db_path.stat().st_size / 1024 / 1024
    print(f"\nDone in {time.time()-t_total:.1f}s  —  {db_path}  ({size_mb:,.0f} MB)")


if __name__ == "__main__":
    main()
