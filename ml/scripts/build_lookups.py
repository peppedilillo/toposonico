#!/usr/bin/env python3
"""Build enriched lookup tables from the training vocab and model checkpoint.

This script uses the checkpoint-scoped entity logic from ``src.entities`` to
determine which tracks, artists, albums, and labels are valid, then joins those
ids back to the tracks metadata SQLite database to recover human-readable
display fields.
"""

import argparse
import os
from pathlib import Path
import sqlite3
import time

import pandas as pd
import torch

from src.entities import Albums
from src.entities import Artists
from src.entities import Labels
from src.entities import Tracks

TEMP_TABLE_NAME = "build_lookups_tracks_tmp"
CHUNK_SIZE_DEFAULT = 50_000

TRACK_METADATA_QUERY = """
    WITH primary_artists AS (
        SELECT
            tt.track_rowid AS track_rowid,
            (
                SELECT artist_rowid
                FROM track_artists
                WHERE track_rowid = tt.track_rowid
                LIMIT 1
            ) AS artist_rowid
        FROM {temp_table} AS tt
    ), primary_genre AS (
        SELECT
            pa.artist_rowid AS artist_rowid,
            (
                SELECT genre
                FROM artist_genres
                WHERE artist_rowid = pa.artist_rowid
                LIMIT 1
            ) AS artist_genre
        FROM primary_artists AS pa
        GROUP BY pa.artist_rowid
    )
    SELECT
        tt.track_rowid               AS track_rowid,
        t.name                       AS track_name,
        t.popularity                 AS track_popularity,
        t.external_id_isrc           AS id_isrc,
        pa.artist_rowid              AS artist_rowid,
        pg.artist_genre              AS artist_genre,
        a.name                       AS artist_name,
        t.album_rowid                AS album_rowid,
        al.name                      AS album_name,
        al.label                     AS label,
        al.total_tracks              AS total_tracks,
        al.album_type                AS album_type,
        al.release_date              AS release_date,
        al.release_date_precision    AS release_date_precision
    FROM {temp_table} AS tt
    INNER JOIN primary_artists AS pa ON pa.track_rowid  = tt.track_rowid
    LEFT JOIN  primary_genre   AS pg ON pg.artist_rowid = pa.artist_rowid
    INNER JOIN tracks          AS t  ON t.rowid         = tt.track_rowid
    INNER JOIN albums          AS al ON t.album_rowid   = al.rowid
    INNER JOIN artists         AS a  ON a.rowid         = pa.artist_rowid
    ORDER BY tt.track_rowid
"""


def get_connection(database_path: Path) -> sqlite3.Connection:
    """Open a read-only SQLite connection to the given database path."""
    uri = f"file:{database_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def create_temp_track_table(conn: sqlite3.Connection, table_name: str = TEMP_TABLE_NAME) -> None:
    """Create (or replace) a temporary track-rowid table for the metadata join."""
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"""
        CREATE TEMP TABLE {table_name} (
            track_rowid INTEGER PRIMARY KEY
        )
        """)


def load_temp_track_table(
    conn: sqlite3.Connection,
    track_rowids: pd.Series,
    chunk_size: int,
    table_name: str = TEMP_TABLE_NAME,
) -> None:
    """Insert track rowids into the temp table in chunks, printing progress."""
    rows = [(int(track_rowid),) for track_rowid in track_rowids.tolist()]
    total = len(rows)
    started_at = time.time()
    for start in range(0, len(rows), chunk_size):
        conn.executemany(
            f"INSERT INTO {table_name} (track_rowid) VALUES (?)",
            rows[start : start + chunk_size],
        )
        done = min(start + chunk_size, total)
        elapsed = time.time() - started_at
        rate = done / elapsed if elapsed > 0 else 0.0
        print(f"  {done:>10,} / {total:,} staged  ({rate:,.0f} rows/s)", end="\r")
    print()


def fetch_track_metadata(
    conn: sqlite3.Connection,
    chunk_size: int,
    table_name: str = TEMP_TABLE_NAME,
) -> pd.DataFrame:
    """Fetch joined track metadata for rows staged in the temp table.

    Returns one row per track with name, popularity, ISRC, primary artist,
    album, label, and release date. Streams results in chunks to avoid
    materialising the full result set at once.
    """
    query = TRACK_METADATA_QUERY.format(temp_table=table_name)
    cursor = conn.execute(query)
    col_names = [desc[0] for desc in cursor.description]
    chunks = []
    total_rows = 0
    started_at = time.time()

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break

        chunk = pd.DataFrame.from_records(rows, columns=col_names)
        chunk["track_rowid"] = chunk["track_rowid"].astype("int64")
        chunk["artist_rowid"] = chunk["artist_rowid"].astype("int64")
        chunk["album_rowid"] = chunk["album_rowid"].astype("int64")
        chunk["track_popularity"] = chunk["track_popularity"].fillna(0).astype("uint8")
        chunks.append(chunk)

        total_rows += len(chunk)
        elapsed = time.time() - started_at
        rate = total_rows / elapsed if elapsed > 0 else 0.0
        print(f"  {total_rows:>10,} fetched  ({rate:,.0f} rows/s)", end="\r")

    print()
    if not chunks:
        return pd.DataFrame(columns=col_names)
    return pd.concat(chunks, ignore_index=True)


def build_track_lookup(
    t1_df: pd.DataFrame,
    model_dict: dict,
    track_meta: pd.DataFrame,
) -> pd.DataFrame:
    track_lookup = Tracks.lookup(t1_df, model_dict)
    label_ids = t1_df[t1_df["track_rowid"].isin(track_lookup["track_rowid"])][["track_rowid", "label_rowid"]].copy()
    label_ids["track_rowid"] = label_ids["track_rowid"].astype("int64")
    label_ids["label_rowid"] = label_ids["label_rowid"].astype("int32")
    track_lookup = track_lookup.merge(track_meta, on="track_rowid", how="inner")
    track_lookup = track_lookup.merge(label_ids, on="track_rowid", how="inner")
    return (
        track_lookup[
            [
                "track_rowid",
                "track_name",
                "artist_rowid",
                "artist_name",
                "album_rowid",
                "album_name",
                "track_popularity",
                "release_date",
                "id_isrc",
                "label",
                "label_rowid",
                "logcount",
            ]
        ]
        .sort_values("track_rowid")
        .reset_index(drop=True)
    )


def build_artist_lookup(
    t1_df: pd.DataFrame,
    model_dict: dict,
    track_meta: pd.DataFrame,
) -> pd.DataFrame:
    artist_lookup = Artists.lookup(t1_df, model_dict)
    artist_meta = (
        track_meta.groupby("artist_rowid", as_index=False)
        .agg(
            artist_name=("artist_name", "first"),
            artist_genre=("artist_genre", "first"),
        )
        .sort_values("artist_rowid")
    )
    artist_lookup = artist_lookup.merge(artist_meta, on="artist_rowid", how="inner")
    return (
        artist_lookup[["artist_rowid", "artist_name", "artist_genre", "logcount", "ntrack", "nalbum",]].sort_values("artist_rowid").reset_index(drop=True)
    )


def build_album_lookup(
    t1_df: pd.DataFrame,
    model_dict: dict,
    track_meta: pd.DataFrame,
) -> pd.DataFrame:
    album_lookup = Albums.lookup(t1_df, model_dict)
    enriched = track_meta.merge(
        t1_df[["track_rowid", "label_rowid"]].drop_duplicates("track_rowid"),
        on="track_rowid", how="left",
    )
    album_meta = (
        enriched.groupby("album_rowid", as_index=False)
        .agg(
            album_name=("album_name", "first"),
            artist_rowid=("artist_rowid", "first"),
            artist_name=("artist_name", "first"),
            label_rowid=("label_rowid", "first"),
            label=("label", "first"),
            album_type=("album_type", "first"),
            release_date=("release_date", "first"),
            release_date_precision=("release_date_precision", "first"),
            total_tracks=("total_tracks", "first"),
        )
        .sort_values("album_rowid")
    )
    album_lookup = album_lookup.merge(album_meta, on="album_rowid", how="inner")
    return (
        album_lookup[["album_rowid", "album_name", "artist_rowid", "label_rowid", "label", "artist_name", "album_type", "release_date", "release_date_precision", "logcount", "total_tracks",]]
        .sort_values("album_rowid")
        .reset_index(drop=True)
    )


def build_label_lookup(
    t1_df: pd.DataFrame,
    model_dict: dict,
    track_meta: pd.DataFrame,
) -> pd.DataFrame:
    label_lookup = Labels.lookup(t1_df, model_dict)
    label_meta = (
        t1_df[["track_rowid", "label_rowid"]]
        .merge(track_meta[["track_rowid", "label"]], on="track_rowid", how="inner")
        .groupby("label_rowid", as_index=False)
        .agg(label=("label", "first"))
        .sort_values("label_rowid")
    )
    label_lookup = label_lookup.merge(label_meta, on="label_rowid", how="inner")
    label_lookup["label_rowid"] = label_lookup["label_rowid"].astype("int32")
    return label_lookup[["label_rowid", "label", "logcount", "ntrack", "nalbum", "nartist"]].sort_values("label_rowid").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(
        description="Build enriched lookup parquets from t1 vocab and model checkpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("model", type=Path, help="Path to .pt model checkpoint file")
    parser.add_argument(
        "--database",
        default=os.environ.get("SICK_TRACKS_DB"),
        help="Path to track SQLite database. Set to `SICK_TRACKS_DB` by default.",
    )
    parser.add_argument(
        "--input",
        default=os.environ.get("SICK_T1_VOCAB"),
        help="Enriched training vocab path. Defaults to `SICK_T1_VOCAB`.",
    )
    parser.add_argument(
        "--track-output",
        default=os.environ.get("SICK_LOOKUP_TRACK"),
        help="Track lookup output path. Defaults to `SICK_LOOKUP_TRACK`.",
    )
    parser.add_argument(
        "--artist-output",
        default=os.environ.get("SICK_LOOKUP_ARTIST"),
        help="Artist lookup output path. Defaults to `SICK_LOOKUP_ARTIST`.",
    )
    parser.add_argument(
        "--album-output",
        default=os.environ.get("SICK_LOOKUP_ALBUM"),
        help="Album lookup output path. Defaults to `SICK_LOOKUP_ALBUM`.",
    )
    parser.add_argument(
        "--label-output",
        default=os.environ.get("SICK_LOOKUP_LABEL"),
        help="Label lookup output path. Defaults to `SICK_LOOKUP_LABEL`.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
        help=f"Rows per temp-table insert batch (default: {CHUNK_SIZE_DEFAULT:,})",
    )
    args = parser.parse_args()

    if args.database is None:
        raise ValueError(
            "No `SICK_TRACKS_DB` environment variable set. "
            "Either run with --database argument or define the environment variable."
        )
    if args.input is None:
        raise ValueError(
            "No `SICK_T1_VOCAB` environment variable set. "
            "Either run with --input argument or define the environment variable."
        )
    for path, envvar in [
        (args.track_output, "SICK_LOOKUP_TRACK"),
        (args.artist_output, "SICK_LOOKUP_ARTIST"),
        (args.album_output, "SICK_LOOKUP_ALBUM"),
        (args.label_output, "SICK_LOOKUP_LABEL"),
    ]:
        if path is None:
            raise ValueError(
                f"No `{envvar}` environment variable set. "
                f"Either run with the matching output argument or define the environment variable."
            )

    database_path = Path(args.database)
    input_path = Path(args.input)
    model_path = Path(args.model)
    if not database_path.exists():
        raise FileNotFoundError(f"Database not found: {database_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"Enriched training vocab not found: {input_path}")
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")

    output_paths = {
        "track": Path(args.track_output),
        "artist": Path(args.artist_output),
        "album": Path(args.album_output),
        "label": Path(args.label_output),
    }
    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Database   : {database_path}")
    print(f"Input      : {input_path}")
    print(f"Model      : {model_path}")
    print(f"Track out  : {output_paths['track']}")
    print(f"Artist out : {output_paths['artist']}")
    print(f"Album out  : {output_paths['album']}")
    print(f"Label out  : {output_paths['label']}")
    print()

    t0 = time.time()
    print("Loading enriched training vocab...")
    t1_df = pd.read_parquet(
        input_path,
        columns=[
            "track_rowid",
            "playlist_count",
            "artist_rowid",
            "album_rowid",
            "label_rowid",
        ],
    )
    t1_df["track_rowid"] = t1_df["track_rowid"].astype("int64")
    t1_df["playlist_count"] = t1_df["playlist_count"].astype("int32")
    t1_df["artist_rowid"] = t1_df["artist_rowid"].astype("int64")
    t1_df["album_rowid"] = t1_df["album_rowid"].astype("int64")
    t1_df["label_rowid"] = t1_df["label_rowid"].astype("Int32")
    print(f"  {len(t1_df):,} rows loaded")

    print("Loading checkpoint...")
    model_dict = torch.load(model_path, map_location="cpu", weights_only=False)
    print(f"  {len(model_dict['vocab']['track_rowid']):,} checkpoint rowids loaded")

    print("Building base lookup ids and logcount...")
    track_lookup_base = Tracks.lookup(t1_df, model_dict)
    artist_lookup_base = Artists.lookup(t1_df, model_dict)
    album_lookup_base = Albums.lookup(t1_df, model_dict)
    label_lookup_base = Labels.lookup(t1_df, model_dict)
    print(
        "  "
        f"{len(track_lookup_base):,} tracks, "
        f"{len(artist_lookup_base):,} artists, "
        f"{len(album_lookup_base):,} albums, "
        f"{len(label_lookup_base):,} labels"
    )

    print("Fetching track metadata from SQLite...")
    conn = get_connection(database_path)
    create_temp_track_table(conn)
    print("Staging checkpoint-supported track ids in SQLite temp table...")
    load_temp_track_table(conn, track_lookup_base["track_rowid"], args.chunk_size)
    track_meta = fetch_track_metadata(conn, args.chunk_size)
    conn.close()
    if len(track_meta) != len(track_lookup_base):
        raise RuntimeError(
            "Track metadata coverage mismatch: " f"expected {len(track_lookup_base):,} rows, got {len(track_meta):,}"
        )

    print("Joining metadata and writing lookup parquets...")
    build_track_lookup(t1_df, model_dict, track_meta).to_parquet(output_paths["track"], index=False)
    build_artist_lookup(t1_df, model_dict, track_meta).to_parquet(output_paths["artist"], index=False)
    build_album_lookup(t1_df, model_dict, track_meta).to_parquet(output_paths["album"], index=False)
    build_label_lookup(t1_df, model_dict, track_meta).to_parquet(output_paths["label"], index=False)

    print(f"Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
