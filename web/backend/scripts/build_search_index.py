"""Populate Meilisearch index from SQLite DB.

Indexes all four entity types (tracks, albums, artists, labels) from sick.db
into a single Meilisearch index for full-text search.
"""

import argparse
import os
import sqlite3
import time

import meilisearch
from meilisearch.index import Index

from src.utils import ALBUM
from src.utils import ARTIST
from src.utils import LABEL
from src.utils import TRACK

INDEX_SETTINGS = {
    "searchableAttributes": ["label", "artist_name", "album_name", "track_name"],
    "sortableAttributes": ["rank", "logcount"],
    "rankingRules": [
        "words",
        "typo",
        "proximity",
        "rank:asc",
        "logcount:desc",
        "sort",
        "attribute",
        "exactness",
    ],
}


def add_tracks(
    conn: sqlite3.Connection,
    index: Index,
    batch_size: int = 10_000,
):
    QUERY = f"SELECT {TRACK.key}, track_name, artist_name, logcount FROM {TRACK.table} WHERE searchable = 1"
    cursor = conn.execute(QUERY)
    total = 0
    t0 = time.time()
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        docs = [
            {
                "id": f"{TRACK.name}_{rowid}",
                "track_name": track_name,
                "artist_name": artist_name,
                "logcount": logcount,
                "rank": 3,
            }
            for (rowid, track_name, artist_name, logcount) in batch
        ]
        index.add_documents(docs, primary_key="id")
        total += len(batch)
        elapsed = time.time() - t0
        rate = total / elapsed if elapsed > 0 else 0
        print(f"  {total:>9,}  ({rate:,.0f} docs/s)", end="\r")
    print()


def add_albums(
    conn: sqlite3.Connection,
    index: Index,
    batch_size: int = 10_000,
):
    QUERY = f"SELECT {ALBUM.key}, album_name_norm, artist_name, logcount FROM {ALBUM.table} WHERE searchable = 1"
    cursor = conn.execute(QUERY)
    total = 0
    t0 = time.time()
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        docs = [
            {
                "id": f"{ALBUM.name}_{rowid}",
                "album_name": album_name_norm,
                "artist_name": artist_name,
                "logcount": logcount,
                "rank": 2,
            }
            for (rowid, album_name_norm, artist_name, logcount) in batch
        ]
        index.add_documents(docs, primary_key="id")
        total += len(batch)
        elapsed = time.time() - t0
        rate = total / elapsed if elapsed > 0 else 0
        print(f"  {total:>9,}  ({rate:,.0f} docs/s)", end="\r")
    print("")


def add_artists(
    conn: sqlite3.Connection,
    index: Index,
    batch_size: int = 10_000,
):
    QUERY = f"SELECT {ARTIST.key}, artist_name, logcount FROM {ARTIST.table} WHERE searchable = 1"
    cursor = conn.execute(QUERY)
    total = 0
    t0 = time.time()
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        docs = [
            {
                "id": f"{ARTIST.name}_{rowid}",
                "artist_name": artist_name,
                "logcount": logcount,
                "rank": 1,
            }
            for (rowid, artist_name, logcount) in batch
        ]
        index.add_documents(docs, primary_key="id")
        total += len(batch)
        elapsed = time.time() - t0
        rate = total / elapsed if elapsed > 0 else 0
        print(f"  {total:>9,}  ({rate:,.0f} docs/s)", end="\r")
    print()


def add_labels(
    conn: sqlite3.Connection,
    index: Index,
    batch_size: int = 10_000,
):
    QUERY = f"SELECT {LABEL.key}, label, logcount FROM {LABEL.table} WHERE searchable = 1"
    cursor = conn.execute(QUERY)
    total = 0
    t0 = time.time()
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        docs = [
            {
                "id": f"{LABEL.name}_{rowid}",
                "label": label,
                "logcount": logcount,
                "rank": 0,
            }
            for (rowid, label, logcount) in batch
        ]
        index.add_documents(docs, primary_key="id")
        total += len(batch)
        elapsed = time.time() - t0
        rate = total / elapsed if elapsed > 0 else 0
        print(f"  {total:>9,}  ({rate:,.0f} docs/s)", end="\r")
    print()


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Populate a Meilisearch index from the SQLite DB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("MEILI_URL"),
        metavar="URL",
        help="Meilisearch URL. $MEILI_URL",
    )
    parser.add_argument(
        "--uid",
        default=os.environ.get("MEILI_UID"),
        metavar="UID",
        help="Meilisearch index UID. $MEILI_UID",
    )
    parser.add_argument(
        "--key",
        default=os.environ.get("MEILI_KEY"),
        metavar="KEY",
        help="Meilisearch API key. $MEILI_KEY",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("SICK_DB"),
        metavar="PATH",
        help="Path to sick.db. $SICK_DB",
    )
    args = parser.parse_args(argv)

    if args.url is None:
        raise ValueError("--url / $MEILI_URL not set")
    if args.uid is None:
        raise ValueError("--uid / $MEILI_UID not set")
    if args.key is None:
        raise ValueError("--key / $MEILI_KEY not set")
    if args.db is None:
        raise ValueError("--db / $SICK_DB not set")

    client = meilisearch.Client(args.url, args.key)
    print("Cleaning index.")
    existing = {idx.uid for idx in client.get_indexes()["results"]}
    if args.uid in existing:
        client.delete_index(args.uid)
    index = client.index(args.uid)
    index.update_settings(INDEX_SETTINGS)

    with sqlite3.connect(args.db) as conn:
        print("Adding tracks..")
        add_tracks(conn, index)
        print("Adding albums..")
        add_albums(conn, index)
        print("Adding artists..")
        add_artists(conn, index)
        print("Adding labels..")
        add_labels(conn, index)

    print("Done.")


if __name__ == "__main__":
    main()
