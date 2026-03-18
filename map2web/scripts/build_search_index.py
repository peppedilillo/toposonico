"""Build the Meilisearch search index from the track2map SQLite DB.

One index, four entity types: track, album, artist, label.
Tracks are filtered to logcounts >= --track-threshold (default 2.0, ≈100 playlists).
Albums, artists, and labels are indexed in full.

Usage:
    source config.env && uv run python scripts/build_search_index.py [options]

Examples:
    uv run python scripts/build_search_index.py --dry-run
    uv run python scripts/build_search_index.py
    uv run python scripts/build_search_index.py --track-threshold 2.5

To delete an index and start fresh:
    curl -X DELETE http://localhost:7700/indexes/entities -H "Authorization: Bearer $MEILI_MASTER_KEY"
"""

import argparse
import os
import sqlite3
import sys
import time

import meilisearch


INDEX_SETTINGS = {
    "searchableAttributes": ["search_text", "track_name", "album_name", "artist_name", "label"],
    "filterableAttributes": ["entity_type"],
    "sortableAttributes": ["logcounts", "entity_rank"],
    "rankingRules": ["exactness", "entity_rank:asc", "words", "typo", "proximity", "attribute", "sort", "logcounts:desc"],
}

ENTITY_RANK = {"label": 0, "artist": 1, "album": 2, "track": 3}


def iter_docs(conn, entity, threshold=None):
    queries = {
        "track": (
            "SELECT track_rowid, track_name, artist_name, logcounts, lon, lat "
            "FROM tracks WHERE logcounts >= ?",
            (threshold,),
        ),
        "album": (
            "SELECT album_rowid, album_name, artist_name, logcounts, lon, lat FROM albums",
            (),
        ),
        "artist": (
            "SELECT artist_rowid, artist_name, logcounts, lon, lat FROM artists",
            (),
        ),
        "label": (
            "SELECT label_id, label, logcounts, lon, lat FROM labels",
            (),
        ),
    }
    sql, params = queries[entity]
    cursor = conn.execute(sql, params)
    cols   = [d[0] for d in cursor.description]

    for row in cursor:
        r   = dict(zip(cols, row))
        doc = {
            "entity_type": entity,
            "entity_rank": ENTITY_RANK[entity],
            "lon":         r["lon"],
            "lat":         r["lat"],
            "logcounts":   r["logcounts"],
        }

        if entity == "track":
            doc.update({
                "id":          f"track_{r['track_rowid']}",
                "rowid":       r["track_rowid"],
                "track_name":  r["track_name"],
                "artist_name": r["artist_name"],
                "search_text": f"{r['artist_name']} - {r['track_name']}",
            })
        elif entity == "album":
            doc.update({
                "id":          f"album_{r['album_rowid']}",
                "rowid":       r["album_rowid"],
                "album_name":  r["album_name"],
                "artist_name": r["artist_name"],
                "search_text": f"{r['artist_name']} - {r['album_name']}",
            })
        elif entity == "artist":
            doc.update({
                "id":          f"artist_{r['artist_rowid']}",
                "rowid":       r["artist_rowid"],
                "artist_name": r["artist_name"],
            })
        else:  # label
            doc.update({
                "id":    f"label_{r['label_id']}",
                "rowid": r["label_id"],
                "label": r["label"],
            })

        yield doc


def upload(index, docs, batch_size, label):
    batch, total, t0 = [], 0, time.time()
    for doc in docs:
        batch.append(doc)
        if len(batch) == batch_size:
            index.add_documents(batch, primary_key="id")
            total += len(batch)
            print(f"  {label}: {total:,}  ({time.time() - t0:.1f}s)", end="\r")
            batch = []
    if batch:
        index.add_documents(batch, primary_key="id")
        total += len(batch)
    print(f"  {label}: {total:,}  ({time.time() - t0:.1f}s)     ")
    return total


def main():
    parser = argparse.ArgumentParser(
        description="Build Meilisearch search index from the track2map SQLite DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db",              default=os.environ.get("T2M_DB"),
                        help="Path to track2map.db ($T2M_DB)")
    parser.add_argument("--track-threshold", type=float, default=2.0,
                        help="Min logcounts for tracks (default: 2.0 ≈ 100 playlists)")
    parser.add_argument("--index",           default=os.environ.get("MEILI_INDEX_NAME"),
                        help="Index name (overrides MEILI_INDEX_NAME)")
    parser.add_argument("--batch-size",      type=int, default=50_000)
    parser.add_argument("--dry-run",         action="store_true",
                        help="Print counts only, skip upload")
    parser.add_argument("--url",             default=os.environ.get("MEILI_URL"),
                        help="Meilisearch URL (overrides MEILI_URL)")
    parser.add_argument("--key",             default=os.environ.get("MEILI_MASTER_KEY"),
                        help="Meilisearch master key (overrides MEILI_MASTER_KEY)")
    args = parser.parse_args()

    for name, val in [("--db / $T2M_DB", args.db),
                      ("--index / $MEILI_INDEX_NAME", args.index),
                      ("--url / $MEILI_URL", args.url),
                      ("--key / $MEILI_MASTER_KEY", args.key)]:
        if not val:
            print(f"Error: {name} is not set. source config.env or pass the flag.")
            sys.exit(1)

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    if args.dry_run:
        for entity in ("track", "album", "artist", "label"):
            threshold = args.track_threshold if entity == "track" else None
            n = sum(1 for _ in iter_docs(conn, entity, threshold))
            print(f"  {n:>8,} {entity}s")
        conn.close()
        return

    print(f"Connecting to Meilisearch at {args.url} …")
    index = meilisearch.Client(args.url, args.key).index(args.index)

    print("Configuring index settings …")
    index.update_settings(INDEX_SETTINGS)

    print("Uploading …")
    total = 0
    for entity in ("track", "album", "artist", "label"):
        threshold = args.track_threshold if entity == "track" else None
        total += upload(index, iter_docs(conn, entity, threshold), args.batch_size, entity)

    conn.close()
    print(f"\nDone. {total:,} documents queued in index '{args.index}'.")
    print("Note: Meilisearch indexes asynchronously — allow a few minutes before querying.")


if __name__ == "__main__":
    main()
