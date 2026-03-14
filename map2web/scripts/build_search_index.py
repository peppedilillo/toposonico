"""Build the Meilisearch search index from ndjson files produced by prepare_geojson.py.

One index, four entity types: track, album, artist, label.
Tracks are filtered to logcounts >= --track-threshold (default 2.0, ≈100 playlists).
Albums, artists, and labels are indexed in full.

Field names are identical to tile feature properties, guaranteeing consistency with
the map layer tooltips.

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
import json
import os
import sys
import time
from pathlib import Path

import meilisearch


INDEX_SETTINGS = {
    "searchableAttributes": ["search_text", "track_name", "album_name", "artist_name", "label"],
    "filterableAttributes": ["entity_type"],
    "sortableAttributes": ["logcounts", "entity_rank"],
    "rankingRules": ["exactness", "entity_rank:asc", "words", "typo", "proximity", "attribute", "sort", "logcounts:desc"],
}

ENTITY_RANK = {"label": 0, "artist": 1, "album": 2, "track": 3}


def iter_docs(path: Path, entity: str, threshold: float | None = None):
    with open(path) as f:
        for i, line in enumerate(f):
            feature = json.loads(line)
            props = feature["properties"]
            lon, lat = feature["geometry"]["coordinates"]

            if threshold is not None and props.get("logcounts", 0) < threshold:
                continue

            doc = {
                "entity_type": entity,
                "entity_rank": ENTITY_RANK[entity],
                "lon": lon,
                "lat": lat,
                "logcounts": props["logcounts"],
            }

            if entity == "track":
                doc.update({
                    "id": f"track_{props['track_rowid']}",
                    "rowid": props["track_rowid"],
                    "track_name": props["track_name"],
                    "artist_name": props["artist_name"],
                    "search_text": f"{props['artist_name']} - {props['track_name']}",
                })
            elif entity == "album":
                doc.update({
                    "id": f"album_{props['album_rowid']}",
                    "rowid": props["album_rowid"],
                    "album_name": props["album_name"],
                    "artist_name": props["artist_name"],
                    "search_text": f"{props['artist_name']} - {props['album_name']}",
                })
            elif entity == "artist":
                doc.update({
                    "id": f"artist_{props['artist_rowid']}",
                    "rowid": props["artist_rowid"],
                    "artist_name": props["artist_name"],
                })
            else:  # label
                doc.update({
                    "id": f"label_{props['label_rowid']}",
                    "rowid": props["label_rowid"],
                    "label": props["label"],
                })

            yield doc


def upload(index, docs, batch_size: int, label: str) -> int:
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


def env_path(var: str) -> Path:
    val = os.environ.get(var)
    if not val:
        print(f"Error: {var} is not set. source config.env or pass explicit flags.")
        sys.exit(1)
    return Path(val)


def main():
    parser = argparse.ArgumentParser(
        description="Build Meilisearch search index from ndjson files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--track-threshold", type=float, default=2.0,
                        help="Min logcounts for tracks (default: 2.0 ≈ 100 playlists)",)
    parser.add_argument("--index", default=os.environ.get("MEILI_INDEX_NAME"),
                        help="Index name (overrides MEILI_INDEX_NAME)",)
    parser.add_argument("--batch-size", type=int, default=50_000,)
    parser.add_argument("--dry-run", action="store_true", help="Print counts only, skip upload",)
    parser.add_argument("--url", default=os.environ.get("MEILI_URL"), help="Meilisearch URL (overrides MEILI_URL)",)
    parser.add_argument("--key", default=os.environ.get("MEILI_MASTER_KEY"), help="Meilisearch master key (overrides MEILI_MASTER_KEY)",)
    args = parser.parse_args()


    if args.index is None:
        raise ValueError(
            "No `MEILI_INDEX_NAME` environment variable set. "
            "Either run with --index argument or define the environment variable."
        )
    if args.url is None:
        raise ValueError(
            "No `MELILI_URL` environment variable set. "
            "Either run with --url argument or define the environment variable."
        )
    if args.key is None:
        raise ValueError(
            "No `MEILI_MASTER_KEY` environment variable set. "
            "Either run with --key argument or define the environment variable."
        )


    geojson_dir = env_path("M2W_GEOJSON_DIR")

    entities = [
        ("track",  geojson_dir / "track.ndjson",  args.track_threshold),
        ("album",  geojson_dir / "album.ndjson",  None),
        ("artist", geojson_dir / "artist.ndjson", None),
        ("label",  geojson_dir / "label.ndjson",  None),
    ]

    if args.dry_run:
        for entity, path, threshold in entities:
            n = sum(1 for _ in iter_docs(path, entity, threshold))
            print(f"  {n:>8,} {entity}s")
        return

    print(f"Connecting to Meilisearch at {args.url} …")
    index = meilisearch.Client(args.url, args.key).index(args.index)

    print("Configuring index settings …")
    index.update_settings(INDEX_SETTINGS)

    print("Uploading …")
    total = 0
    for entity, path, threshold in entities:
        total += upload(index, iter_docs(path, entity, threshold), args.batch_size, entity)

    print(f"\nDone. {total:,} documents queued in index '{args.index}'.")
    print("Note: Meilisearch indexes asynchronously — allow a few minutes before querying.")


if __name__ == "__main__":
    main()
