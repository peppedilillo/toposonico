"""Build the Meilisearch search index from geo-parquets + lookup parquets.

One index, four entity types: track, album, artist, label.
Tracks are filtered to logcounts >= --track-threshold (default 2.0, ≈100 playlists).
Albums, artists, and labels are indexed in full.

Usage:
    source config.env && uv run python scripts/build_search_index.py [options]

Examples:
    uv run python scripts/build_search_index.py --dry-run
    uv run python scripts/build_search_index.py
    uv run python scripts/build_search_index.py --track-threshold 2.5 --index entities_v2

To delete an index and start fresh:
    curl -X DELETE http://localhost:7700/indexes/entities -H "Authorization: Bearer $MEILI_MASTER_KEY"
"""

import argparse
import os
import sys
import time
from pathlib import Path

import meilisearch
import pandas as pd


INDEX_SETTINGS = {
    "searchableAttributes": ["search_text", "track_name", "album_name", "artist_name", "label"],
    "filterableAttributes": ["entity_type"],
    "sortableAttributes": ["logcounts", "entity_rank"],
    "rankingRules": ["exactness", "entity_rank:asc", "words", "typo", "proximity", "attribute", "sort",  "logcounts:desc"],
}

ENTITY_RANK = {"label": 0, "artist": 1, "album": 2, "track": 3}


def load_entity(geo_path: Path, lookup_path: Path, key: str, lookup_cols: list[str]) -> pd.DataFrame:
    geo = pd.read_parquet(geo_path, columns=[key, "lon", "lat"])
    lookup = pd.read_parquet(lookup_path, columns=lookup_cols)
    return geo.merge(lookup, on=key, how="inner")


def build_docs_track(df: pd.DataFrame) -> list[dict]:
    docs = []
    for row in df.itertuples(index=False):
        docs.append({
            "id": f"track_{row.track_rowid}",
            "entity_type": "track",
            "track_name": row.track_name,
            "artist_name": row.artist_name,
            "search_text": f"{row.artist_name} - {row.track_name}",
            "rowid": int(row.track_rowid),
            "entity_rank": ENTITY_RANK["track"],
            "lon": round(float(row.lon), 6),
            "lat": round(float(row.lat), 6),
            "logcounts": round(float(row.logcounts), 3),
        })
    return docs


def build_docs_album(df: pd.DataFrame) -> list[dict]:
    docs = []
    for row in df.itertuples(index=False):
        docs.append({
            "id": f"album_{row.album_rowid}",
            "entity_type": "album",
            "album_name": row.album_name,
            "artist_name": row.artist_name,
            "search_text": f"{row.artist_name} - {row.album_name}",
            "rowid": int(row.album_rowid),
            "entity_rank": ENTITY_RANK["album"],
            "lon": round(float(row.lon), 6),
            "lat": round(float(row.lat), 6),
            "logcounts": round(float(row.logcounts), 3),
        })
    return docs


def build_docs_artist(df: pd.DataFrame) -> list[dict]:
    docs = []
    for row in df.itertuples(index=False):
        docs.append({
            "id": f"artist_{row.artist_rowid}",
            "entity_type": "artist",
            "artist_name": row.artist_name,
            "rowid": int(row.artist_rowid),
            "entity_rank": ENTITY_RANK["artist"],
            "lon": round(float(row.lon), 6),
            "lat": round(float(row.lat), 6),
            "logcounts": round(float(row.logcounts), 3),
        })
    return docs


def build_docs_label(df: pd.DataFrame, id_offset: int) -> list[dict]:
    docs = []
    for i, row in enumerate(df.itertuples(index=False)):
        docs.append({
            "id": f"label_{id_offset + i}",
            "entity_type": "label",
            "label": row.label,
            "entity_rank": ENTITY_RANK["label"],
            "lon": round(float(row.lon), 6),
            "lat": round(float(row.lat), 6),
            "logcounts": round(float(row.logcounts), 3),
        })
    return docs


def upload(index, docs: list[dict], batch_size: int, label: str) -> None:
    total = len(docs)
    t0 = time.time()
    for start in range(0, total, batch_size):
        batch = docs[start:start + batch_size]
        index.add_documents(batch, primary_key="id")
        end = min(start + batch_size, total)
        elapsed = time.time() - t0
        print(f"  {label}: {end:>8,} / {total:,}  ({elapsed:.1f}s)", end="\r")
    print(f"  {label}: {total:>8,} / {total:,}  ({time.time() - t0:.1f}s)     ")


def env_path(var: str) -> Path:
    val = os.environ.get(var)
    if not val:
        print(f"Error: {var} is not set. source config.env or pass explicit flags.")
        sys.exit(1)
    return Path(val)


def main():
    parser = argparse.ArgumentParser(
        description="Build Meilisearch search index",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--track-threshold", type=float, default=2.0,
                        help="Min logcounts for tracks (default: 2.0 ≈ 100 playlists)")
    parser.add_argument(
        "--index",
        default=os.environ.get("MEILI_INDEX_NAME"),
        help="Index name (default: entities)",
    )
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print counts only, skip upload")
    parser.add_argument("--url", default=None, help="Meilisearch URL (overrides MEILI_URL)")
    parser.add_argument("--key", default=None, help="Meilisearch master key (overrides MEILI_MASTER_KEY)")
    args = parser.parse_args()

    if args.index is None:
        raise ValueError(
            "No `MEILI_INDEX_NAME` environment variable set. "
            "Either run with --index argument or define the environment variable."
        )

    meili_url = args.url or os.environ.get("MEILI_URL", "http://localhost:7700")
    meili_key = args.key or os.environ.get("MEILI_MASTER_KEY")

    print("Loading track data …")
    track_df = load_entity(
        env_path("M2W_TRACK_GEO"), env_path("M2W_TRACK_LOOKUP"),
        "track_rowid", ["track_rowid", "track_name", "artist_name", "logcounts"],
    )
    track_df = track_df.dropna(subset=["track_name", "artist_name"])
    track_df = track_df[track_df["logcounts"] >= args.track_threshold]
    print(f"  {len(track_df):,} tracks (logcounts >= {args.track_threshold})")

    print("Loading album data …")
    album_df = load_entity(
        env_path("M2W_ALBUM_GEO"), env_path("M2W_ALBUM_LOOKUP"),
        "album_rowid", ["album_rowid", "album_name", "artist_name", "logcounts"],
    )
    album_df = album_df.dropna(subset=["album_name", "artist_name"])
    print(f"  {len(album_df):,} albums")

    print("Loading artist data …")
    artist_df = load_entity(
        env_path("M2W_ARTIST_GEO"), env_path("M2W_ARTIST_LOOKUP"),
        "artist_rowid", ["artist_rowid", "artist_name", "logcounts"],
    )
    artist_df = artist_df.dropna(subset=["artist_name"])
    print(f"  {len(artist_df):,} artists")

    print("Loading label data …")
    label_df = load_entity(
        env_path("M2W_LABEL_GEO"), env_path("M2W_LABEL_LOOKUP"),
        "label", ["label", "logcounts"],
    )
    label_df = label_df.dropna(subset=["label"])
    print(f"  {len(label_df):,} labels")

    total = len(track_df) + len(album_df) + len(artist_df) + len(label_df)
    print(f"\nTotal documents: {total:,}")

    if args.dry_run:
        print("Dry run — skipping upload.")
        return

    print(f"\nConnecting to Meilisearch at {meili_url} …")
    client = meilisearch.Client(meili_url, meili_key)
    index = client.index(args.index)

    print("Configuring index settings …")
    index.update_settings(INDEX_SETTINGS)

    print("\nUploading …")
    upload(index, build_docs_track(track_df), args.batch_size, "tracks")
    upload(index, build_docs_album(album_df), args.batch_size, "albums")
    upload(index, build_docs_artist(artist_df), args.batch_size, "artists")
    upload(index, build_docs_label(label_df, id_offset=0), args.batch_size, "labels")

    print(f"\nDone. {total:,} documents queued in index '{args.index}'.")
    print("Note: Meilisearch indexes asynchronously — allow a few minutes before querying.")


if __name__ == "__main__":
    main()