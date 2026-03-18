# map2web

Interactive 2D slippy map of the Spotify track embedding space.

Tracks (and albums, artists, labels) are placed on a tile map using their UMAP
coordinates. Pan, zoom, and click to explore the latent space.

## Setup

```sh
uv sync
cd web && npm install
```

Format JS with Prettier: `cd web && npx prettier --write src/`

## Environment

```sh
cp config.env.sample config.env
# fill in all paths
source config.env
```

| Variable | Description |
|---|---|
| `M2W_ROOT` | Root of this directory (`map2web/`) |
| `M2W_TRACK_GEO` | Path to track geo-parquet (UMAP + lon/lat) |
| `M2W_TRACK_LOOKUP` | Path to `track_lookup.parquet` |
| `M2W_ALBUM_GEO` | Path to album geo-parquet |
| `M2W_ALBUM_LOOKUP` | Path to `album_lookup.parquet` |
| `M2W_ARTIST_GEO` | Path to artist geo-parquet |
| `M2W_ARTIST_LOOKUP` | Path to `artist_lookup.parquet` |
| `M2W_LABEL_GEO` | Path to label geo-parquet |
| `M2W_LABEL_LOOKUP` | Path to `label_lookup.parquet` |
| `M2W_GEOJSON_DIR` | Output directory for `.ndjson` files (e.g. `$M2W_ROOT/assets/`) |
| `MEILI_MASTER_KEY` | Meilisearch master key |
| `MEILI_INDEX_NAME` | Meilisearch index name (e.g. `entities`) |
| `MEILI_URL` | Meilisearch URL (default: `http://localhost:7700`) |

## Pipeline

### Step 1 — Generate ndjson

Run once per entity type. Output goes to `$M2W_GEOJSON_DIR/{entity}.ndjson`.

```sh
python scripts/prepare_geojson.py track
python scripts/prepare_geojson.py album
python scripts/prepare_geojson.py artist
python scripts/prepare_geojson.py label
```

Paths are resolved from `M2W_*` env vars. You can also override per-run:

```sh
python scripts/prepare_geojson.py track \
    --geo /path/to/track_geo.parquet \
    --lookup /path/to/track_lookup.parquet \
    --output /path/to/track.ndjson
```

### Step 2 — Build tiles with tippecanoe

Run the full pipeline (ndjson + tiles):

```sh
bash scripts/build_tiles.sh
```

If the `.ndjson` files are already up to date, skip step 1:

```sh
bash scripts/build_tiles.sh --tiles-only
```

### Step 3 — Build the search index

Start Meilisearch, then index all entities (tracks filtered to `logcounts >= 2.0`):

```sh
docker compose up -d
uv run python scripts/build_search_index.py
```

To wipe and rebuild from scratch:

```sh
curl -X DELETE http://localhost:7700/indexes/entities -H "Authorization: Bearer $MEILI_MASTER_KEY"
uv run python scripts/build_search_index.py
```

Indexing is asynchronous — allow a few minutes before querying.

## Running in dev

Three processes, each in its own terminal:

```sh
# 1. Search engine
docker compose up

# 2. API backend
source config.env && uv run fastapi dev backend/main.py

# 3. Frontend (proxies /api/* to :8000)
cd web && npm run dev -- --host
```

Open `http://localhost:5173`.

## Extras

Run prettier for linter with `npx prettier web/src --write`
