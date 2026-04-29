# Usage

## Development setup

Install tippecanoe with:

```shell
git clone https://github.com/mapbox/tippecanoe.git
cd tippecanoe
make -j
make install
```

For generating the tiles run:

```shell
chmod +x build.sh
./build.sh
```

For local development with Docker Compose:

```shell
docker compose up
```

The frontend requests tiles directly from Martin at
`/sick-tiles/{z}/{x}/{y}`.

## Production setup

1. Create `config.prod.env`, you can use `config.prod.sample.env` for template.
2. Launch docker. The backend health check requires the search index to exist. To create the index you are supposed to
   manually launch `backend/scripts/build_search_index.py`.

```shell
docker compose -f docker-compose.prod.yml --env-file config.prod.env build
docker compose -f docker-compose.prod.yml --env-file config.prod.env up -d meilisearch
docker compose -f docker-compose.prod.yml --env-file config.prod.env run --rm --no-deps backend \
   uv run --no-sync python scripts/build_search_index.py
docker compose -f docker-compose.prod.yml --env-file config.prod.env up -d
```

## Utils

The script `scripts/tile_size_stats.py` produces a few tiles summary. Useful for keeping tiles size at check while experimenting with tippecanoe parameters.

Example:

```shell
python scripts/tile_size_stats.py --tiles /path/to/sick.mbtiles
```

## Linter

In frontend:

```shell
npx prettier src --write
npm run lint -- --fix
```

In backend:

```shell
black -l 120 .
isort --profile google .
```
