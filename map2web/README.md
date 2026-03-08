# map

Interactive 2D slippy map of the Spotify track embedding space.

## Generating tiles

**1. Generate ndjson** (pipe straight into tippecanoe or save to file):

```sh
python scripts/prepare_geojson.py \
    ../umap/outs/umap/umap_2d_pure_bolt_nn100_md0d01_cosine.parquet \
    ../w2v/outs/track_lookup.parquet \
    > assets/tracks.ndjson
```

**2. Run tippecanoe:**

```sh
tippecanoe -e web/public/tiles -z10 -Z2 -pS --extend-zooms-if-still-dropping \
  --drop-densest-as-needed -l tracks --read-parallel --no-tile-compression \
  --force assets/tracks.ndjson
```

- `-z9 -Z2` — zoom range 2–9
- `-pS` — no tile size limit
- `-pf` — no feature count limit
- LOD is controlled per-feature via `tippecanoe-minzoom` (set from `track_popularity` in the script)

## Dev server

```sh
cd web && npm install && npm run dev
```
