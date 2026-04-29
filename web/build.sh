#!/usr/bin/env bash
# Build vector tiles from precomputed GeoJSON exports.
#
# Usage:
#   source config.env && bash build.sh
#   bash build.sh  # auto-sources config.env if SICK_JSON_TRACK is unset
#
# Incremental workflow:
#   Comment out any layer build block you want to reuse from a previous run.
#   The script no longer wipes SICK_TILES_BUILD_DIR, so existing layer MBTiles
#   remain available for tile-join.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
    echo "Error: $*" >&2
    exit 1
}

if [[ -z "${SICK_JSON_TRACK:-}" ]]; then
    if [[ -f "$SCRIPT_DIR/config.env" ]]; then
        # shellcheck source=/dev/null
        source "$SCRIPT_DIR/config.env"
    else
        die "SICK_JSON_TRACK is not set and config.env was not found."
    fi
fi

required_vars=(
    SICK_JSON_TRACK
    SICK_JSON_ALBUM
    SICK_JSON_ARTIST
    SICK_JSON_LABEL
    SICK_MAX_ZOOM
    SICK_BASE_ZOOM_TRACK
    SICK_BASE_ZOOM_ALBUM
    SICK_BASE_ZOOM_ARTIST
    SICK_BASE_ZOOM_LABEL
    SICK_TILES_MB
    SICK_TILES_BUILD_DIR
)
required_files=(
    SICK_JSON_TRACK
    SICK_JSON_ALBUM
    SICK_JSON_ARTIST
    SICK_JSON_LABEL
)

missing=()
for var in "${required_vars[@]}"; do
    [[ -z "${!var:-}" ]] && missing+=("$var")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    die "missing env vars: ${missing[*]}"
fi

for path_var in "${required_files[@]}"; do
    path="${!path_var}"
    if [[ ! -f "$path" ]]; then
        die "input file not found for $path_var: $path"
    fi
done

mkdir -p "$SICK_TILES_BUILD_DIR"
mkdir -p "$(dirname "$SICK_TILES_MB")"

echo "=== Building tiles ==="
echo "Scratch dir: $SICK_TILES_BUILD_DIR"
echo "Output: $SICK_TILES_MB"

tippecanoe \
    -o "$SICK_TILES_BUILD_DIR/tracks.mbtiles" \
    --full-detail=7 --low-detail=7 \
    --maximum-zoom="$SICK_MAX_ZOOM" --minimum-zoom=5 --base-zoom="$SICK_BASE_ZOOM_TRACK" \
    --drop-rate=1.8 --drop-densest-as-needed \
    --read-parallel --force \
    --layer=tracks \
    "$SICK_JSON_TRACK"

tippecanoe \
    -o "$SICK_TILES_BUILD_DIR/labels.mbtiles" \
    --full-detail=7 --low-detail=7 \
    --maximum-zoom="$SICK_MAX_ZOOM" --minimum-zoom=5 --base-zoom="$SICK_BASE_ZOOM_LABEL" \
    --drop-rate=2.05 --drop-densest-as-needed \
    --read-parallel --force \
    --layer=labels \
    "$SICK_JSON_LABEL"

tippecanoe \
    -o "$SICK_TILES_BUILD_DIR/albums.mbtiles" \
    --full-detail=7 --low-detail=7 \
    --maximum-zoom="$SICK_MAX_ZOOM" --minimum-zoom=5 --base-zoom="$SICK_BASE_ZOOM_ALBUM" \
    --drop-rate=2.27 --drop-densest-as-needed \
    --read-parallel --force \
    --layer=albums \
    "$SICK_JSON_ALBUM"

tippecanoe \
    -o "$SICK_TILES_BUILD_DIR/artists.mbtiles" \
    --full-detail=7 --low-detail=7 \
    --maximum-zoom="$SICK_MAX_ZOOM" --minimum-zoom=5 --base-zoom="$SICK_BASE_ZOOM_ARTIST" \
    --drop-rate=2.36 --drop-densest-as-needed \
    --read-parallel --force \
    --layer=artists \
    "$SICK_JSON_ARTIST"

echo ""
echo "=== Merging tiles ==="
tile-join \
    -o "$SICK_TILES_MB" \
    -pk \
    -f \
    "$SICK_TILES_BUILD_DIR/tracks.mbtiles" \
    "$SICK_TILES_BUILD_DIR/albums.mbtiles" \
    "$SICK_TILES_BUILD_DIR/artists.mbtiles" \
    "$SICK_TILES_BUILD_DIR/labels.mbtiles"

echo ""
echo "Done."
