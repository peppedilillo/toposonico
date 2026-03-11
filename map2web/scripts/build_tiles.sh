#!/usr/bin/env bash
# Build the full tile pipeline: ndjson → tippecanoe → web/public/tiles/
#
# Usage:
#   source config.env && bash scripts/build_tiles.sh
#   bash scripts/build_tiles.sh          # auto-sources config.env if M2W_ROOT is unset
#   bash scripts/build_tiles.sh --tiles-only  # skip ndjson generation, run tippecanoe only
#
# Requires: python (with prepare_geojson.py deps), tippecanoe

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAP2WEB_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -z "${M2W_ROOT:-}" ]]; then
    if [[ -f "$MAP2WEB_DIR/config.env" ]]; then
        # shellcheck source=/dev/null
        source "$MAP2WEB_DIR/config.env"
    else
        echo "Error: M2W_ROOT is not set and config.env was not found."
        echo "Run: cp config.env.sample config.env  # fill in paths, then source it"
        exit 1
    fi
fi

required_vars=(
    M2W_TRACK_GEO   M2W_TRACK_LOOKUP
    M2W_ALBUM_GEO   M2W_ALBUM_LOOKUP
    M2W_ARTIST_GEO  M2W_ARTIST_LOOKUP
    M2W_LABEL_GEO   M2W_LABEL_LOOKUP
    M2W_GEOJSON_DIR
)
missing=()
for var in "${required_vars[@]}"; do
    [[ -z "${!var:-}" ]] && missing+=("$var")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: missing env vars: ${missing[*]}"
    exit 1
fi

TILES_DIR="$MAP2WEB_DIR/web/public/tiles"
TILES_ONLY=0
for arg in "$@"; do [[ "$arg" == "--tiles-only" ]] && TILES_ONLY=1; done

# ---------------------------------------------------------------------------
if [[ $TILES_ONLY -eq 0 ]]; then
    echo "=== Step 1: generate ndjson ==="
    for entity in track album artist label; do
        echo "--- $entity ---"
        python "$SCRIPT_DIR/prepare_geojson.py" "$entity"
    done
else
    echo "=== Step 1: skipped (--tiles-only) ==="
fi

# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: tippecanoe ==="
echo "Output: $TILES_DIR"
echo ""

tippecanoe -e "$TILES_DIR" \
    -z11 -Z2 -pS -rg -g 1 -d 8 \
    --drop-densest-as-needed \
    --order-descending-by=popularity \
    --read-parallel --force \
    --named-layer=tracks:"$M2W_GEOJSON_DIR/track.ndjson" \
    --named-layer=albums:"$M2W_GEOJSON_DIR/album.ndjson" \
    --named-layer=artists:"$M2W_GEOJSON_DIR/artist.ndjson" \
    --named-layer=labels:"$M2W_GEOJSON_DIR/label.ndjson"

echo ""
echo "Done. Tiles at $TILES_DIR"
