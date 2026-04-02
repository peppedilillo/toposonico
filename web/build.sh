#!/usr/bin/env bash
# Build vector tiles from precomputed GeoJSON exports.
#
# Usage:
#   source config.env && bash build.sh
#   bash build.sh  # auto-sources config.env if SICK_JSON_TRACK is unset
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${SICK_JSON_TRACK:-}" ]]; then
    if [[ -f "$SCRIPT_DIR/config.env" ]]; then
        # shellcheck source=/dev/null
        source "$SCRIPT_DIR/config.env"
    else
        echo "Error: SICK_JSON_TRACK is not set and config.env was not found."
        echo "Run: cp config.sample.env config.env  # fill in paths, then source it"
        exit 1
    fi
fi

required_vars=(
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
    echo "Error: missing env vars: ${missing[*]}"
    exit 1
fi

for path_var in "${required_vars[@]}"; do
    path="${!path_var}"
    if [[ ! -f "$path" ]]; then
        echo "Error: input file not found for $path_var: $path"
        exit 1
    fi
done

TILES_DIR="$SCRIPT_DIR/frontend/public/tiles"
rm -rf "$TILES_DIR"
mkdir -p "$TILES_DIR"

echo "=== Building tiles ==="
echo "Output: $TILES_DIR"

tippecanoe -e "$TILES_DIR" \
    -z12 -Z3 -pS -rg -g 1 -d 8 \
    --drop-densest-as-needed \
    --extend-zooms-if-still-dropping \
    --order-descending-by=logcount \
    --exclude=logcount \
    --read-parallel --force \
    --named-layer=tracks:"$SICK_JSON_TRACK" \
    --named-layer=albums:"$SICK_JSON_ALBUM" \
    --named-layer=artists:"$SICK_JSON_ARTIST" \
    --named-layer=labels:"$SICK_JSON_LABEL"

echo ""
echo "Done."
