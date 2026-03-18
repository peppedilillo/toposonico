#!/usr/bin/env bash
# Build the full tile pipeline: ndjson → tippecanoe → Meilisearch
#
# Usage:
#   source config.env && bash scripts/build_tiles.sh
#   bash scripts/build_tiles.sh               # auto-sources config.env if M2W_ROOT is unset
#   bash scripts/build_tiles.sh --from-tiles  # skip ndjson, run tippecanoe + search
#   bash scripts/build_tiles.sh --from-search # skip ndjson + tippecanoe, rebuild search only
#   bash scripts/build_tiles.sh --tiles-only  # skip ndjson + search, run tippecanoe only
#
# Requires: python (with prepare_geojson.py deps), tippecanoe, curl

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
    T2M_DB
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
FROM_TILES=0
FROM_SEARCH=0
for arg in "$@"; do
    [[ "$arg" == "--tiles-only" ]]  && TILES_ONLY=1
    [[ "$arg" == "--from-tiles" ]]  && FROM_TILES=1
    [[ "$arg" == "--from-search" ]] && FROM_SEARCH=1
done

RUN_NDJSON=1
RUN_TIPPECANOE=1
RUN_SEARCH=1

if   [[ $TILES_ONLY  -eq 1 ]]; then RUN_NDJSON=0; RUN_SEARCH=0
elif [[ $FROM_SEARCH -eq 1 ]]; then RUN_NDJSON=0; RUN_TIPPECANOE=0
elif [[ $FROM_TILES  -eq 1 ]]; then RUN_NDJSON=0
fi

if [[ $RUN_SEARCH -eq 1 ]]; then
    meili_missing=()
    for var in MEILI_INDEX_NAME MEILI_MASTER_KEY; do
        [[ -z "${!var:-}" ]] && meili_missing+=("$var")
    done
    if [[ ${#meili_missing[@]} -gt 0 ]]; then
        echo "Error: missing env vars for search step: ${meili_missing[*]}"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
if [[ $RUN_NDJSON -eq 1 ]]; then
    echo "=== Step 1: generate ndjson ==="
    for entity in track album artist label; do
        echo "--- $entity ---"
        python "$SCRIPT_DIR/prepare_geojson.py" "$entity"
    done
else
    echo "=== Step 1: skipped ==="
fi

# ---------------------------------------------------------------------------
if [[ $RUN_TIPPECANOE -eq 1 ]]; then
    echo ""
    echo "=== Step 2: tippecanoe ==="
    echo "Output: $TILES_DIR"
    echo ""

    tippecanoe -e "$TILES_DIR" \
        -z11 -Z2 -pS -rg -g 1 -d 8 \
        --drop-densest-as-needed \
        --order-descending-by=logcounts \
        --exclude=logcounts \
        --exclude=track_count \
        --read-parallel --force \
        --named-layer=tracks:"$M2W_GEOJSON_DIR/track.ndjson" \
        --named-layer=albums:"$M2W_GEOJSON_DIR/album.ndjson" \
        --named-layer=artists:"$M2W_GEOJSON_DIR/artist.ndjson" \
        --named-layer=labels:"$M2W_GEOJSON_DIR/label.ndjson"
else
    echo ""
    echo "=== Step 2: skipped ==="
fi

# ---------------------------------------------------------------------------
if [[ $RUN_SEARCH -eq 1 ]]; then
    echo ""
    echo "=== Step 3: rebuild search index ==="
    meili_url="${MEILI_URL:-http://localhost:7700}"
    curl -sf -X DELETE "$meili_url/indexes/$MEILI_INDEX_NAME" \
        -H "Authorization: Bearer $MEILI_MASTER_KEY" > /dev/null
    echo "  index '$MEILI_INDEX_NAME' deleted"
    uv run python "$SCRIPT_DIR/build_search_index.py"
fi

echo ""
echo "Done."
