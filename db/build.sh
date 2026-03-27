#!/usr/bin/env bash
# Build the full db pipeline: geo → knn → db
#
# Usage:
#   source config.env && bash build.sh
#   bash build.sh               # auto-sources config.env if SICK_OUT_DIR is unset
#   bash build.sh --from-knn    # skip geomap, run knn + db
#   bash build.sh --from-db     # skip geomap + knn, run db only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${SICK_OUT_DIR:-}" ]]; then
    if [[ -f "$SCRIPT_DIR/config.env" ]]; then
        # shellcheck source=/dev/null
        source "$SCRIPT_DIR/config.env"
    else
        echo "Error: SICK_OUT_DIR is not set and config.env was not found."
        echo "Run: cp config.sample.env config.env  # fill in paths, then source it"
        exit 1
    fi
fi

required_vars=(SICK_MANIFEST SICK_DB)
missing=()
for var in "${required_vars[@]}"; do
    [[ -z "${!var:-}" ]] && missing+=("$var")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: missing env vars: ${missing[*]}"
    exit 1
fi

RUN_GEO=1
RUN_KNN=1
RUN_DB=1

for arg in "$@"; do
    [[ "$arg" == "--from-knn" ]] && RUN_GEO=0
    [[ "$arg" == "--from-db"  ]] && RUN_GEO=0 && RUN_KNN=0
done

# ---------------------------------------------------------------------------
if [[ $RUN_GEO -eq 1 ]]; then
    echo "=== Step 1: build_geomap ==="
    uv run python "$SCRIPT_DIR/scripts/build_geomap.py"
else
    echo "=== Step 1: skipped ==="
fi

# ---------------------------------------------------------------------------
if [[ $RUN_KNN -eq 1 ]]; then
    echo ""
    echo "=== Step 2: build_knn ==="
    uv run python "$SCRIPT_DIR/scripts/build_knn.py"
else
    echo ""
    echo "=== Step 2: skipped ==="
fi

# ---------------------------------------------------------------------------
if [[ $RUN_DB -eq 1 ]]; then
    echo ""
    echo "=== Step 3: build_db ==="
    uv run python "$SCRIPT_DIR/scripts/build_db.py"
else
    echo ""
    echo "=== Step 3: skipped ==="
fi

echo ""
echo "Done."
