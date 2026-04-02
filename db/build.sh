#!/usr/bin/env bash
# Build the db pipeline: geo → db → sim
#
# Usage:
#   source config.env && bash build.sh
#   bash build.sh                 # auto-sources config.env if SICK_OUT_DIR is unset
#   bash build.sh --from-db       # skip geo, run from db onward
#   bash build.sh --from-sim      # skip geo + db, run sim only
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

RUN_GEO=1
RUN_DB=1
RUN_SIM=1

for arg in "$@"; do
    case "$arg" in
        --from-db)
            RUN_GEO=0
            ;;
        --from-sim)
            RUN_GEO=0
            RUN_DB=0
            ;;
        *)
            echo "Error: unknown argument: $arg"
            exit 1
            ;;
    esac
done

required_vars=(SICK_MANIFEST SICK_DB)
if [[ $RUN_SIM -eq 1 ]]; then
    required_vars+=(SICK_INDEX_FAISS_TRACK SICK_INDEX_FAISS_ALBUM SICK_INDEX_FAISS_ARTIST SICK_INDEX_FAISS_LABEL)
fi

missing=()
for var in "${required_vars[@]}"; do
    [[ -z "${!var:-}" ]] && missing+=("$var")
done
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: missing env vars: ${missing[*]}"
    exit 1
fi

# ---------------------------------------------------------------------------
if [[ $RUN_GEO -eq 1 ]]; then
    echo "=== Step 1: build_geomap ==="
    uv run python "$SCRIPT_DIR/scripts/build_geomap.py"
else
    echo "=== Step 1: skipped ==="
fi

# ---------------------------------------------------------------------------
if [[ $RUN_DB -eq 1 ]]; then
    echo ""
    echo "=== Step 2: build_db ==="
    uv run python "$SCRIPT_DIR/scripts/build_db.py"
else
    echo ""
    echo "=== Step 2: skipped ==="
fi

# ---------------------------------------------------------------------------
if [[ $RUN_SIM -eq 1 ]]; then
    echo ""
    echo "=== Step 3: build_sim ==="
    uv run python "$SCRIPT_DIR/scripts/build_sim.py"
else
    echo ""
    echo "=== Step 3: skipped ==="
fi

echo ""
echo "Done."
