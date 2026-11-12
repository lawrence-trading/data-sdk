#!/usr/bin/env bash
# Refresh the warrant database end to end.
#
#   bash update.sh [daily|full] [cache-dir]
#
#   daily (default) — snapshot, announcements, strike/ratio events. ~3 min.
#   full            — the above plus warrant_basic_info's delisted sweep. The
#                     cursor is exercise_end_date, so it re-queries only the
#                     到期日 year-windows from the cursor's year on (a handful
#                     of requests); the first run on an empty state sweeps
#                     back to 2003 (~30 min).
#
# Why full is still needed: a warrant that listed after the last full sweep
# exists only in the snapshot, and the snapshot only carries live warrants.
# Once it expires it drops out, and without the delisted sweep having recorded
# it, it disappears from the database entirely.
#
# Crawl is incremental (dlt cursors under <cache-dir>/mops_pipeline_state);
# the two curated tables are pure functions of the raw layer and are rebuilt
# whole every run, so there is no build state to keep in sync.
#
# Do NOT delete <cache-dir>/mops_raw/warrant_strike_ratio_* to "start clean":
# MOPS serves only a rolling ~18 months of those reports, so the accumulated
# append-only copy is the only record of anything older.
set -euo pipefail

MODE="${1:-daily}"
CACHE_DIR="${2:-${DATA_SDK_WARRANT_CACHE_PATH:-/mnt/nfs/backup/warrant_history}}"
PYTHON="${PYTHON:-python}"

DAILY_RESOURCES=(
    warrant_active_snapshot
    warrant_announcement
    warrant_strike_ratio_adjustment
    warrant_pending_adjustment
    warrant_strike_ratio_reset
    warrant_pending_reset
)

case "$MODE" in
    daily)
        echo "=== crawl: ${DAILY_RESOURCES[*]} ==="
        "$PYTHON" -m data_sdk.crawlers.warrant \
            --cache-dir "$CACHE_DIR" --resources "${DAILY_RESOURCES[@]}"
        ;;
    full)
        echo "=== crawl: all resources (includes the delisted sweep) ==="
        "$PYTHON" -m data_sdk.crawlers.warrant --cache-dir "$CACHE_DIR"
        ;;
    *)
        echo "usage: update.sh [daily|full] [cache-dir]" >&2
        exit 2
        ;;
esac

echo "=== build dimension table ==="
"$PYTHON" -m data_sdk.crawlers.warrant.build_basic_info --cache-dir "$CACHE_DIR"

echo "=== build history (SCD2) ==="
"$PYTHON" -m data_sdk.crawlers.warrant.build_history --cache-dir "$CACHE_DIR"

echo "=== validate (offline, internal consistency) ==="
"$PYTHON" -m data_sdk.crawlers.warrant.validate --cache-dir "$CACHE_DIR"

echo "=== verify (online, against TWSE/TPEx OpenAPI) ==="
"$PYTHON" -m data_sdk.crawlers.warrant.verify_openapi --cache-dir "$CACHE_DIR"
