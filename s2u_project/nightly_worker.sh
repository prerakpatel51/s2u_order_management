#!/usr/bin/env bash
# Nightly worker for Railway (Option A): runs all syncs in order.
# Usage on Railway: set the Service Start Command to
#   bash -lc './s2u_project/nightly_worker.sh'
# and the Cron Schedule to the desired time (UTC), e.g. 0 4 * * *

set -Eeuo pipefail

log() { printf "[%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"; }

PY="/opt/venv/bin/python"
MANAGE="s2u_project/manage.py"
DAYS="${MONTHLY_DAYS:-30}"

log "Starting nightly sync chain (days=$DAYS)"
log "1/4 sync_stores"
$PY $MANAGE sync_stores

log "2/4 load_products --skip-csv"
$PY $MANAGE load_products --skip-csv

log "3/4 sync_stocks (all products)"
$PY $MANAGE sync_stocks

log "4/4 sync_all_monthly_sales --days $DAYS"
$PY $MANAGE sync_all_monthly_sales --days "$DAYS"

log "Nightly sync chain completed"
