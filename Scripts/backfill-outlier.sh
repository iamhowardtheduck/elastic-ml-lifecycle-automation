#!/usr/bin/env bash
# backfill-outlier.sh
# ━━━━━━━━━━━━━━━━━━━
# Backfills 30 days of synthetic OpsPath APM span data for the
# Outlier Detection workshop.  Reads connection settings from
# workshop-config-outlier.json written by bootstrap-outlier.py.
#
# Usage (Instruqt):
#   bash /workspace/workshop/elastic-ml-lifecycle-automation/Scripts/backfill-outlier.sh
#
# Optional overrides (env vars):
#   DAYS              – historical window in days   (default: 30)
#   EVENTS_PER_DAY    – weekday events per day      (default: 3000)
#   ANOMALY_PCT       – % anomalous spans           (default: 5)
#   TIMEZONE          – diurnal alignment timezone  (default: UTC)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

DAYS="${DAYS:-30}"
EVENTS_PER_DAY="${EVENTS_PER_DAY:-3000}"
ANOMALY_PCT="${ANOMALY_PCT:-5}"
TIMEZONE="${TIMEZONE:-UTC}"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " OpsPath Outlier Workshop — Backfill"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Repo    : $REPO_DIR"
echo " Days    : $DAYS"
echo " Epd     : $EVENTS_PER_DAY"
echo " Anomaly : ${ANOMALY_PCT}%"
echo " TZ      : $TIMEZONE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

cd "$REPO_DIR"

python3 sdg-prime-outlier.py \
  --days          "$DAYS"           \
  --events-per-day "$EVENTS_PER_DAY" \
  --anomaly-pct   "$ANOMALY_PCT"    \
  --timezone      "$TIMEZONE"       \
  --backfill-only
