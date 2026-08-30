#!/usr/bin/env bash
#
# 01_setup.sh — idempotent AIStor wiring for the aiops lakehouse (milestone M0).
#
# Reads config.ini, points `mc` at the AIStor route, creates the raw telemetry
# bucket, and creates the Iceberg warehouse. Safe to re-run: every step checks
# for existing state before acting and exits 0 whether it created or found it.
#
# Usage:
#   bin/01_setup.sh [--config config.ini] [--alias aiops] [--insecure]
#
# Requires: mc >= RELEASE.2026-02-03 and an AIStor server >= RELEASE.2026-02-02
# (Iceberg Tables + built-in REST catalog).

set -euo pipefail

# --- locate repo root regardless of where we're invoked from -----------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- defaults / args ---------------------------------------------------------
CONFIG_FILE="$REPO_ROOT/config.ini"
ALIAS="aiops"
INSECURE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)   CONFIG_FILE="$2"; shift 2 ;;
    --alias)    ALIAS="$2"; shift 2 ;;
    --insecure) INSECURE="--insecure"; shift ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

# --- preflight ---------------------------------------------------------------
if ! command -v mc >/dev/null 2>&1; then
  echo "ERROR: 'mc' (MinIO client) not found on PATH." >&2
  echo "       Install mc >= RELEASE.2026-02-03 (needs Iceberg Tables support)." >&2
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "ERROR: config file not found: $CONFIG_FILE" >&2
  echo "       Copy config.ini.example to config.ini and fill in your values." >&2
  exit 1
fi

# --- tiny INI reader ---------------------------------------------------------
# ini_get <section> <key> : prints the value or empty string. Ignores comments
# (# and ;), trims whitespace around '='. Pure awk, no deps.
ini_get() {
  local section="$1" key="$2"
  awk -F '=' -v section="$section" -v key="$key" '
    /^[[:space:]]*[#;]/ { next }
    /^[[:space:]]*\[/ {
      gsub(/^[[:space:]]*\[|\][[:space:]]*$/, "", $0)
      cur = $0
      next
    }
    {
      k = $1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", k)
      if (cur == section && k == key) {
        sub(/^[^=]*=/, "", $0)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
        print $0
        exit
      }
    }
  ' "$CONFIG_FILE"
}

require() {
  # require <value> <human-name>
  if [[ -z "$1" || "$1" == "CHANGEME" ]]; then
    echo "ERROR: missing/placeholder config value: $2" >&2
    exit 1
  fi
}

# --- read config -------------------------------------------------------------
MINIO_ENDPOINT="$(ini_get minio endpoint)"
MINIO_ACCESS_KEY="$(ini_get minio access_key)"
MINIO_SECRET_KEY="$(ini_get minio secret_key)"
MINIO_SECURE="$(ini_get minio secure)"
RAW_BUCKET="$(ini_get minio raw_bucket)"
WAREHOUSE="$(ini_get iceberg warehouse)"
ICEBERG_URI="$(ini_get iceberg uri)"

require "$MINIO_ENDPOINT"   "[minio] endpoint"
require "$MINIO_ACCESS_KEY" "[minio] access_key"
require "$MINIO_SECRET_KEY" "[minio] secret_key"
RAW_BUCKET="${RAW_BUCKET:-telemetry-raw}"
WAREHOUSE="${WAREHOUSE:-telemetry}"

# Build the scheme from secure=true/false unless the endpoint already has one.
case "$MINIO_ENDPOINT" in
  http://*|https://*) URL="$MINIO_ENDPOINT" ;;
  *)
    if [[ "${MINIO_SECURE,,}" == "false" || "${MINIO_SECURE,,}" == "no" || "$MINIO_SECURE" == "0" ]]; then
      URL="http://$MINIO_ENDPOINT"
    else
      URL="https://$MINIO_ENDPOINT"
    fi
    ;;
esac

echo "==> aiops lakehouse setup"
echo "    config    : $CONFIG_FILE"
echo "    endpoint  : $URL"
echo "    alias     : $ALIAS"
echo "    raw bucket: $RAW_BUCKET"
echo "    warehouse : $WAREHOUSE"
[[ -n "$INSECURE" ]] && echo "    tls       : verification DISABLED (--insecure)"
echo

# --- 1. mc alias (idempotent: `mc alias set` overwrites in place) ------------
echo "==> [1/3] configuring mc alias '$ALIAS'"
mc alias set $INSECURE "$ALIAS" "$URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null
echo "    ok: alias '$ALIAS' -> $URL"

# --- 2. raw bucket (create only if missing) ----------------------------------
echo "==> [2/3] ensuring raw bucket '$RAW_BUCKET'"
if mc ls $INSECURE "$ALIAS/$RAW_BUCKET" >/dev/null 2>&1; then
  echo "    ok: bucket '$RAW_BUCKET' already exists"
else
  mc mb $INSECURE "$ALIAS/$RAW_BUCKET" >/dev/null
  echo "    created: bucket '$RAW_BUCKET'"
fi

# --- 3. Iceberg warehouse (create only if missing) ---------------------------
echo "==> [3/3] ensuring Iceberg warehouse '$WAREHOUSE'"
if mc table warehouse ls $INSECURE "$ALIAS" 2>/dev/null | grep -qw "$WAREHOUSE"; then
  echo "    ok: warehouse '$WAREHOUSE' already exists"
else
  # `mc table warehouse create` is not idempotent; guarded by the info check above.
  if mc table warehouse create $INSECURE "$ALIAS" "$WAREHOUSE" >/dev/null 2>&1; then
    echo "    created: warehouse '$WAREHOUSE'"
  else
    echo "    ERROR: could not create warehouse '$WAREHOUSE'." >&2
    echo "           Check that AIStor server >= RELEASE.2026-02-02 (Iceberg Tables)" >&2
    echo "           and mc >= RELEASE.2026-02-03, and that the keys can manage tables." >&2
    exit 1
  fi
fi

# --- summary -----------------------------------------------------------------
echo
echo "==> done. endpoints:"
echo "    S3 API           : $URL"
echo "    raw bucket        : s3://$RAW_BUCKET  (mc: $ALIAS/$RAW_BUCKET)"
echo "    Iceberg warehouse : $WAREHOUSE"
echo "    Iceberg REST cat. : ${ICEBERG_URI:-$URL/_iceberg}"
echo
echo "    next: make gen && make load"
