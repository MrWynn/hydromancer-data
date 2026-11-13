#!/usr/bin/env bash

set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
CONFIG="${PROJECT_DIR}/config.toml"
LOG_DIR="${PROJECT_DIR}/logs"
RUN_DIR="${PROJECT_DIR}/.run"
PID_FILE="${RUN_DIR}/hydromancer-sync.pid"
DEFAULT_DATASETS="perp,spot,account_values"

usage() {
    echo "Usage: $0 START_DATE [END_DATE] [DATASETS]"
    echo "Example: $0 2026-11-10"
    echo "Example: $0 2026-11-01 2026-11-10 perp,spot"
}

log() {
    printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

run_cli() {
    PYTHONPATH="${PROJECT_DIR}/src" "${PYTHON}" -m hydro_snapshot.cli "$@" \
        --config "${CONFIG}"
}

run_worker() {
    start_date="$1"
    end_date="$2"
    datasets="$3"

    echo "$$" > "${PID_FILE}"
    cleanup() {
        exit_code=$?
        if [ "${exit_code}" -eq 0 ]; then
            log "Sync job completed"
        else
            log "Sync job failed with exit code ${exit_code}"
        fi
        if [ -f "${PID_FILE}" ] && [ "$(sed -n '1p' "${PID_FILE}")" = "$$" ]; then
            rm -f "${PID_FILE}"
        fi
    }
    trap cleanup EXIT

    log "Sync job started: from=${start_date} to=${end_date} datasets=${datasets}"

    log "Step 1/4: refreshing source inspection"
    run_cli inspect \
        --from "${start_date}" \
        --to "${end_date}" \
        --dataset "${datasets}" \
        --refresh

    log "Step 2/4: checking sync plan"
    run_cli sync \
        --from "${start_date}" \
        --to "${end_date}" \
        --dataset "${datasets}" \
        --dry-run

    log "Step 3/4: downloading and syncing"
    run_cli sync \
        --from "${start_date}" \
        --to "${end_date}" \
        --dataset "${datasets}" \
        --yes

    log "Step 4/4: reading final status"
    run_cli status \
        --from "${start_date}" \
        --to "${end_date}" \
        --dataset "${datasets}"
}

mkdir -p "${LOG_DIR}" "${RUN_DIR}"

if [ "${1:-}" = "--worker" ]; then
    run_worker "$2" "$3" "$4"
    exit 0
fi

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
    usage
    exit 2
fi

START_DATE="$1"
END_DATE="${2:-$START_DATE}"
DATASETS="${3:-$DEFAULT_DATASETS}"

if [[ ! "${START_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || \
   [[ ! "${END_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Dates must use YYYY-MM-DD format" >&2
    exit 2
fi

if [ ! -x "${PYTHON}" ]; then
    echo "Python not found: ${PYTHON}" >&2
    exit 1
fi

if [ ! -f "${CONFIG}" ]; then
    echo "Config not found: ${CONFIG}" >&2
    exit 1
fi

if [ -f "${PID_FILE}" ]; then
    RUNNING_PID="$(sed -n '1p' "${PID_FILE}")"
    if [ -n "${RUNNING_PID}" ] && kill -0 "${RUNNING_PID}" 2>/dev/null; then
        echo "A sync job is already running with PID ${RUNNING_PID}" >&2
        exit 1
    fi
    rm -f "${PID_FILE}"
fi

TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_FILE="${LOG_DIR}/sync_${START_DATE}_${END_DATE}_${TIMESTAMP}.log"

nohup "$0" --worker "${START_DATE}" "${END_DATE}" "${DATASETS}" \
    >> "${LOG_FILE}" 2>&1 < /dev/null &
BACKGROUND_PID=$!

echo "Sync job started in background"
echo "PID: ${BACKGROUND_PID}"
echo "Log: ${LOG_FILE}"
echo "Follow log: tail -f '${LOG_FILE}'"
