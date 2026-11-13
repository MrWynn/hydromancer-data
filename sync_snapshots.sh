#!/usr/bin/env bash

set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
CONFIG="${PROJECT_DIR}/config.toml"
LOG_DIR="${PROJECT_DIR}/logs"
RUN_DIR="${PROJECT_DIR}/.run"
PID_FILE="${RUN_DIR}/hydromancer-sync.pid"
STATE_FILE="${RUN_DIR}/hydromancer-sync.state"
STOP_FILE="${RUN_DIR}/hydromancer-sync.stop"
LOCK_FILE="${RUN_DIR}/hydromancer-sync.lock"
DEFAULT_DATASETS="perp,spot,account_values"

usage() {
    echo "Usage: $0 start START_DATE [END_DATE] [DATASETS]"
    echo "       $0 status"
    echo "       $0 stop"
    echo "       $0 restart [START_DATE [END_DATE] [DATASETS]]"
    echo "Example: $0 start 2026-11-10"
    echo "Example: $0 start 2026-11-01 2026-11-10 perp,spot"
}

log() {
    printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

run_cli() {
    PYTHONPATH="${PROJECT_DIR}/src" "${PYTHON}" -m hydro_snapshot.cli "$@" \
        --config "${CONFIG}"
}

state_value() {
    key="$1"
    if [ -f "${STATE_FILE}" ]; then
        sed -n "s/^${key}=//p" "${STATE_FILE}" | head -n 1
    fi
}

running_pid() {
    if [ ! -f "${PID_FILE}" ]; then
        return 1
    fi
    pid="$(sed -n '1p' "${PID_FILE}")"
    if [[ ! "${pid}" =~ ^[0-9]+$ ]] || ! kill -0 "${pid}" 2>/dev/null; then
        return 1
    fi
    process_args="$(ps -o args= -p "${pid}" 2>/dev/null)" || return 1
    case "${process_args}" in
        *sync_snapshots.sh*--worker*) ;;
        *) return 1 ;;
    esac
    printf '%s\n' "${pid}"
}

notify_lark() {
    message="$1"
    webhook="$(
        PYTHONPATH="${PROJECT_DIR}/src" "${PYTHON}" -c \
            'import sys; from pathlib import Path; from hydro_snapshot.config import _load_toml; print(_load_toml(Path(sys.argv[1])).get("lark", {}).get("webhook_url", ""))' \
            "${CONFIG}"
    )" || {
        log "Lark notification skipped: failed to read configuration"
        return 0
    }
    if [ -z "${webhook}" ]; then
        log "Lark notification skipped: lark.webhook_url is not configured"
        return 0
    fi
    payload="$(
        "${PYTHON}" -c \
            'import json, sys; print(json.dumps({"msg_type": "text", "content": {"text": sys.argv[1]}}))' \
            "${message}"
    )"
    if ! curl -fsS --connect-timeout 5 --max-time 15 \
        -H 'Content-Type: application/json' \
        -d "${payload}" "${webhook}" >/dev/null; then
        log "Lark notification failed"
    fi
}

run_worker() {
    start_date="$1"
    end_date="$2"
    datasets="$3"
    log_file="$4"

    cleanup() {
        exit_code=$?
        set +e
        trap - EXIT TERM INT
        if [ -f "${STOP_FILE}" ]; then
            result="正常退出（手动停止）"
            rm -f "${STOP_FILE}"
        elif [ "${exit_code}" -eq 0 ]; then
            result="正常退出"
        else
            result="异常退出（exit code ${exit_code}）"
        fi
        log "Sync job ${result}: from=${start_date} to=${end_date} datasets=${datasets}"
        notification="$(printf 'Hydromancer sync %s\nfrom: %s\nto: %s\ndatasets: %s\nlog: %s' \
            "${result}" "${start_date}" "${end_date}" "${datasets}" "${log_file}")"
        notify_lark "${notification}"
        if [ -f "${PID_FILE}" ] && [ "$(sed -n '1p' "${PID_FILE}")" = "$$" ]; then
            rm -f "${PID_FILE}"
        fi
        exit "${exit_code}"
    }
    trap cleanup EXIT
    trap 'exit 143' TERM INT

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

validate_start_args() {
    if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
        usage
        return 2
    fi
    if [[ ! "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || \
       { [ "$#" -ge 2 ] && [[ ! "$2" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; }; then
        echo "Dates must use YYYY-MM-DD format" >&2
        return 2
    fi
    if [ ! -x "${PYTHON}" ]; then
        echo "Python not found: ${PYTHON}" >&2
        return 1
    fi
    if [ ! -f "${CONFIG}" ]; then
        echo "Config not found: ${CONFIG}" >&2
        return 1
    fi
}

start_job() {
    validate_start_args "$@" || return $?
    start_date="$1"
    end_date="${2:-$start_date}"
    datasets="${3:-$DEFAULT_DATASETS}"

    if pid="$(running_pid)"; then
        echo "A sync job is already running with PID ${pid}" >&2
        return 1
    fi
    rm -f "${PID_FILE}" "${STOP_FILE}"

    timestamp="$(date -u '+%Y%m%dT%H%M%SZ')"
    log_file="${LOG_DIR}/sync_${start_date}_${end_date}_${timestamp}.log"
    state_tmp="${STATE_FILE}.tmp"
    printf 'start_date=%s\nend_date=%s\ndatasets=%s\nlog_file=%s\n' \
        "${start_date}" "${end_date}" "${datasets}" "${log_file}" > "${state_tmp}"
    mv "${state_tmp}" "${STATE_FILE}"

    nohup setsid "$0" --worker "${start_date}" "${end_date}" "${datasets}" \
        "${log_file}" >> "${log_file}" 2>&1 < /dev/null 9>&- &
    background_pid=$!
    printf '%s\n' "${background_pid}" > "${PID_FILE}"

    echo "Sync job started in background"
    echo "PID: ${background_pid}"
    echo "Log: ${log_file}"
    echo "Follow log: tail -f '${log_file}'"
}

status_job() {
    if pid="$(running_pid)"; then
        echo "Sync job is running"
        echo "PID: ${pid}"
        echo "From: $(state_value start_date)"
        echo "To: $(state_value end_date)"
        echo "Datasets: $(state_value datasets)"
        echo "Log: $(state_value log_file)"
        return 0
    fi
    rm -f "${PID_FILE}"
    echo "Sync job is not running"
    if [ -f "${STATE_FILE}" ]; then
        echo "Last log: $(state_value log_file)"
    fi
}

stop_job() {
    if ! pid="$(running_pid)"; then
        rm -f "${PID_FILE}"
        echo "Sync job is not running"
        return 0
    fi
    session_id="$(ps -o sid= -p "${pid}" | tr -d ' ')"
    : > "${STOP_FILE}"
    if [ "${session_id}" = "${pid}" ]; then
        kill -TERM -- "-${pid}"
    else
        kill -TERM "${pid}"
    fi
    for _ in $(seq 1 100); do
        if ! kill -0 "${pid}" 2>/dev/null; then
            rm -f "${PID_FILE}"
            echo "Sync job stopped"
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for sync job ${pid} to stop" >&2
    return 1
}

restart_job() {
    if [ "$#" -eq 0 ]; then
        if [ ! -f "${STATE_FILE}" ]; then
            echo "No previous sync arguments found; provide START_DATE" >&2
            return 1
        fi
        set -- "$(state_value start_date)" "$(state_value end_date)" \
            "$(state_value datasets)"
    fi
    validate_start_args "$@" || return $?
    stop_job || return $?
    start_job "$@"
}

mkdir -p "${LOG_DIR}" "${RUN_DIR}"

if [ "${1:-}" = "--worker" ]; then
    run_worker "$2" "$3" "$4" "$5"
    exit 0
fi

if ! command -v flock >/dev/null 2>&1; then
    echo "Required command not found: flock" >&2
    exit 1
fi
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "Another sync control command is running; try again" >&2
    exit 1
fi

command="${1:-}"
case "${command}" in
    start)
        shift
        start_job "$@"
        ;;
    status)
        shift
        if [ "$#" -ne 0 ]; then usage; exit 2; fi
        status_job
        ;;
    stop)
        shift
        if [ "$#" -ne 0 ]; then usage; exit 2; fi
        stop_job
        ;;
    restart)
        shift
        restart_job "$@"
        ;;
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])
        start_job "$@"
        ;;
    *)
        usage
        exit 2
        ;;
esac
