#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  shift
  printf '[%s] %s\n' "$level" "$*"
}

info() {
  log "INFO" "$*"
}

error() {
  log "ERROR" "$*" >&2
}

require_cmd() {
  local cmd="$1"
  local hint="${2:-}"

  if ! command -v "$cmd" >/dev/null 2>&1; then
    error "Missing required command: $cmd"
    if [[ -n "$hint" ]]; then
      error "$hint"
    fi
    return 1
  fi
}

check_docker_daemon() {
  if ! docker info >/dev/null 2>&1; then
    error "Docker daemon is not reachable. Ensure Docker Desktop/Engine is running."
    return 1
  fi
}

check_docker_compose() {
  if ! docker compose version >/dev/null 2>&1; then
    error "Docker Compose v2 is not available via 'docker compose'."
    return 1
  fi
}

run_health_check() {
  local script_path="scripts/health-check.sh"

  if [[ ! -f "$script_path" ]]; then
    error "Health-check script not found at $script_path"
    return 1
  fi

  info "Running health-check script..."
  bash "$script_path"
}

ensure_airbyte_running() {
  local status_output

  info "Checking Airbyte status with abctl..."
  set +e
  status_output="$(abctl local status 2>&1)"
  local status_exit=$?
  set -e

  if [[ $status_exit -eq 0 ]]; then
    if [[ "$status_output" =~ [Rr]unning|[Hh]ealthy|[Uu]p ]]; then
      info "Airbyte is already running."
      return 0
    fi

    info "Airbyte appears installed but not running. Starting Airbyte..."
    abctl local start
    return 0
  fi

  if [[ "$status_output" =~ [Nn]ot[[:space:]]+found|[Nn]o[[:space:]]+local[[:space:]]+installation|[Nn]ot[[:space:]]+installed ]]; then
    info "No local Airbyte installation detected. Creating Airbyte local installation..."
    abctl local install
    return 0
  fi

  info "Unable to confirm Airbyte status; attempting idempotent start before install."
  set +e
  abctl local start >/tmp/abctl_start.log 2>&1
  local start_exit=$?
  set -e

  if [[ $start_exit -eq 0 ]]; then
    info "Airbyte start succeeded."
    return 0
  fi

  info "Airbyte start failed; attempting install."
  abctl local install
}

main() {
  info "Running bootstrap preflight checks..."
  require_cmd docker "Install Docker and ensure it is on PATH."
  check_docker_daemon
  check_docker_compose
  require_cmd abctl "Install Airbyte abctl and ensure it is on PATH."

  info "Starting core services (postgres, clickhouse, airflow, dbt)..."
  docker compose up -d postgres clickhouse airflow dbt

  ensure_airbyte_running

  run_health_check

  info "Bootstrap completed successfully."
}

if ! main "$@"; then
  error "Bootstrap failed."
  exit 1
fi