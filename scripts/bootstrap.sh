#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

log() {
  local level="$1"
  shift
  printf '[%s] %s\n' "$level" "$*"
}

info() {
  log "INFO" "$*"
}

warn() {
  log "WARN" "$*"
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

require_env_files() {
  info "Validating required environment files..."

  local required_files=(
    ".env"
    "infra/postgres/postgres.env"
    "infra/clickhouse/clickhouse.env"
    "infra/airflow/airflow.env"
    "infra/dbt/dbt.env"
  )

  local missing=()
  local f
  for f in "${required_files[@]}"; do
    if [[ ! -f "$REPO_ROOT/$f" ]]; then
      missing+=("$f")
    fi
  done

  if [[ ${#missing[@]} -gt 0 ]]; then
    error "Missing required environment file(s):"
    local m
    for m in "${missing[@]}"; do
      error " - $m"
    done
    error "Create them from templates, for example:"
    error "  cp .env.example .env"
    error "  cp infra/postgres/postgres.env.example infra/postgres/postgres.env"
    error "  cp infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env"
    error "  cp infra/airflow/airflow.env.example infra/airflow/airflow.env"
    error "  cp infra/dbt/dbt.env.example infra/dbt/dbt.env"
    return 1
  fi

  info "All required environment files are present."
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
  local script_path="$SCRIPT_DIR/check-health.sh"

  if [[ ! -f "$script_path" ]]; then
    error "Health-check script not found at $script_path"
    return 1
  fi

  info "Running health-check script..."
  bash "$script_path"
}

airbyte_status_healthy() {
  local text="$1"
  grep -Eqi 'running|healthy|up|available' <<<"$text"
}

airbyte_not_installed_message() {
  local text="$1"
  grep -Eqi 'not[[:space:]]+found|no[[:space:]]+local[[:space:]]+installation|not[[:space:]]+installed' <<<"$text"
}

ensure_airbyte_running() {
  local status_output status_exit

  info "Checking Airbyte status with abctl..."
  set +e
  status_output="$(abctl local status 2>&1)"
  status_exit=$?
  set -e

  if [[ $status_exit -eq 0 ]] && airbyte_status_healthy "$status_output"; then
    info "Airbyte is already running."
    return 0
  fi

  if airbyte_not_installed_message "$status_output"; then
    info "No local Airbyte installation detected. Installing Airbyte locally..."
    abctl local install
    info "Airbyte installation completed."
    return 0
  fi

  info "Airbyte appears installed but is not running. Starting Airbyte (skipping reinstall)..."
  if abctl local start; then
    info "Airbyte started successfully."
    return 0
  fi

  error "Airbyte start failed. A local installation already exists; bootstrap does not run 'abctl local install' again."
  error "Fix the issue or inspect with: abctl local status && abctl local logs"
  return 1
}

start_core_services() {
  info "Starting core services (postgres, clickhouse, airflow-init, airflow-webserver, airflow-scheduler, dbt)..."
  docker compose up -d \
    postgres \
    clickhouse \
    airflow-init \
    airflow-webserver \
    airflow-scheduler \
    dbt
  info "Core services started successfully."
}

main() {
  info "Running bootstrap preflight checks..."

  require_env_files

  require_cmd docker "Install Docker and ensure it is on PATH."
  require_cmd abctl "Install Airbyte abctl and ensure it is on PATH."

  check_docker_daemon
  check_docker_compose

  start_core_services
  ensure_airbyte_running

  run_health_check

  info "Bootstrap completed successfully."
}

if ! main "$@"; then
  error "Bootstrap failed."
  exit 1
fi
