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

assert_required_dotenv_keys() {
  local f="$REPO_ROOT/.env"
  local required=(
    POSTGRES_USER
    POSTGRES_DB
    POSTGRES_PASSWORD
    AIRFLOW_DB_USER
    AIRFLOW_DB_PASSWORD
    AIRFLOW_DB_NAME
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  )

  local key line val
  for key in "${required[@]}"; do
    line="$(grep -E "^${key}=" "$f" | tail -n1 || true)"
    if [[ -z "$line" ]]; then
      error "Missing required key in .env: ${key}"
      return 1
    fi
    val="${line#*=}"
    val="${val%$'\r'}"
    if [[ -z "${val// }" ]]; then
      error "Empty required key in .env: ${key}"
      return 1
    fi
  done
}

wait_postgres_ready() {
  local timeout_seconds="${1:-120}"
  local deadline=$((SECONDS + timeout_seconds))

  info "Waiting for Postgres to accept connections (timeout ${timeout_seconds}s)..."
  while ((SECONDS < deadline)); do
    if docker compose exec -T postgres bash -lc \
      'export PGPASSWORD="${POSTGRES_PASSWORD?}"; pg_isready -h 127.0.0.1 -U "${POSTGRES_USER?}" -d "${POSTGRES_DB?}"' \
      >/dev/null 2>&1; then
      info "Postgres is accepting connections."
      return 0
    fi
    sleep 2
  done

  error "Postgres did not become reachable within ${timeout_seconds}s."
  error "Check: docker compose ps postgres"
  error "Logs: docker compose logs --tail=200 postgres"
  return 1
}

test_postgres_pipeline_role() {
  info "Validating pipeline database connectivity (POSTGRES_USER -> POSTGRES_DB)..."
  if ! docker compose exec -T postgres bash -lc \
    'set -euo pipefail; export PGPASSWORD="${POSTGRES_PASSWORD?}"; psql -h 127.0.0.1 -U "${POSTGRES_USER?}" -d "${POSTGRES_DB?}" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null' \
    >/dev/null 2>&1; then
    error "Pipeline Postgres authentication or database access failed (user=\$POSTGRES_USER, db=\$POSTGRES_DB)."
    error "Typical causes: wrong POSTGRES_PASSWORD in .env, or the Postgres volume predates init scripts (recreate with: docker compose down -v)."
    return 1
  fi
  info "Pipeline Postgres connectivity OK."
}

escape_sql_literal_host() {
  printf '%s' "$1" | sed "s/'/''/g"
}

wrap_bash_single_quoted_sql() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

assert_airflow_postgres_objects_exist() {
  info "Checking that Airflow metadata role and database exist (postgres init scripts)..."

  # Same logic as bootstrap.ps1: avoid psql :'var'. AIRFLOW_* may not exist on the host shell; read from the container.
  local airflow_user airflow_db eu ed check_sql sql_arg remote
  airflow_user="$(docker compose exec -T postgres printenv AIRFLOW_DB_USER | tr -d '\r')"
  airflow_db="$(docker compose exec -T postgres printenv AIRFLOW_DB_NAME | tr -d '\r')"
  eu="$(escape_sql_literal_host "$airflow_user")"
  ed="$(escape_sql_literal_host "$airflow_db")"
  check_sql="SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${eu}') AND EXISTS (SELECT 1 FROM pg_database WHERE datname = '${ed}') THEN 0 ELSE 1 END;"
  sql_arg="$(wrap_bash_single_quoted_sql "$check_sql")"

  remote="set -eu; export PGPASSWORD=\${POSTGRES_PASSWORD}; r=\$(psql -h 127.0.0.1 -U \"\${POSTGRES_USER}\" -d \"\${POSTGRES_DB}\" -v ON_ERROR_STOP=1 -tAc ${sql_arg} | tr -d \"[:space:]\"); if [ \"\$r\" = \"0\" ]; then exit 0; elif [ \"\$r\" = \"1\" ]; then echo MISSING_AIRFLOW_PROVISIONING check_sql_returned=1 >&2; exit 2; else echo MISSING_AIRFLOW_PROVISIONING unexpected_r=\${r:-empty} >&2; exit 2; fi"

  set +e
  docker compose exec -T postgres sh -c "$remote" >/dev/null 2>&1
  local ec=$?
  set -e

  if [[ "$ec" -eq 0 ]]; then
    info "Airflow metadata role and database are present in Postgres."
    return 0
  fi

  if [[ "$ec" -eq 2 ]]; then
    error "Airflow metadata is not provisioned in Postgres (missing role/database or check failed)."
    error "This usually means the Postgres volume was created before infra/postgres/init ran. Recreate the volume from repo root:"
    error "  docker compose down -v"
    error "  docker compose up -d"
    error "Then re-run bootstrap."
    return 1
  fi

  error "Could not verify Airflow metadata provisioning in Postgres (unexpected exit ${ec})."
  return 1
}

test_postgres_airflow_metadata_role() {
  info "Validating Airflow metadata database connectivity (AIRFLOW_DB_USER -> AIRFLOW_DB_NAME)..."
  if ! docker compose exec -T postgres bash -lc \
    'set -euo pipefail; export PGPASSWORD="${AIRFLOW_DB_PASSWORD?}"; psql -h 127.0.0.1 -U "${AIRFLOW_DB_USER?}" -d "${AIRFLOW_DB_NAME?}" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null' \
    >/dev/null 2>&1; then
    error "Airflow metadata Postgres authentication or database access failed (user=\$AIRFLOW_DB_USER, db=\$AIRFLOW_DB_NAME)."
    error "Typical causes: AIRFLOW_DB_* not provisioned (recreate Postgres volume after adding infra/postgres/init), or password mismatch vs AIRFLOW__DATABASE__SQL_ALCHEMY_CONN."
    return 1
  fi
  info "Airflow metadata Postgres connectivity OK."
}

wait_airflow_init_completed() {
  local timeout_seconds="${1:-600}"
  local deadline=$((SECONDS + timeout_seconds))

  info "Waiting for airflow-init to finish (timeout ${timeout_seconds}s)..."
  while ((SECONDS < deadline)); do
    local row svc state exit_code
    row="$(docker compose ps -a --format '{{.Service}}	{{.State}}	{{.ExitCode}}' 2>/dev/null | awk -F'	' '$1=="airflow-init"{print; exit}')"
    if [[ -n "$row" ]]; then
      IFS=$'\t' read -r svc state exit_code <<<"$row"
      if [[ "$state" == *exited* ]] || [[ "$state" == *stopped* ]]; then
        if [[ "${exit_code:-1}" == "0" ]]; then
          info "airflow-init completed successfully."
          return 0
        fi
        error "airflow-init failed (exit code ${exit_code:-unknown}). Check: docker compose logs --tail=200 airflow-init"
        return 1
      fi
    fi
    sleep 3
  done

  error "Timed out waiting for airflow-init to complete."
  return 1
}

test_airflow_metadata_db_check() {
  local timeout_seconds="${1:-180}"
  local deadline=$((SECONDS + timeout_seconds))

  info "Running 'airflow db check' (timeout ${timeout_seconds}s)..."
  while ((SECONDS < deadline)); do
    if docker compose exec -T airflow-webserver airflow db check >/dev/null 2>&1; then
      info "Airflow metadata DB check OK."
      return 0
    fi
    sleep 4
  done

  error "airflow db check did not succeed before timeout. Check: docker compose logs --tail=200 airflow-webserver"
  return 1
}

test_postgres_provisioning() {
  wait_postgres_ready 120 || return 1
  test_postgres_pipeline_role || return 1
  assert_airflow_postgres_objects_exist || return 1
  test_postgres_airflow_metadata_role || return 1
  wait_airflow_init_completed 600 || return 1
  test_airflow_metadata_db_check 180 || return 1
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
  assert_required_dotenv_keys || return 1

  require_cmd docker "Install Docker and ensure it is on PATH."
  require_cmd abctl "Install Airbyte abctl and ensure it is on PATH."

  check_docker_daemon
  check_docker_compose

  start_core_services
  test_postgres_provisioning || return 1
  ensure_airbyte_running

  run_health_check

  info "Bootstrap completed successfully."
}

if ! main "$@"; then
  error "Bootstrap failed."
  exit 1
fi
