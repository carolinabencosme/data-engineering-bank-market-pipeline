#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Servicios que deben permanecer corriendo (alineado con check-health.ps1)
expected_running_services=(postgres clickhouse airflow-webserver airflow-scheduler dbt)

summary=()
critical_failures=0

log_summary() {
  local service="$1"
  local status="$2"
  local detail="$3"
  summary+=("$(printf '%-22s | %-4s | %s' "$service" "$status" "$detail")")
}

mark_pass() {
  log_summary "$1" "PASS" "$2"
}

mark_fail() {
  log_summary "$1" "FAIL" "$2"
  critical_failures=$((critical_failures + 1))
}

capture() {
  set +e
  local output
  output="$($@ 2>&1)"
  local exit_code=$?
  set -e
  printf '%s\n%s' "$exit_code" "$output"
}

compose_ps_lines() {
  docker compose ps -a --format '{{.Service}}	{{.State}}' 2>/dev/null || true
}

state_for_service() {
  local target="$1"
  local line svc state
  while IFS=$'\t' read -r svc state; do
    [[ -z "${svc:-}" ]] && continue
    if [[ "$svc" == "$target" ]]; then
      printf '%s' "${state:-}"
      return 0
    fi
  done < <(compose_ps_lines)
  return 1
}

check_compose_services() {
  local ps_out
  if ! ps_out="$(compose_ps_lines)"; then
    mark_fail "compose" "Unable to read docker compose ps output"
    return
  fi
  if [[ -z "$ps_out" ]]; then
    mark_fail "compose" "Unable to read docker compose ps output"
    return
  fi

  local missing=()
  local not_running=()
  local svc st

  for svc in "${expected_running_services[@]}"; do
    st="$(state_for_service "$svc" || true)"
    if [[ -z "$st" ]]; then
      missing+=("$svc")
      continue
    fi
    if [[ "$st" != "running" ]]; then
      not_running+=("${svc}=${st}")
    fi
  done

  if [[ ${#missing[@]} -gt 0 || ${#not_running[@]} -gt 0 ]]; then
    local detail=""
    [[ ${#missing[@]} -gt 0 ]] && detail+="missing:${missing[*]} "
    [[ ${#not_running[@]} -gt 0 ]] && detail+="not-running:${not_running[*]}"
    mark_fail "compose" "${detail%% }"
    return
  fi

  mark_pass "compose" "expected running services are present"
}

check_airflow_init() {
  local st
  st="$(state_for_service "airflow-init" || true)"

  if [[ -z "$st" ]]; then
    mark_fail "airflow-init" "service not found"
    return
  fi

  if [[ "$st" == *exited* ]] || [[ "$st" == *stopped* ]]; then
    mark_pass "airflow-init" "completed with state ${st}"
  elif [[ "$st" == "running" ]]; then
    mark_pass "airflow-init" "still running (may still be initializing)"
  else
    mark_fail "airflow-init" "unexpected state: ${st}"
  fi
}

check_postgres_ready() {
  local result exit_code output
  result="$(capture docker compose exec -T postgres pg_isready)"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  output="$(printf '%s\n' "$result" | tail -n +2)"

  if [[ "$exit_code" -eq 0 ]]; then
    mark_pass "postgres" "pg_isready ok"
  else
    mark_fail "postgres" "pg_isready failed (${output//$'\n'/; })"
  fi
}

check_clickhouse() {
  local result exit_code output
  result="$(capture docker compose exec -T clickhouse clickhouse-client --query 'SELECT 1')"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  output="$(printf '%s\n' "$result" | tail -n +2 | tr -d '[:space:]')"

  if [[ "$exit_code" -eq 0 && "$output" == "1" ]]; then
    mark_pass "clickhouse" "SELECT 1 ok"
  else
    mark_fail "clickhouse" "smoke query failed (${output:-no-output})"
  fi
}

check_airflow_webserver() {
  local result exit_code output
  result="$(capture docker compose exec -T airflow-webserver curl --silent --show-error --fail http://localhost:8080/health)"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  output="$(printf '%s\n' "$result" | tail -n +2)"

  if [[ "$exit_code" -eq 0 && "$output" == *healthy* ]]; then
    mark_pass "airflow-webserver" "health endpoint healthy"
  else
    mark_fail "airflow-webserver" "health endpoint failed (${output//$'\n'/; })"
  fi
}

check_airflow_scheduler() {
  local max_attempts=4
  local sleep_seconds=5
  local attempt last_result last_exit last_output

  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    last_result="$(capture docker compose exec -T airflow-scheduler bash -lc 'scheduler_host="$(hostname)"; airflow jobs check --job-type SchedulerJob --hostname "$scheduler_host"')"
    last_exit="$(printf '%s\n' "$last_result" | head -n1)"
    last_output="$(printf '%s\n' "$last_result" | tail -n +2)"

    if [[ "$last_exit" -eq 0 ]]; then
      if [[ "$attempt" -eq 1 ]]; then
        mark_pass "airflow-scheduler" "scheduler responsive"
      else
        mark_pass "airflow-scheduler" "scheduler responsive after retry ${attempt}/${max_attempts}"
      fi
      return
    fi

    if [[ "$attempt" -lt "$max_attempts" ]]; then
      sleep "$sleep_seconds"
    fi
  done

  local summary
  summary="$(printf '%s' "$last_output" | tr '\n' '; ' | head -c 240)"
  [[ ${#summary} -eq 240 ]] && summary="${summary}..."
  mark_fail "airflow-scheduler" "scheduler check failed (${summary})"
}

check_dbt() {
  local result exit_code headline
  result="$(capture docker compose exec -T dbt dbt --version)"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  headline="$(printf '%s\n' "$result" | tail -n +2 | head -n1)"

  if [[ "$exit_code" -eq 0 ]]; then
    mark_pass "dbt" "${headline:-dbt available}"
  else
    mark_fail "dbt" "dbt --version failed"
  fi
}

check_airbyte() {
  if ! command -v abctl >/dev/null 2>&1; then
    mark_fail "airbyte" "abctl not found"
    return
  fi

  local result exit_code output
  result="$(capture abctl local status)"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  output="$(printf '%s\n' "$result" | tail -n +2)"

  if [[ "$exit_code" -eq 0 ]] && printf '%s' "$output" | grep -Eqi 'running|healthy|available|up'; then
    mark_pass "airbyte" "abctl local status healthy"
  else
    mark_fail "airbyte" "status unavailable (${output//$'\n'/; })"
  fi
}

print_summary() {
  printf '\n%-22s | %-4s | %s\n' 'Service' 'Check' 'Detail'
  printf '%s\n' '---------------------- | ---- | ---------------------------------------------'
  local line
  for line in "${summary[@]}"; do
    printf '%s\n' "$line"
  done
}

main() {
  check_compose_services
  check_airflow_init
  check_postgres_ready
  check_clickhouse
  check_airflow_webserver
  check_airflow_scheduler
  check_dbt
  check_airbyte

  print_summary

  if [[ "$critical_failures" -gt 0 ]]; then
    exit 1
  fi
}

main "$@"
