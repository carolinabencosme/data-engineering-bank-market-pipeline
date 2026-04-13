#!/usr/bin/env bash
set -euo pipefail

expected_services=(postgres clickhouse airflow dbt)

summary=()
critical_failures=0

log_summary() {
  local service="$1"
  local status="$2"
  local detail="$3"
  summary+=("$(printf '%-10s | %-4s | %s' "$service" "$status" "$detail")")
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

check_compose_services() {
  local output
  output="$(docker compose ps --format json 2>/dev/null || true)"

  if [[ -z "$output" ]]; then
    mark_fail "compose" "Unable to read docker compose ps output"
    return
  fi

  local missing=()
  local unhealthy=()

  for svc in "${expected_services[@]}"; do
    local service_block
    service_block="$(printf '%s\n' "$output" | awk -v svc="$svc" '$0 ~ "\"Service\":\""svc"\"" {print}')"

    if [[ -z "$service_block" ]]; then
      missing+=("$svc")
      continue
    fi

    if ! printf '%s' "$service_block" | grep -Eq '"State":"running"'; then
      unhealthy+=("$svc")
    fi
  done

  if [[ ${#missing[@]} -gt 0 || ${#unhealthy[@]} -gt 0 ]]; then
    local detail=""
    if [[ ${#missing[@]} -gt 0 ]]; then
      detail+="missing:${missing[*]} "
    fi
    if [[ ${#unhealthy[@]} -gt 0 ]]; then
      detail+="not-running:${unhealthy[*]}"
    fi
    mark_fail "compose" "${detail%% }"
    return
  fi

  mark_pass "compose" "expected services are running"
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

check_airflow() {
  local result exit_code output
  result="$(capture docker compose exec -T airflow curl --silent --show-error --fail http://localhost:8080/health)"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  output="$(printf '%s\n' "$result" | tail -n +2)"

  if [[ "$exit_code" -eq 0 && "$output" == *"healthy"* ]]; then
    mark_pass "airflow" "health endpoint healthy"
  else
    mark_fail "airflow" "health endpoint failed"
  fi
}

check_dbt() {
  local result exit_code output
  result="$(capture docker compose exec -T dbt dbt --version)"
  exit_code="$(printf '%s\n' "$result" | head -n1)"
  output="$(printf '%s\n' "$result" | tail -n +2 | head -n1)"

  if [[ "$exit_code" -eq 0 ]]; then
    mark_pass "dbt" "${output:-dbt available}"
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
  printf '\nService    | Check | Detail\n'
  printf '%s\n' '---------- | ----- | ---------------------------------------------'
  for line in "${summary[@]}"; do
    printf '%s\n' "$line"
  done
}

main() {
  check_compose_services
  check_postgres_ready
  check_clickhouse
  check_airflow
  check_dbt
  check_airbyte

  print_summary

  if [[ "$critical_failures" -gt 0 ]]; then
    exit 1
  fi
}

main "$@"