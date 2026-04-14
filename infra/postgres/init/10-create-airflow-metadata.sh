#!/bin/sh
# Provision Airflow metadata role + database on first cluster init only.
# CREATE DATABASE is not allowed inside DO/plpgsql; role logic stays in DO, DB DDL runs as top-level SQL.
# Values from container env (Compose loads .env). POSIX sh.
set -eu

: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${POSTGRES_DB:?POSTGRES_DB must be set}"
: "${AIRFLOW_DB_USER:?AIRFLOW_DB_USER must be set}"
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"
: "${AIRFLOW_DB_NAME:?AIRFLOW_DB_NAME must be set}"

if [ "${POSTGRES_DB}" = "${AIRFLOW_DB_NAME}" ]; then
  echo "Refusing to use the same database name for pipeline (${POSTGRES_DB}) and Airflow metadata (${AIRFLOW_DB_NAME})." >&2
  exit 1
fi

if [ "${POSTGRES_USER}" = "${AIRFLOW_DB_USER}" ]; then
  echo "Refusing to use the same PostgreSQL role for pipeline (${POSTGRES_USER}) and Airflow (${AIRFLOW_DB_USER})." >&2
  exit 1
fi

escape_sql_literal() {
  printf '%s' "$1" | sed "s/'/''/g"
}

u=$(escape_sql_literal "$AIRFLOW_DB_USER")
p=$(escape_sql_literal "$AIRFLOW_DB_PASSWORD")
d=$(escape_sql_literal "$AIRFLOW_DB_NAME")

psql -v ON_ERROR_STOP=1 \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  <<SQLEND
DO \$body\$
DECLARE
  airflow_user text := '$u';
  airflow_pass text := '$p';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = airflow_user) THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', airflow_user::text, airflow_pass);
  ELSE
    EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', airflow_user::text, airflow_pass);
  END IF;
END
\$body\$;

SELECT format('CREATE DATABASE %I OWNER %I', '$d'::name, '$u'::name)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '$d');
\\gexec

SELECT format('ALTER DATABASE %I OWNER TO %I', '$d'::name, '$u'::name)
WHERE EXISTS (SELECT 1 FROM pg_database WHERE datname = '$d');
\\gexec

SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', '$d'::name, '$u'::name);
\\gexec
SQLEND

echo "Airflow metadata role and database provisioning completed."
