# data-engineering-bank-market-pipeline
Data engineering pipeline for extracting, validating, transforming, and serving US-listed bank market data using Docker, PostgreSQL, ClickHouse, Airbyte, dbt, and Airflow.

## Infra local con Docker Compose

Se agregó una orquestación local con servicios separados para:

- `postgres`
- `clickhouse`
- `airbyte`
- `airflow`
- `dbt`

Archivo principal: `docker-compose.yml`.

## Estructura de configuración

Cada servicio tiene su carpeta dedicada en `infra/`:

- `infra/postgres/`
- `infra/clickhouse/`
- `infra/airbyte/`
- `infra/airflow/`
- `infra/dbt/`

La configuración evita hardcodear secretos: las credenciales y flags se inyectan mediante variables de entorno (`.env`).

## Variables requeridas

1. Copiar el archivo de ejemplo:

```bash
cp .env.example .env
```

2. Completar todos los secretos y valores de entorno en `.env`:

- usuarios y contraseñas (`POSTGRES_PASSWORD`, `CLICKHOUSE_PASSWORD`, `AIRFLOW_DB_PASSWORD`, etc.)
- claves sensibles de Airflow (`AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`)
- hosts/puertos y flags por entorno (`ENVIRONMENT`, `LOG_LEVEL`)

## Orden recomendado de levantamiento

Aunque `docker compose up` puede levantar todo junto, para facilitar troubleshooting se recomienda este orden:

1. **Bases de datos**

```bash
docker compose up -d postgres clickhouse
```

2. **Orquestación e ingesta**

```bash
docker compose up -d airbyte airflow
```

3. **Capa de transformación (dbt)**

```bash
docker compose up -d dbt
```

4. **Todo el stack** (opcional)

```bash
docker compose up -d
```

## Validación de salud de servicios

### Revisar estado general

```bash
docker compose ps
```

### Checks por servicio

- **Postgres**

```bash
docker compose exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

- **ClickHouse**

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT 1"
```

- **Airbyte**

```bash
curl -fsS "http://localhost:${AIRBYTE_PORT}/api/v1/health"
```

- **Airflow**

```bash
curl -fsS "http://localhost:${AIRFLOW_WEBSERVER_PORT}/health"
```

- **dbt**

```bash
docker compose exec dbt dbt --version
```

Si algún servicio queda en estado `unhealthy`, revisar logs:

```bash
docker compose logs --tail=100 <service>
```