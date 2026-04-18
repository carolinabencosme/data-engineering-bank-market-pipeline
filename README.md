# data-engineering-bank-market-pipeline

Pipeline end-to-end: extracción (Yahoo / yfinance), landing en PostgreSQL, validación y modelado con dbt, carga incremental a ClickHouse (staging + curated), mart mensual y orquestación con Airflow. Docker Compose para el núcleo; Airbyte local opcional vía `abctl` (no participa en la ruta activa de datos).

## Requisitos

- **Docker** y **Docker Compose** (plugin `compose` v2).
- **`abctl`** en el `PATH` (el bootstrap lo usa para instalar/levantar Airbyte local; el pipeline de datos no depende de Airbyte).
- **Git**. En Windows, conviene `core.autocrlf` coherente con [`.gitattributes`](.gitattributes) para scripts bajo `infra/postgres/init/` (LF); si Postgres falla con `bad interpreter`, ver [Troubleshooting](#troubleshooting).

## Stack

| Componente   | Uso |
|-------------|-----|
| Docker Compose | Postgres, ClickHouse, Airflow, contenedor `dbt` |
| PostgreSQL   | `bank_market`: landing + analytics (dbt `dev`). Base `airflow`: metadata Airflow |
| ClickHouse   | Base `olap`: `stg_*`, `cur_*`, `mart_monthly_stock_summary` |
| Airflow      | DAGs `bank_market_pipeline`, `bank_market_landing_to_olap` |
| dbt          | Target `dev` (Postgres) y `olap` (ClickHouse, solo `models/olap`) |
| Python       | Extracción y cargas en tareas Airflow (`src/`) |

Arquitectura y decisiones: [docs/architecture.md](docs/architecture.md). Mapeo de datos: [docs/data_mapping.md](docs/data_mapping.md).

## Estructura del repositorio

```text
.
├── airflow/dags/          # DAGs Airflow
├── dbt/                   # Proyecto dbt (models, tests, snapshots)
├── docs/                  # Arquitectura, evidencia, checklists
├── infra/                 # Dockerfiles, env de ejemplo, ClickHouse/Postgres/Airflow/dbt
├── scripts/               # bootstrap.*, check-health.*
├── sql/                   # DDL landing (Postgres) y OLAP (ClickHouse)
├── src/                   # extract / load / validate
├── docker-compose.yml
├── .env.example
└── README.md
```

## Guía rápida: ejecutar el proyecto end-to-end

### 1. Configurar entorno

Copia los ejemplos y edita **`.env` en la raíz** (Compose carga ese archivo; `infra/*/*.env` son overrides opcionales).

**Linux / macOS**

```bash
cp .env.example .env
cp infra/postgres/postgres.env.example infra/postgres/postgres.env
cp infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
cp infra/airflow/airflow.env.example infra/airflow/airflow.env
cp infra/dbt/dbt.env.example infra/dbt/dbt.env
```

**Windows (PowerShell)**

```powershell
Copy-Item .env.example .env
Copy-Item infra/postgres/postgres.env.example infra/postgres/postgres.env
Copy-Item infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
Copy-Item infra/airflow/airflow.env.example infra/airflow/airflow.env
Copy-Item infra/dbt/dbt.env.example infra/dbt/dbt.env
```

En **`.env`** deben quedar valores reales (no placeholders) para al menos:

- Credenciales **Postgres** (`POSTGRES_*`, `AIRFLOW_DB_*`) y cadena **`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`** alineada con la base metadata `AIRFLOW_DB_NAME`.
- **`AIRFLOW__CORE__FERNET_KEY`** y **`AIRFLOW__WEBSERVER__SECRET_KEY`** (sin comillas en el archivo).

Generar claves con la misma imagen de Airflow que usa el proyecto:

```powershell
docker run --rm apache/airflow:2.11.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
docker run --rm apache/airflow:2.11.2 python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Opcional API Airbyte (solo scripts): copia `infra/airbyte/airbyte.env.example` → `infra/airbyte/airbyte.env` si lo necesitas.

### 2. Bootstrap

Desde la **raíz del repo**:

**Linux / macOS:** `bash scripts/bootstrap.sh`  
**Windows:** `powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1`

El script levanta servicios Compose, espera init de esquemas, arranca/instala Airbyte con `abctl` y ejecuta el health check.

### 3. Comprobar salud

**Linux / macOS:** `bash scripts/check-health.sh`  
**Windows:** `powershell -ExecutionPolicy Bypass -File .\scripts\check-health.ps1`

Útil también: `docker compose ps` y `docker compose logs --tail=100 <servicio>`.

### 4. Ejecutar el pipeline en Airflow

1. Abre la UI: `http://localhost:8080` (puerto según `AIRFLOW_WEBSERVER_PORT`).
2. Activa (**unpause**) el DAG **`bank_market_pipeline`**.
3. Lanza una corrida manual o espera al schedule (`0 */6 * * *`).

Primera vez con datos vacíos: este DAG incluye extracción → `landing` → dbt (`dev`) → `load_olap` → mart ClickHouse (`dbt` `olap`) → validaciones y watermark.

El DAG **`bank_market_landing_to_olap`** repite la parte posterior sin Yahoo (útil si landing ya tiene datos); schedule por defecto `*/30 * * * *` (configurable con `AIRFLOW__PIPELINE__LANDING_TO_OLAP_CRON`).

### 5. Validar datos y calidad (opcional, manual)

**Postgres (staging/marts, como en el DAG `dbt_validation`):**

```powershell
docker compose exec -T dbt dbt run --target dev --select path:models/staging path:models/marts
docker compose exec -T dbt dbt test --target dev --select path:models/staging path:models/marts path:tests --exclude path:tests/olap
```

**ClickHouse (solo mart OLAP y tests bajo `tests/olap`, como en `monthly_summary`):**

```powershell
docker compose exec -T dbt dbt run --target olap --select path:models/olap
docker compose exec -T dbt dbt test --target olap --select path:models/olap path:tests/olap
```

No ejecutes `dbt test --target olap` sin `--select`: los tests de `sources`/`staging`/`marts` de Postgres no aplican a ClickHouse y fallarán con bases inexistentes (`landing`, `olap_staging`, `olap_marts`).

Consultas útiles en ClickHouse:

```powershell
docker compose exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM olap"
docker compose exec -T clickhouse clickhouse-client --query "SELECT * FROM olap.mart_monthly_stock_summary ORDER BY symbol, year, month LIMIT 20"
```

Detalle del mart: [docs/mart_monthly_stock_summary.md](docs/mart_monthly_stock_summary.md).

## Postgres: dos bases en el mismo servidor

- **`POSTGRES_DB`** (p. ej. `bank_market`): landing + objetos dbt `dev` (usuario típico `POSTGRES_USER` / `pipeline`).
- **`AIRFLOW_DB_NAME`** (p. ej. `airflow`): solo metadata Airflow (`AIRFLOW_DB_USER`).

`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` debe apuntar a la base **airflow**, no a `bank_market`. La conexión `postgres_landing` del DAG se define con `AIRFLOW_CONN_POSTGRES_LANDING` o la conexión homónima en Airflow hacia `bank_market`.

Los scripts en `infra/postgres/init/` solo corren en **primer arranque con volumen vacío**. Si cambias init o el volumen quedó inconsistente: `docker compose down -v`, vuelve a levantar y repite bootstrap.

## Watermark e idempotencia

Airflow guarda una variable de watermark sobre `MAX(ingested_at)` en tablas `landing.*`. Si no hay filas nuevas respecto al watermark, las tareas downstream de dbt/OLAP/mart pueden **omitirse** (`skip`); hay lógica de resync si staging en ClickHouse está vacío pero landing tiene datos. Detalle: código en `airflow/dags/bank_market_pipeline.py` (`_check_new_rows_since_watermark`).

## ClickHouse: `stg_*` vs `cur_*`

- **`olap.stg_*`**: ReplacingMergeTree; pueden existir varias versiones físicas hasta `OPTIMIZE`/merge.
- **`olap.cur_*`**: vista o capa deduplicada para negocio.
- **`olap.mart_monthly_stock_summary`**: mart mensual (dbt `olap`).

Inspección rápida:

```powershell
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.stg_stock_daily_price"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.stg_stock_daily_price FINAL"
```

`OPTIMIZE TABLE ... FINAL` solo en ventanas de mantenimiento (costoso).

## Airbyte

Provisionado con **`abctl`** (`local install` / `start` / `status`). El bootstrap **requiere** `abctl` para este paso. La ingesta **activa** del repo no usa conectores Airbyte; opciones futuras: [docs/airbyte_integration_options.md](docs/airbyte_integration_options.md).

## Airflow en Compose

Servicios separados: `airflow-init` (one-shot), `airflow-webserver`, `airflow-scheduler`. Imagen custom: [infra/airflow/Dockerfile](infra/airflow/Dockerfile) (dbt-postgres + dbt-clickhouse). Volúmenes: `./dbt` → `/opt/airflow/dbt`, `./infra/dbt` → `/opt/airflow/infra/dbt`, `./src` con `PYTHONPATH` para importar `src.*`.

## Evidencia y cumplimiento

- [docs/evidence/README.md](docs/evidence/README.md)
- [docs/compliance_checklist.md](docs/compliance_checklist.md)

## Troubleshooting

| Problema | Qué revisar |
|----------|-------------|
| Postgres init / `bad interpreter` / rol `airflow` ausente | Fin de línea LF en `infra/postgres/init/`; `docker compose down -v` y subir de nuevo; [`.gitattributes`](.gitattributes) |
| `airflow-init` o webserver fallan | Logs: `docker compose logs airflow-init`, `airflow-webserver`; `FERNET_KEY`, `SECRET_KEY`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` |
| Webserver lento / health check | El script `check-health` espera hasta `HEALTHCHECK_AIRFLOW_WEBSERVER_MAX_WAIT_SECONDS` (ver `.env.example`) |
| dbt en tareas Airflow: `dbt: command not found` | `docker compose build airflow-init airflow-webserver airflow-scheduler` y `docker compose up -d ... --force-recreate` |
| Contenedor `dbt`: `No such command 'sleep'` | En `docker-compose.yml`, `entrypoint: []` con `command: ["sleep", "infinity"]` (ya aplicado en el repo) |
| Airbyte / `abctl` | `abctl local status`, `abctl local start`, `abctl local logs` |
| Health check falla | `scripts/check-health.ps1` o `.sh` y corregir cada `FAIL` |

---

Los archivos `.env` reales no se versionan (`.gitignore`). Usa solo `.env.example` y `*.env.example` como plantillas.
