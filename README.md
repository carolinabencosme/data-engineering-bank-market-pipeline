# data-engineering-bank-market-pipeline

Pipeline de ingeniería de datos para extraer, validar, transformar y servir información de mercado de bancos listados en la bolsa de valores de Estados Unidos, utilizando Docker, PostgreSQL, ClickHouse, Airbyte, dbt y Airflow.

## Descripción general

Este proyecto fue desarrollado como parte de una prueba técnica de ingeniería de datos. El objetivo es construir un pipeline end-to-end que permita extraer datos desde Yahoo Finance, almacenarlos inicialmente en una landing zone, validarlos y transformarlos, integrarlos en un entorno OLAP y automatizar su actualización de forma trazable y reproducible.

La solución sigue una arquitectura desacoplada y orientada a buenas prácticas de ingeniería, priorizando mantenibilidad, portabilidad, validación de calidad de datos, documentación clara y facilidad de ejecución en entorno local con Docker.

## Objetivo

Construir un pipeline de datos que permita:

- Extraer información de bancos listados en la bolsa de valores de Estados Unidos.
- Almacenar los datos crudos en una landing zone.
- Validar y transformar los datos utilizando dbt.
- Integrar la información curada en una solución OLAP.
- Generar una tabla de resumen mensual con métricas clave.
- Automatizar la actualización del pipeline cuando exista nueva información.
- Documentar el proceso completo para que cualquier persona pueda ejecutarlo localmente.

## Alcance funcional

El pipeline contempla la extracción de información correspondiente a los años **2024 y 2025** para bancos listados en bolsa, incluyendo:

### 1. Informaciones básicas
- Industry
- Sector
- Employee Count
- City
- Phone
- State
- Country
- Website
- Address

### 2. Precio diario en bolsa
- Date
- Open
- High
- Low
- Close
- Volume

### 3. Fundamentales
- Assets
- Debt
- Invested Capital
- Share Issued

### 4. Tenedores
- Date
- Holder
- Shares
- Value

### 5. Calificadores
- Date
- To Grade
- From Grade
- Action

Además, el proyecto genera una **tabla de resumen mensual** con:
- Precio promedio de apertura y cierre
- Volumen promedio mensual

## Stack tecnológico

Las herramientas principales utilizadas en la solución son:

- **Docker Compose**: orquestación local de los servicios core del pipeline
- **PostgreSQL**: landing zone para datos crudos
- **ClickHouse**: entorno OLAP para datos curados y explotación analítica
- **Airbyte**: plataforma opcional de integración local (provisionada con `abctl`; no es el path activo de carga en este repo)
- **dbt**: validación, modelado y transformación de datos
- **Airflow**: orquestación y automatización del pipeline
- **Python + yfinance**: extracción de datos desde Yahoo Finance
- **abctl**: provisioning local soportado para Airbyte

## Arquitectura de la solución

La arquitectura del proyecto separa claramente las responsabilidades del flujo de datos:

1. **Extracción**
   - Un script en Python se conecta a Yahoo Finance y descarga los datos requeridos.
   - Se consideran los límites de solicitudes del API para evitar bloqueos o fallos por rate limiting.

2. **Landing zone**
   - Los datos crudos se almacenan inicialmente en PostgreSQL.

3. **Integración**
   - La integración activa del flujo de datos se implementa con **Airflow + Python**:
     - `load_landing`: UPSERT a `landing.*` en PostgreSQL.
     - `load_olap`: sincronización incremental `landing -> olap.stg_*` y rebuild deduplicado de `olap.cur_*`.
   - Airbyte se mantiene como plataforma opcional para una integración futura, gestionada por `abctl`.

4. **Validación y transformación**
   - dbt implementa reglas de calidad y transforma los datos hacia estructuras curadas.

5. **Capa analítica / OLAP**
   - ClickHouse centraliza la información curada para consumo analítico.

6. **Orquestación**
   - Airflow automatiza y coordina la ejecución del pipeline.

Diagrama y decisiones de arquitectura: [docs/architecture.md](docs/architecture.md).

## Estructura del proyecto

```text
.
├── src/
│   ├── extract/          # Cliente Yahoo (solo lectura)
│   └── load/               # UPSERT a tablas landing en Postgres
├── dbt/
│   ├── models/
│   ├── snapshots/
│   ├── tests/
│   └── dbt_project.yml
├── infra/
│   ├── airbyte/
│   ├── airflow/
│   ├── clickhouse/
│   ├── dbt/
│   └── postgres/
│       └── init/
├── scripts/
│   ├── bootstrap.sh
│   ├── bootstrap.ps1
│   ├── check-health.sh
│   └── check-health.ps1
├── docker-compose.yml
├── .env.example
└── README.md
````

Cada servicio tiene su carpeta dedicada dentro de `infra/` para mantener una organización clara y facilitar la configuración independiente de cada componente.

## Configuración del entorno

La configuración evita hardcodear secretos: las credenciales y parámetros sensibles se inyectan mediante variables de entorno.

Este repositorio **no incluye archivos `.env` reales ni secretos**.
Solo se versionan archivos de ejemplo sanitizados para mantener la seguridad del proyecto y facilitar su reproducción.

### Archivos de ejemplo incluidos

* `.env.example`
* `infra/postgres/postgres.env.example`
* `infra/clickhouse/clickhouse.env.example`
* `infra/airflow/airflow.env.example`
* `infra/dbt/dbt.env.example`

### Pasos de configuración

#### En Linux / macOS

```bash
cp .env.example .env
cp infra/postgres/postgres.env.example infra/postgres/postgres.env
cp infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
cp infra/airflow/airflow.env.example infra/airflow/airflow.env
cp infra/dbt/dbt.env.example infra/dbt/dbt.env
```

#### En PowerShell (Windows)

```powershell
Copy-Item .env.example .env
Copy-Item infra/postgres/postgres.env.example infra/postgres/postgres.env
Copy-Item infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
Copy-Item infra/airflow/airflow.env.example infra/airflow/airflow.env
Copy-Item infra/dbt/dbt.env.example infra/dbt/dbt.env
```

Luego de eso, completa los valores requeridos en los archivos `.env` locales. **El `.env` en la raíz del repositorio es la fuente principal de variables para Docker Compose** (incluido Postgres, Airflow, ClickHouse y dbt); los archivos bajo `infra/*/*.env` sirven como overrides opcionales. Los scripts de bootstrap validan explícitamente claves críticas en el `.env` raíz (por ejemplo `POSTGRES_*`, `AIRFLOW_DB_*` y `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`).

> Opcional: si se desea implementar scripts que consumen la API de Airbyte, se puede crear `infra/airbyte/airbyte.env` desde `infra/airbyte/airbyte.env.example` para declarar endpoints de integración (`AIRBYTE_API_*`).

### Variables sensibles esperadas

Entre las variables que deben configurarse están:

* usuarios y contraseñas de PostgreSQL y ClickHouse
* credenciales de Airflow
* `AIRFLOW__CORE__FERNET_KEY`
* `AIRFLOW__WEBSERVER__SECRET_KEY`
* nombres de base de datos, puertos y hosts
* parámetros de entorno como `ENVIRONMENT` y `LOG_LEVEL`

### Claves obligatorias para Airflow

Airflow requiere claves reales para su cifrado interno y para la sesión del webserver. No deben dejarse placeholders como:

```env
AIRFLOW__CORE__FERNET_KEY=replace_with_fernet_key
AIRFLOW__WEBSERVER__SECRET_KEY=replace_with_web_secret
```

Debes generar valores reales y colocarlos en `.env`.

#### Generar `FERNET_KEY` con Docker

```powershell
docker run --rm apache/airflow:2.11.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

#### Generar `WEBSERVER__SECRET_KEY` con Docker

```powershell
docker run --rm apache/airflow:2.11.2 python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Luego coloca ambos valores en `.env`, **sin comillas**:

```env
AIRFLOW__CORE__FERNET_KEY=valor_generado
AIRFLOW__WEBSERVER__SECRET_KEY=valor_generado
```

### Nota de seguridad

Los archivos `.env` reales están excluidos del control de versiones mediante `.gitignore`.

No se deben subir al repositorio:

* contraseñas
* tokens
* API keys
* credenciales reales
* configuraciones sensibles locales

## PostgreSQL: datos del pipeline y metadata de Airflow

En el **mismo** servidor PostgreSQL conviven dos bases lógicas distintas:

* **`POSTGRES_DB` (por defecto `bank_market`)**: landing zone y datos operativos del pipeline; usuario **`POSTGRES_USER`** (por defecto `pipeline`).
* **`AIRFLOW_DB_NAME` (por defecto `airflow`)**: metadata interna de Airflow; usuario **`AIRFLOW_DB_USER`** (por defecto `airflow`).

La cadena `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` del `.env` debe apuntar a la base de metadata (`AIRFLOW_DB_NAME`) con el usuario `AIRFLOW_DB_USER`. No se reutiliza `bank_market` para metadata de Airflow.

### Airflow → PostgreSQL landing (`bank_market`) — ingesta Fase 1

El DAG `bank_market_pipeline` separa **extracción** (solo lectura a Yahoo vía [`src/extract/yahoo_finance_client.py`](src/extract/yahoo_finance_client.py)) y **carga** (UPSERT en tablas `landing.*` vía [`src/load/landing_psql_loader.py`](src/load/landing_psql_loader.py)). Entre ambas tareas el payload intermedio se guarda en disco bajo `PIPELINE_XCOM_SCRATCH_DIR` (por defecto `airflow/dags/_pipeline_scratch/` montado en el contenedor) para evitar volúmenes grandes en XCom.

* **Idempotencia:** cada tabla usa su llave natural definida en [`sql/landing/V001__create_landing_tables.sql`](sql/landing/V001__create_landing_tables.sql); en conflicto se actualizan columnas de negocio, `ingested_at`, `source_system` y `batch_id` (trazabilidad por corrida `DAG_ID + run_id`).
* **Conexión:** el task de carga usa `PostgresHook` con `AIRFLOW__PIPELINE__LANDING_CONN_ID` (por defecto `postgres_landing`). Debe existir una conexión Airflow apuntando a `POSTGRES_DB` con el usuario del pipeline, o definir en `.env` la URI estándar `AIRFLOW_CONN_POSTGRES_LANDING` (ver `.env.example`).
* **Configuración:** símbolos y ventana temporal se controlan con `PIPELINE_YAHOO_SYMBOLS`, `PIPELINE_BANK_UNIVERSE_LIMIT`, `PIPELINE_EXTRACT_*`, throttling con `PIPELINE_YAHOO_BATCH_SIZE` / `PIPELINE_YAHOO_BATCH_PAUSE_SECONDS`, y `PIPELINE_SOURCE_SYSTEM` para la columna homónima en landing.

Los servicios `airflow-webserver` y `airflow-scheduler` montan `./src` con `PYTHONPATH=/opt/airflow/pipeline` para importar `src.*` sin duplicar código fuera del repositorio.

En el primer arranque con un volumen de datos **vacío**, los scripts montados en `infra/postgres/init/` (por ejemplo `10-create-airflow-metadata.sh`) crean automáticamente el rol y la base de Airflow. El SQL usa literales interpolados desde el shell (escapando comillas simples), porque las variables de cliente `psql` del tipo `:'nombre` **no** son sustituibles de forma fiable dentro de bloques `DO ... $$` enviados al servidor. Además, **`CREATE DATABASE` no puede ejecutarse dentro de un `DO`**; el script crea el rol en un `DO` y la base con sentencias de nivel superior (incl. `\gexec` en `psql`). El script usa **POSIX `sh`** (sin bashismos como `[[`) para que sea válido también cuando la imagen de Postgres lo **carga con `. script`** en lugar de ejecutarlo (puede ocurrir en Windows si el bind mount no conserva el bit ejecutable).

**Importante en Windows:** los scripts bajo `infra/postgres/init/` deben guardarse con finales de línea **LF** (Unix). Si el archivo tiene **CRLF**, el contenedor falla con `bad interpreter: No such file or directory` (aparece como `/bin/sh^M`). El repositorio incluye `.gitattributes` para forzar LF en esos paths; tras clonar o cambiar `core.autocrlf`, ejecuta `git add --renormalize .` y vuelve a crear el volumen de Postgres (`docker compose down -v`). **Docker Compose carga primero el `.env` del raíz del repo** y fusiona `infra/postgres/postgres.env` para overrides opcionales.

### Reinicialización obligatoria del volumen de Postgres

Los archivos bajo `/docker-entrypoint-initdb.d` **solo se ejecutan cuando el volumen de datos de Postgres se crea por primera vez**. Si ya tenías un volumen previo sin el rol/base de Airflow, debes **eliminar el volumen** y volver a levantar los servicios para que se apliquen los scripts de inicialización.

En PowerShell, desde la raíz del repositorio:

```powershell
docker compose down -v
docker compose up -d
```

Sin `docker compose down -v` (o sin borrar el volumen nombrado de Postgres), los scripts de init **no** volverán a ejecutarse y seguirás con el estado antiguo del cluster.

Tras recrear el volumen, vuelve a ejecutar `scripts/bootstrap.*` para validar conectividad (`bank_market` con `pipeline`, base `airflow` con `airflow`, `airflow db check`, etc.).

## Levantamiento del entorno

Usa un flujo **bootstrap-script-first**. No levantes Airbyte desde `docker compose`; Airbyte se gestiona con `abctl` dentro del bootstrap.

## Technical Decision Record: Airbyte Local Provisioning

### 1) `abctl` como camino soportado para despliegue local

Se adopta `abctl` como ruta oficial para Airbyte local porque es el mecanismo recomendado por Airbyte para aprovisionar su stack en entornos de desarrollo de forma reproducible y alineada con su matriz de compatibilidad.

`abctl` encapsula el aprovisionamiento de componentes de Airbyte, estandariza el ciclo de vida (`install`, `start`, `stop`, `reset`) y reduce drift de configuración manual respecto al modelo operativo esperado por el producto.

### 2) Portabilidad preservada

La portabilidad del proyecto se mantiene porque:

* Los servicios core del pipeline siguen definidos en Docker Compose.
* Airbyte continúa ejecutándose en contenedores, solo que provisionados y gestionados por `abctl` en lugar de un servicio ad-hoc del `docker-compose.yml`.

En otras palabras, no se pierde portabilidad: se separa la responsabilidad de orquestación local entre “core del pipeline” y “plataforma Airbyte”, manteniendo ambos componentes containerizados.

### 3) Modelo operativo del entorno local

El entorno queda dividido en dos capas:

* **Capa core del pipeline:** `postgres`, `clickhouse`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `dbt`
* **Capa de integración:** Airbyte provisionado por `abctl`

Esto reduce acoplamiento y facilita troubleshooting cuando falla una parte específica del stack.

### 4) Beneficios operativos

Esta decisión mejora la operación local en cuatro ejes:

* **Reproducibilidad:** `abctl` reduce variaciones de setup entre máquinas y evita configuraciones manuales frágiles para Airbyte.
* **Compatibilidad:** el aprovisionamiento queda más cerca del camino soportado por el vendor, disminuyendo problemas por cambios de versión.
* **Ownership boundaries más limpias:** el repositorio mantiene foco en el pipeline; Airbyte se opera con su herramienta dedicada.
* **Maintainability:** menos lógica específica de Airbyte incrustada en Compose implica menor costo de actualización, debugging y documentación.

## Levantar el entorno

### Linux / macOS

1. Crear archivos de entorno locales:

```bash
cp .env.example .env
cp infra/postgres/postgres.env.example infra/postgres/postgres.env
cp infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
cp infra/airflow/airflow.env.example infra/airflow/airflow.env
cp infra/dbt/dbt.env.example infra/dbt/dbt.env
```

2. Ejecutar bootstrap:

```bash
bash scripts/bootstrap.sh
```

3. Verificar salud del entorno:

```bash
bash scripts/check-health.sh
```

### Windows PowerShell

1. Crear archivos de entorno locales:

```powershell
Copy-Item .env.example .env
Copy-Item infra/postgres/postgres.env.example infra/postgres/postgres.env
Copy-Item infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
Copy-Item infra/airflow/airflow.env.example infra/airflow/airflow.env
Copy-Item infra/dbt/dbt.env.example infra/dbt/dbt.env
```

2. Ejecutar bootstrap:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

3. Verificar salud del entorno:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\check-health.ps1
```

## Verificación de salud

Además de los scripts `scripts/check-health.*`, estos comandos son útiles para diagnóstico rápido.

### Estado de servicios Docker Compose

```bash
docker compose ps
```

### Estado de Airbyte (`abctl`)

```bash
abctl local status
```

### Logs de servicios Docker Compose

```bash
docker compose logs --tail=100 <service>
```

### Logs de Airbyte (`abctl`)

```bash
abctl local logs
```

> Recomendación operativa: corre `scripts/check-health.sh` o `scripts/check-health.ps1` después de cada bootstrap y después de cualquier cambio de configuración.

## Flujo del pipeline

De forma general, el pipeline sigue esta secuencia:

1. El extractor en Python consulta Yahoo Finance.
2. Los datos crudos se cargan en PostgreSQL (`landing.*`).
3. `check_new_landing_rows` evalúa watermark y decide ejecución downstream.
4. dbt (`target=dev`) valida y transforma **PostgreSQL** (`landing` → `analytics` staging/marts).
5. Airflow carga a ClickHouse (`load_olap`: `landing -> olap.stg_* -> olap.cur_*`).
6. dbt (`target=olap`) materializa el mart mensual en ClickHouse: `olap.mart_monthly_stock_summary`.
7. Airflow aplica quality gates y confirma watermark (`commit_watermark`).

Para disparo desacoplado desde landing (sin extracción Yahoo), también está disponible el DAG `bank_market_landing_to_olap`, que ejecuta `check_new_landing_rows -> dbt_prepare_deps -> dbt_validation -> load_olap -> monthly_summary` con las mismas validaciones y watermark.

## Rol real de Airbyte en este repositorio

Airbyte está **instalado/provisionado** para entorno local mediante `abctl` y se verifica en bootstrap/health checks, pero **no participa en la ruta de datos activa** del pipeline.

- Sí se usa:
  - `scripts/bootstrap.*`: levantar/validar estado `abctl local status`.
  - `scripts/check-health.*`: health check operativo de Airbyte.
- No se usa actualmente:
  - Conexiones/sync jobs que muevan `landing -> ClickHouse`.
  - Integración API de Airbyte en DAGs.

Para detalle de alternativas y plan de integración futura, ver [docs/airbyte_integration_options.md](docs/airbyte_integration_options.md).

## Contrato de watermark y detección de cambios en landing

El DAG `bank_market_pipeline` utiliza una marca de agua (`bank_market_pipeline__landing_ingested_at_watermark`) para decidir si corre transformaciones downstream:

1. Calcula `MAX(ingested_at)` sobre `landing.bank_basic_info`, `landing.stock_daily_price`, `landing.bank_fundamentals`, `landing.holders`, `landing.ratings`.
2. Si `MAX(ingested_at)` no supera el watermark, se hace `skip` de dbt/OLAP/mart.
3. Excepción de drift: si ClickHouse staging está vacío y landing tiene datos, se fuerza resync completo.
4. Al finalizar exitosamente, `commit_watermark` persiste la nueva marca.

Esto asume que cualquier mutación relevante en landing actualiza `ingested_at`.

## Semántica `stg_*` vs `cur_*`

- `olap.stg_*`: capa técnica **append/versionada** (física). Puede crecer por re-corridas; `count()` incluye versiones no fusionadas.
- `olap.cur_*`: capa de negocio **deduplicada** por llave natural.
- `olap.mart_monthly_stock_summary`: agregado mensual determinista construido desde `cur_*`.

Runbook mínimo para inspección operativa:

```powershell
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.stg_stock_daily_price"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.stg_stock_daily_price FINAL"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.cur_stock_daily_price"
docker compose exec -T clickhouse clickhouse-client --query "OPTIMIZE TABLE olap.stg_stock_daily_price FINAL"
```

`OPTIMIZE ... FINAL` debe usarse solo en ventanas controladas de mantenimiento (costo alto de CPU/IO).

## Servicios de Airflow en Docker Compose

El stack local utiliza una separación explícita de servicios de Airflow para mejorar mantenibilidad y diagnóstico:

* `airflow-init`: ejecuta migraciones y crea el usuario administrador
* `airflow-webserver`: expone la UI en el puerto configurado
* `airflow-scheduler`: ejecuta el scheduler

Esta separación evita ocultar fallos del webserver detrás de un solo contenedor con múltiples procesos y simplifica el troubleshooting.

Para ejecutar tareas `dbt` desde el DAG (`dbt_validation`, `monthly_summary`), los servicios de Airflow usan una imagen personalizada definida en `infra/airflow/Dockerfile` que instala **`dbt-postgres` y `dbt-clickhouse`**. El Compose también monta:

* `./dbt` en `/opt/airflow/dbt`
* `./infra/dbt` en `/opt/airflow/infra/dbt`

Esto mantiene al DAG autocontenible en el runtime de Airflow sin depender de comandos externos al contenedor.

## Consideraciones sobre la extracción desde Yahoo Finance

La extracción fue diseñada teniendo en cuenta los **request limits del API**, por lo que el proceso debe ejecutarse de forma controlada y proporcional cuando sea necesario.

Para esto, se recomienda:

* paginar o segmentar las consultas por símbolo y/o periodo
* evitar ráfagas de solicitudes innecesarias
* aplicar reintentos controlados
* incorporar backoff en caso de errores temporales
* registrar adecuadamente los fallos de extracción
* hacer el proceso idempotente para soportar re-ejecuciones

## Calidad de datos

La validación de calidad se implementa con dbt y debe cubrir, como mínimo:

* completitud
* unicidad
* tipos de datos válidos
* rangos esperados
* consistencia entre tablas
* control de duplicados
* frescura de datos cuando aplique

También se deben documentar las reglas de calidad implementadas y su propósito.

## Validación OLAP y mart mensual (`mart_monthly_stock_summary`)

### Capas

| Capa | Ubicación | Notas |
|------|-----------|--------|
| Landing | PostgreSQL `landing.*` | Ingesta Airflow. |
| OLAP curated | ClickHouse `olap.cur_*` | Desde landing vía `load_olap`. |
| Mart mensual | ClickHouse `olap.mart_monthly_stock_summary` | dbt `target=olap`, modelo `dbt/models/olap/mart_monthly_stock_summary.sql`. |

### Comandos útiles (desde la raíz del repo)

```powershell
docker compose exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM olap"
docker compose exec -T clickhouse clickhouse-client --query "SELECT * FROM olap.mart_monthly_stock_summary ORDER BY symbol, year, month LIMIT 20"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.mart_monthly_stock_summary"
docker compose exec -T clickhouse clickhouse-client --query "SELECT symbol, year, month, count() AS c FROM olap.mart_monthly_stock_summary GROUP BY symbol, year, month HAVING c > 1"
docker compose exec -T clickhouse clickhouse-client --query "SELECT year, count() FROM olap.mart_monthly_stock_summary GROUP BY year ORDER BY year"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() AS invalid_rows FROM olap.mart_monthly_stock_summary WHERE avg_open_price < 0 OR avg_close_price < 0 OR avg_volume < 0 OR trading_days_count <= 0"
```

### dbt (dos targets)

* **dev** (Postgres): `dbt run --target dev --select path:models/staging path:models/marts`
* **olap** (ClickHouse): `dbt run --target olap --select path:models/olap` (requiere `DBT_CH_*` en el entorno; ver `.env.example`).

Documentación ampliada: `docs/mart_monthly_stock_summary.md`.

### Criterios de aceptación (resumen)

* Tabla `olap.mart_monthly_stock_summary` existe y tiene datos cuando `cur_stock_daily_price` tiene filas en ventana.
* Unicidad lógica (`symbol`, `year`, `month`); años 2024–2025; métricas no negativas; `dbt test --target olap` en verde.
* Airflow: tareas `validate_olap_curated` y `validate_monthly_mart` fallan si hay datos upstream pero OLAP/mart vacíos (evita éxito falso).

### Screenshots sugeridos (entrega)

1. Airflow: grafo del DAG `bank_market_pipeline` con tasks en verde.
2. `dbt test --target olap` finalizado sin errores.
3. Resultado de `SELECT * FROM olap.mart_monthly_stock_summary LIMIT 20`.
4. Consulta de duplicados (cero filas esperadas).

## Buenas prácticas aplicadas

La solución está orientada a estándares de ingeniería de nivel producción, priorizando:

* modularidad
* separación de responsabilidades
* configuraciones externas por variables de entorno
* seguridad básica de secretos
* código legible y mantenible
* procesos reproducibles con Docker
* validaciones explícitas
* documentación clara
* facilidad de troubleshooting

## Archivos importantes

* `docker-compose.yml`: orquestación principal del entorno core
* `scripts/bootstrap.sh` / `scripts/bootstrap.ps1`: levantamiento del entorno local
* `scripts/check-health.sh` / `scripts/check-health.ps1`: verificación de salud
* `infra/postgres/`: configuración de PostgreSQL
* `infra/clickhouse/`: configuración de ClickHouse
* `infra/airbyte/`: plantillas opcionales para integración con la API de Airbyte
* `infra/airflow/`: configuración de Airflow
* `infra/dbt/`: configuración de dbt
* `dbt/`: proyecto dbt con modelos, snapshots y tests

## Estado esperado de Airflow

Tras ejecutar bootstrap y completar la inicialización, el estado esperado es:

* `airflow-init`: **completado** (`Exited (0)`) como contenedor one-shot esperado.
* `airflow-webserver`: **healthy** (`running` + healthcheck OK).
* `airflow-scheduler`: **running**.
* El endpoint `http://localhost:8080/health` responde HTTP 200 cuando el webserver está saludable.

## Ejecución esperada del proyecto

El flujo ideal de ejecución para un usuario nuevo sería:

1. Clonar el repositorio.
2. Crear los archivos `.env` a partir de los `.env.example`.
3. Completar las variables requeridas.
4. Generar y colocar una `FERNET_KEY` y una `WEBSERVER__SECRET_KEY` válidas.
5. Ejecutar bootstrap (`scripts/bootstrap.*`) para levantar servicios core con Compose y Airbyte con `abctl`.
6. Verificar el estado de salud de los servicios.
7. Ejecutar el pipeline.
8. Validar la carga en PostgreSQL y ClickHouse.
9. Revisar la tabla de resumen mensual.
10. Consultar logs y documentación en caso de error.

## Evidencias y documentación adicional

Checklist y comandos reproducibles de evidencia:

- [docs/evidence/README.md](docs/evidence/README.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/compliance_checklist.md](docs/compliance_checklist.md)
- [docs/data_mapping.md](docs/data_mapping.md)

## Troubleshooting básico

### 1) `postgres` no inicia o falla healthcheck

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 postgres`
* Verificar variables en `infra/postgres/postgres.env` contra el ejemplo

### 2) `clickhouse` no inicia o no responde

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 clickhouse`
* Validar puerto, usuario y contraseña en `infra/clickhouse/clickhouse.env`

### 3) `airflow-webserver` en `unhealthy` o sin UI

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 airflow-webserver`
* Verificar `AIRFLOW__CORE__FERNET_KEY` y `AIRFLOW__WEBSERVER__SECRET_KEY`
* Confirmar la conexión de metadata DB hacia PostgreSQL
* Revisar el resultado de `airflow-init`

### 4) `airflow-init` falla migraciones o creación de usuario

* Ver logs: `docker compose logs --tail=100 airflow-init`
* Confirmar conectividad con PostgreSQL
* Validar variables de admin y cadena `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
* Confirmar que existen la base `AIRFLOW_DB_NAME` y el rol `AIRFLOW_DB_USER` (se crean en el primer arranque con volumen vacío vía `infra/postgres/init/`). Si el volumen de Postgres es antiguo, recrea el volumen: `docker compose down -v` y vuelve a levantar.

### 5) `airflow-scheduler` no arranca

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 airflow-scheduler`
* Verificar que `airflow-init` haya terminado exitosamente

### 6) `dbt` falla conexión o comandos

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 dbt`
* Revisar `infra/dbt/dbt.env` y `infra/dbt/profiles.yml`
* Si el error es `/bin/bash: dbt: command not found` en tareas de Airflow, reconstruir imagen y recrear servicios:
  * `docker compose build airflow-init airflow-webserver airflow-scheduler`
  * `docker compose up -d airflow-webserver airflow-scheduler --force-recreate`

### 7) `dbt` muestra `No such command 'sleep'`

La imagen oficial de dbt puede traer un entrypoint que ejecuta `dbt` directamente. En ese caso, `command: ["sleep", "infinity"]` termina interpretándose como `dbt sleep infinity`.

La solución es sobrescribir el entrypoint en `docker-compose.yml`, por ejemplo:

```yaml
entrypoint: []
command: ["sleep", "infinity"]
```

### 8) Airbyte no disponible

* Ver estado: `abctl local status`
* Iniciar si está detenido: `abctl local start`
* Reinstalar o provisionar si no existe instalación local: `abctl local install`
* Ver logs: `abctl local logs`

### 9) Verificación integral falla

* Ejecutar `bash scripts/check-health.sh` o `powershell -ExecutionPolicy Bypass -File .\scripts\check-health.ps1`
* Corregir primero los checks `FAIL` y luego re-ejecutar el script

## Proyecto dbt en `dbt/`

Se agregó un proyecto dbt desacoplado en la carpeta `dbt/` con organización por capas:

* `models/sources.yml`: fuentes de `landing` con configuración de `freshness`, descripciones, metadatos (`meta`) y owner funcional
* `models/staging/`: modelos `stg_*` para estandarizar datos crudos
* `models/marts/`: modelos analíticos orientados a OLAP (dimensiones, hechos y agregado mensual)
* `snapshots/`: snapshot SCD para mantener histórico de cambios en dimensiones lentas
* `tests/`: tests custom de reglas de negocio

### Reglas de calidad de datos implementadas

Las siguientes reglas quedaron declaradas en dbt para validación continua:

1. **Completitud de llaves y campos críticos (`not_null`)**
   Se valida que llaves y fechas de negocio no lleguen nulas en `sources`, `staging` y `marts`.

2. **Unicidad de entidades (`unique`)**
   Se valida unicidad de llaves sustitutas en hechos y agregados, y del símbolo en la dimensión maestra.

3. **Dominios controlados (`accepted_values`)**
   Se restringen valores permitidos para campos categóricos como:

   * `period_type`: `annual`, `quarterly`, `ttm`
   * `holder_type`: `institution`, `insider`, `mutual_fund`, `major`

4. **Integridad referencial (`relationships`)**
   Se valida que los hechos (`fct_*` y agregados) referencien símbolos existentes en `dim_bank_profile`.

5. **Consistencia de precios OHLC (test custom)**
   Se valida que:

   * `high_price >= low_price`
   * `high_price >= max(open_price, close_price)`
   * `low_price <= min(open_price, close_price)`
   * `open/high/low/close >= 0`

6. **Volumen no negativo (test custom)**
   Se valida `volume >= 0` en el hecho diario de mercado.

7. **Frescura por fecha de carga (test custom + source freshness)**
   Se valida que la carga más reciente (`max(ingested_at)`) no supere 3 días de antigüedad, y además se configura `source freshness` con umbrales de advertencia y error.

### Materializaciones y snapshots

* **Materialización en marts:** en `dbt/models/marts/` hay modelos incrementales (`delete+insert`) y modelos materializados como `table` (por ejemplo `monthly_bank_stock_summary`).
* **Snapshot SCD:** `snap_dim_bank_profile` usa estrategia por `timestamp` (`ingested_at`) para preservar histórico de cambios de la dimensión de perfiles de banco

Esto permite mantener costos de procesamiento bajos en cargas recurrentes y, al mismo tiempo, conservar trazabilidad histórica para análisis temporal.

