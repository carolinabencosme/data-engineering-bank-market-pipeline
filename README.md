````md
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
- **Airbyte**: integración de datos entre etapas del pipeline
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
   - Airbyte se utiliza para mover datos entre los componentes del pipeline.

4. **Validación y transformación**
   - dbt implementa reglas de calidad y transforma los datos hacia estructuras curadas.

5. **Capa analítica / OLAP**
   - ClickHouse centraliza la información curada para consumo analítico.

6. **Orquestación**
   - Airflow automatiza y coordina la ejecución del pipeline.

> **Nota:** El diagrama de arquitectura debe incluirse en la carpeta de documentación o en este README una vez finalizado.

## Estructura del proyecto

```text
.
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

Luego de eso, completa los valores requeridos en los archivos `.env` locales.

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
2. Los datos crudos se cargan en PostgreSQL.
3. Airbyte mueve o integra la información entre capas según el diseño del flujo.
4. dbt valida y transforma los datos.
5. Los datos curados se cargan en ClickHouse.
6. Se genera una tabla de resumen mensual.
7. Airflow automatiza la ejecución y actualización del pipeline.

## Servicios de Airflow en Docker Compose

El stack local utiliza una separación explícita de servicios de Airflow para mejorar mantenibilidad y diagnóstico:

* `airflow-init`: ejecuta migraciones y crea el usuario administrador
* `airflow-webserver`: expone la UI en el puerto configurado
* `airflow-scheduler`: ejecuta el scheduler

Esta separación evita ocultar fallos del webserver detrás de un solo contenedor con múltiples procesos y simplifica el troubleshooting.

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

## Tabla de resumen mensual

Como salida analítica, el pipeline debe construir una tabla mensual que incluya:

* promedio mensual de apertura de la acción
* promedio mensual de cierre de la acción
* promedio mensual del volumen de la acción

Esta tabla sirve como ejemplo de consumo analítico a partir de los datos curados en la capa OLAP.

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

## Estado esperado de Airflow (webserver)

Tras ejecutar bootstrap y completar `airflow-init`, el estado esperado es:

* `airflow-init` finaliza en `Exited (0)` (one-shot exitoso).
* `airflow-webserver` queda `Up` y su healthcheck en `healthy`.
* `airflow-scheduler` queda `Up`.
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

Este README debe complementarse con:

* diagrama de arquitectura
* screenshots de los servicios levantados
* screenshots del UI de Airbyte, Airflow y otros componentes relevantes
* evidencia de ejecución del pipeline
* evidencia de validaciones en dbt
* evidencia del resumen mensual en la capa OLAP

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

### 5) `airflow-scheduler` no arranca

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 airflow-scheduler`
* Verificar que `airflow-init` haya terminado exitosamente

### 6) `dbt` falla conexión o comandos

* Confirmar estado: `docker compose ps`
* Ver logs: `docker compose logs --tail=100 dbt`
* Revisar `infra/dbt/dbt.env` y `infra/dbt/profiles.yml`

### 7) `dbt` muestra `No such command 'sleep'`

La imagen oficial de dbt puede traer un entrypoint que ejecuta `dbt` directamente. En ese caso, `command: ["sleep", "infinity"]` termina interpretándose como `dbt sleep infinity`.

La solución es sobrescribir el entrypoint en `docker-compose.yml`, por ejemplo:

```yaml
entrypoint: ["/bin/sh", "-c"]
command: "sleep infinity"
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

* **Incremental en marts:** hechos, dimensión principal y agregado mensual usan materialización incremental con estrategia `delete+insert`
* **Snapshot SCD:** `snap_dim_bank_profile` usa estrategia por `timestamp` (`ingested_at`) para preservar histórico de cambios de la dimensión de perfiles de banco

Esto permite mantener costos de procesamiento bajos en cargas recurrentes y, al mismo tiempo, conservar trazabilidad histórica para análisis temporal.

````
