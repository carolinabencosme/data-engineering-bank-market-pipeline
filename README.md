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

- **Docker Compose**: orquestación local de todos los servicios
- **PostgreSQL**: landing zone para datos crudos
- **ClickHouse**: entorno OLAP para datos curados y explotación analítica
- **Airbyte**: integración de datos entre etapas del pipeline
- **dbt**: validación, modelado y transformación de datos
- **Airflow**: orquestación y automatización del pipeline
- **Python + yfinance**: extracción de datos desde Yahoo Finance

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
├── infra/
│   ├── airbyte/
│   ├── airflow/
│   ├── clickhouse/
│   ├── dbt/
│   └── postgres/
├── docker-compose.yml
├── .env.example
└── README.md
```

Cada servicio tiene su carpeta dedicada dentro de `infra/` para mantener una organización clara y facilitar la configuración independiente de cada componente.

## Configuración del entorno

La configuración evita hardcodear secretos: las credenciales y parámetros sensibles se inyectan mediante variables de entorno.

Este repositorio **no incluye archivos `.env` reales ni secretos**.  
Solo se versionan archivos de ejemplo sanitizados para mantener la seguridad del proyecto y facilitar su reproducción.

### Archivos de ejemplo incluidos

- `.env.example`
- `infra/postgres/postgres.env.example`
- `infra/clickhouse/clickhouse.env.example`
- `infra/airbyte/airbyte.env.example`
- `infra/airflow/airflow.env.example`
- `infra/dbt/dbt.env.example`

### Pasos de configuración

#### En Linux / macOS

```bash
cp .env.example .env
cp infra/postgres/postgres.env.example infra/postgres/postgres.env
cp infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
cp infra/airbyte/airbyte.env.example infra/airbyte/airbyte.env
cp infra/airflow/airflow.env.example infra/airflow/airflow.env
cp infra/dbt/dbt.env.example infra/dbt/dbt.env
```

#### En PowerShell (Windows)

```powershell
Copy-Item .env.example .env
Copy-Item infra/postgres/postgres.env.example infra/postgres/postgres.env
Copy-Item infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env
Copy-Item infra/airbyte/airbyte.env.example infra/airbyte/airbyte.env
Copy-Item infra/airflow/airflow.env.example infra/airflow/airflow.env
Copy-Item infra/dbt/dbt.env.example infra/dbt/dbt.env
```

Luego de eso, completa los valores requeridos en los archivos `.env` locales.

### Variables sensibles esperadas

Entre las variables que deben configurarse están:

- usuarios y contraseñas de PostgreSQL y ClickHouse
- credenciales de Airflow
- `AIRFLOW__CORE__FERNET_KEY`
- `AIRFLOW__WEBSERVER__SECRET_KEY`
- nombres de base de datos, puertos y hosts
- parámetros de entorno como `ENVIRONMENT` y `LOG_LEVEL`

### Nota de seguridad

Los archivos `.env` reales están excluidos del control de versiones mediante `.gitignore`.

No se deben subir al repositorio:
- contraseñas
- tokens
- API keys
- credenciales reales
- configuraciones sensibles locales

## Levantamiento del entorno

Aunque el stack puede levantarse completo con un solo comando, para facilitar troubleshooting se recomienda hacerlo por etapas.

### 1. Bases de datos

```bash
docker compose up -d postgres clickhouse
```

### 2. Integración y orquestación

```bash
docker compose up -d airbyte airflow
```

### 3. Capa de transformación

```bash
docker compose up -d dbt
```

### 4. Todo el stack

```bash
docker compose up -d
```

## Validación de salud de los servicios

### Estado general

```bash
docker compose ps
```

### Checks por servicio

#### PostgreSQL

```bash
docker compose exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

#### ClickHouse

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT 1"
```

#### Airbyte

```bash
curl -fsS "http://localhost:${AIRBYTE_PORT}/api/v1/health"
```

#### Airflow

```bash
curl -fsS "http://localhost:${AIRFLOW_WEBSERVER_PORT}/health"
```

#### dbt

```bash
docker compose exec dbt dbt --version
```

Si algún servicio queda en estado `unhealthy`, revisar logs con:

```bash
docker compose logs --tail=100 <service>
```

## Flujo del pipeline

De forma general, el pipeline sigue esta secuencia:

1. El extractor en Python consulta Yahoo Finance.
2. Los datos crudos se cargan en PostgreSQL.
3. Airbyte mueve o integra la información entre capas según el diseño del flujo.
4. dbt valida y transforma los datos.
5. Los datos curados se cargan en ClickHouse.
6. Se genera una tabla de resumen mensual.
7. Airflow automatiza la ejecución y actualización del pipeline.

## Consideraciones sobre la extracción desde Yahoo Finance

La extracción fue diseñada teniendo en cuenta los **request limits del API**, por lo que el proceso debe ejecutarse de forma controlada y proporcional cuando sea necesario.

Para esto, se recomienda:

- paginar o segmentar las consultas por símbolo y/o periodo
- evitar ráfagas de solicitudes innecesarias
- aplicar reintentos controlados
- incorporar backoff en caso de errores temporales
- registrar adecuadamente los fallos de extracción
- hacer el proceso idempotente para soportar re-ejecuciones

## Calidad de datos

La validación de calidad se implementa con dbt y debe cubrir, como mínimo:

- completitud
- unicidad
- tipos de datos válidos
- rangos esperados
- consistencia entre tablas
- control de duplicados
- frescura de datos cuando aplique

También se deben documentar las reglas de calidad implementadas y su propósito.

## Tabla de resumen mensual

Como salida analítica, el pipeline debe construir una tabla mensual que incluya:

- promedio mensual de apertura de la acción
- promedio mensual de cierre de la acción
- promedio mensual del volumen de la acción

Esta tabla sirve como ejemplo de consumo analítico a partir de los datos curados en la capa OLAP.

## Buenas prácticas aplicadas

La solución está orientada a estándares de ingeniería de nivel producción, priorizando:

- modularidad
- separación de responsabilidades
- configuraciones externas por variables de entorno
- seguridad básica de secretos
- código legible y mantenible
- procesos reproducibles con Docker
- validaciones explícitas
- documentación clara
- facilidad de troubleshooting

## Archivos importantes

- `docker-compose.yml`: orquestación principal del entorno
- `infra/postgres/`: configuración de PostgreSQL
- `infra/clickhouse/`: configuración de ClickHouse
- `infra/airbyte/`: configuración de Airbyte
- `infra/airflow/`: configuración de Airflow
- `infra/dbt/`: configuración de dbt

## Ejecución esperada del proyecto

El flujo ideal de ejecución para un usuario nuevo sería:

1. Clonar el repositorio
2. Crear los archivos `.env` a partir de los `.env.example`
3. Completar las variables requeridas
4. Levantar el entorno con Docker Compose
5. Verificar el estado de salud de los servicios
6. Ejecutar el pipeline
7. Validar la carga en PostgreSQL y ClickHouse
8. Revisar la tabla de resumen mensual
9. Consultar logs y documentación en caso de error

## Evidencias y documentación adicional

Este README debe complementarse con:

- diagrama de arquitectura
- screenshots de los servicios levantados
- screenshots del UI de Airbyte, Airflow y otros componentes relevantes
- evidencia de ejecución del pipeline
- evidencia de validaciones en dbt
- evidencia del resumen mensual en la capa OLAP


## Troubleshooting básico

### Un servicio no levanta
- Revisar `docker compose ps`
- Revisar `docker compose logs --tail=100 <service>`

### Error por variables de entorno
- Verificar que todos los `.env` locales existan
- Comparar con los archivos `.env.example`

### Error de conexión entre servicios
- Revisar nombres de host, puertos y credenciales
- Confirmar que los contenedores estén en ejecución

### Airflow no carga correctamente
- Revisar variables de base de datos
- Revisar `FERNET_KEY` y `SECRET_KEY`
- Validar configuración en `airflow.cfg`

### dbt no conecta
- Revisar `profiles.yml`
- Confirmar variables `DBT_*`
- Verificar conectividad a la base de datos de destino

