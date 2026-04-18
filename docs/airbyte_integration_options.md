# Airbyte PG -> ClickHouse: opciones de integración

## Estado actual (implementado)

El pipeline productivo del repositorio usa:

- Airflow + Python para `landing -> olap.stg_* -> olap.cur_*`;
- dbt `target=olap` para `mart_monthly_stock_summary`.

Airbyte está provisionado con `abctl` solo como plataforma local opcional.

## Objetivo de esta guía

Definir una ruta segura para incorporar Airbyte sin romper idempotencia ni crear doble escritura entre `load_olap` (Airflow) y sync jobs de Airbyte.

## Recomendación de ownership

Elegir **una sola** de estas estrategias antes de implementar:

1. **Airflow-owned (recomendada para este repo):**
   - mantener `load_olap` como único escritor a ClickHouse;
   - Airbyte continúa fuera del path de datos.
2. **Airbyte-owned (futuro):**
   - Airbyte replica `landing -> ClickHouse`;
   - el DAG deja de escribir `stg_*` para evitar duplicidad;
   - `cur_*` y mart siguen orquestados por Airflow/dbt.

No mezclar ambas simultáneamente sin controles de partición/origen y reconciliación.

## Diseño mínimo si se adopta Airbyte-owned

### Source (PostgreSQL)

- Base: `bank_market`
- Esquema: `landing`
- Tablas: `bank_basic_info`, `stock_daily_price`, `bank_fundamentals`, `holders`, `ratings`
- Incremental cursor: `ingested_at`

### Destination (ClickHouse)

- Database: `olap`
- Tablas destino dedicadas para réplica (ejemplo `ab_stg_*`) o reemplazo explícito de `stg_*` con cutover controlado.

### Cursor y estado

- Cursor primario recomendado: `ingested_at` (debe avanzar en cada inserción/actualización relevante).
- Si no se puede garantizar ese contrato, definir `landing_batch_seq` monotónico.
- Evitar CDC/WAL en este proyecto por complejidad operacional para el alcance actual.

## Plan de cutover (sin downtime lógico)

1. Crear sync Airbyte en modo shadow hacia tablas `ab_stg_*`.
2. Comparar conteos y checks de calidad vs `stg_*` y `cur_*` actuales.
3. Ejecutar 2 corridas consecutivas y validar idempotencia del downstream (`cur_*`, mart).
4. Cambiar readers/downstream al nuevo origen.
5. Desactivar `load_olap` en DAG para eliminar doble escritura.

## Riesgos y controles

- **Doble escritura:** bloquear con ownership único y feature flag en DAG.
- **Saltos de cursor:** monitorear continuidad de `MAX(ingested_at)` por tabla.
- **Drift semántico:** validar con pruebas de reconciliación (conteos, duplicados, ventanas temporales).
