# Mart mensual `mart_monthly_stock_summary` (ClickHouse OLAP)

## Rol en el pipeline

| Capa | Motor | Descripción breve |
|------|--------|---------------------|
| **landing** | PostgreSQL | Ingesta cruda / UPSERT desde Airflow. |
| **stg_*** / **cur_*** | ClickHouse `olap` | Staging versionado + curated deduplicado desde landing (tarea `load_olap`). |
| **staging / marts** (dbt `dev`) | PostgreSQL `analytics` | Modelos dbt sobre `landing` para analytics relacional. |
| **mart_monthly_stock_summary** (dbt `olap`) | ClickHouse `olap` | Agregación mensual desde `cur_stock_daily_price` + `cur_bank_basic_info`. |

## Grano y columnas

- **Grano:** `symbol` × `year` × `month` (dentro de la ventana configurable, por defecto 2024-01-01 a 2025-12-31).
- **Materialización:** `table` en ClickHouse (rebuild completo por ejecución; volumen acotado; idempotencia lógica por reemplazo de tabla).
- **Columnas:** ver `dbt/models/olap/mart_monthly_stock_summary.yml` (documentación dbt y tests declarativos).

## Supuestos

1. `cur_*` ya refleja la deduplicación operativa (ReplacingMergeTree + lógica de rebuild).
2. `company_name` se obtiene con `argMax(company_name, ingested_at)` por `symbol` en `cur_bank_basic_info`.
3. `trading_days_count` = `uniqExact(price_date)` en el mes (días con al menos una fila tras filtros).
4. `source_row_count` = filas diarias agregadas; con datos deduplicados por día debería ser ≥ `trading_days_count`.

## Cómo ejecutar dbt

Dentro del contenedor `dbt` (o Airflow, mismas rutas):

```bash
dbt run --target dev --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt --select path:models/staging path:models/marts
dbt test --target dev --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt --select path:models/staging path:models/marts path:tests --exclude path:tests/olap
dbt run --target olap --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt --select path:models/olap
dbt test --target olap --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt --select path:models/olap path:tests/olap
```

> El target `olap` requiere `dbt-clickhouse` y variables `DBT_CH_*` (ver `.env.example`).

## Consultas ClickHouse de validación

Ver README sección “Validación OLAP y mart mensual” para el set completo de comandos.

## Criterios de aceptación (fase mart)

1. Tabla `olap.mart_monthly_stock_summary` existe y tiene filas cuando `cur_stock_daily_price` tiene datos en ventana.
2. Sin duplicados en (`symbol`, `year`, `month`).
3. Años solo 2024 y 2025; métricas no negativas; `trading_days_count` > 0.
4. `dbt test --target olap` pasa (incluye singulares bajo `tests/olap/`).
5. Airflow: gates `validate_olap_curated` y `validate_monthly_mart` evitan éxito falso si OLAP o mart quedan vacíos con upstream poblado.

## Limitaciones

- El mart depende de que `load_olap` haya corrido antes del `dbt run --target olap`.
- Cobertura “al menos un mes por símbolo” se valida respecto a símbolos presentes en `cur_stock_daily_price` en la ventana; no impone calendario completo de meses si un banco cotiza solo parte del año.
