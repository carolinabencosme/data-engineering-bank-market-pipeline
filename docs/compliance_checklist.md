# Checklist de cumplimiento (requisitos 1-13 y lineamientos senior)

## Requisitos de la prueba

| Req | Estado | Evidencia principal |
|---|---|---|
| 1. Landing zone en Docker | Cumplido | PostgreSQL en `docker-compose.yml`, tablas `landing.*`, carga en `src/load/landing_psql_loader.py`. |
| 2. OLAP en Docker | Cumplido | ClickHouse en `docker-compose.yml`, capas `olap.stg_*`, `olap.cur_*`, mart OLAP. |
| 3. Entorno de integración | Cumplido (opcional documentado) | Airbyte operativo vía `abctl` + rol explícito no activo en path de datos (`README.md`, `docs/airbyte_integration_options.md`). |
| 4. Validación/transformación | Cumplido | dbt `target=dev` y `target=olap` con tests genéricos y singulares. |
| 5. Orquestación/automatización | Cumplido | DAGs `bank_market_pipeline` y `bank_market_landing_to_olap`. |
| 6. Diagrama de arquitectura | Cumplido | `docs/architecture.md` (Mermaid). |
| 7. Script Python Yahoo | Cumplido | `src/extract/yahoo_finance_client.py` (+ fallback controlado). |
| 8. Extracción 2024-2025 y dominios | Cumplido | Variables `PIPELINE_EXTRACT_*`, modelos y validaciones dbt. |
| 9. Carga a landing | Cumplido | UPSERT idempotente por llave natural en `src/load/landing_psql_loader.py`. |
| 10. Integración a OLAP + reglas dbt | Cumplido | `src/load/olap_clickhouse_loader.py`, gates y tests dbt. |
| 11. Tabla resumen mensual | Cumplido | `dbt/models/olap/mart_monthly_stock_summary.sql`. |
| 12. Automatización por nueva data landing | Cumplido | Watermark + skip + drift + DAG desacoplado landing->OLAP. |
| 13. Documentación GitHub reproducible | Cumplido | README + docs de arquitectura, evidencia, mart y troubleshooting. |

## Lineamientos obligatorios

| Línea | Estado | Evidencia principal |
|---|---|---|
| Arquitectura modular/desacoplada | Cumplido | Separación `src/extract`, `src/load`, `src/validate`, `dbt`, `airflow`. |
| Clean code y nombres consistentes | Cumplido | Estructura de módulos y funciones pequeñas; helpers centralizados para dbt en DAG. |
| Buenas prácticas ingeniería | Cumplido | Tipado Python, manejo de errores, logs estructurados, `.env.example`, Docker reproducible. |
| Calidad de datos | Cumplido | Tests dbt (`not_null`, `unique`, `accepted_values`, `relationships`) + singulares OLAP. |
| Performance y confiabilidad | Cumplido (con trade-off documentado) | Incremental landing->stg, rebuild curado, runbook `FINAL/OPTIMIZE`, retries/backoff Yahoo. |
| Seguridad y gobernanza | Cumplido | Variables externas, separación metadata Airflow/landing, trazabilidad `batch_id` y `ingested_at`. |
| Documentación y entrega | Cumplido | README operativo, arquitectura, evidence pack, troubleshooting, decisiones técnicas. |
| Estándar senior | Cumplido | Decisiones justificadas, idempotencia, quality gates, evidencia reproducible. |

## Validación ejecutada (última corrida)

- `docker compose ps`: servicios saludables.
- `docker compose exec -T airflow-webserver airflow dags list`: DAG principal y desacoplado visibles.
- `docker compose exec -T dbt dbt test --target dev ...`: PASS.
- `docker compose exec -T dbt dbt test --target olap --select path:models/olap path:tests/olap`: PASS (25/25).
- Queries de conteo/duplicados en Postgres y ClickHouse: sin anomalías de grano.

## Riesgos residuales controlados

1. `stg_*` puede crecer físicamente (estrategia append técnica): mitigado con runbook (`FINAL/OPTIMIZE`) y capa `cur_*` deduplicada.
2. Dependencia de `ingested_at` para watermark: contrato documentado y gates para evitar éxito falso.
3. Airbyte fuera del path activo: decisión explícita para evitar doble escritura sin strategy de cutover.
