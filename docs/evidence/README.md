# Evidence Pack de cierre

Este documento define la evidencia mínima para demostrar el cierre técnico del pipeline en una evaluación o handoff.

## Checklist de capturas

- [ ] `docker compose ps` con servicios core saludables (`postgres`, `clickhouse`, `airflow-webserver`, `airflow-scheduler`, `dbt`).
- [ ] `abctl local status` mostrando Airbyte disponible como plataforma opcional.
- [ ] UI de Airflow con el DAG `bank_market_pipeline` en verde (vista graph o grid).
- [ ] Log de `load_olap` mostrando inserts por tabla (`stg_*`).
- [ ] Resultado de `dbt test --target olap` sin fallas.
- [ ] Consulta a `olap.mart_monthly_stock_summary` con filas (`LIMIT 20`).
- [ ] Consulta de duplicados por grano (`symbol`, `year`, `month`) retornando 0 filas.
- [ ] Conteos de landing (PostgreSQL), curated y mart (ClickHouse).

## Comandos de verificación

Ejecutar desde la raíz del repositorio:

```powershell
docker compose ps
abctl local status
docker compose exec -T postgres psql -U pipeline -d bank_market -c "SELECT 'bank_basic_info' AS table_name, COUNT(*) FROM landing.bank_basic_info UNION ALL SELECT 'stock_daily_price', COUNT(*) FROM landing.stock_daily_price UNION ALL SELECT 'bank_fundamentals', COUNT(*) FROM landing.bank_fundamentals UNION ALL SELECT 'holders', COUNT(*) FROM landing.holders UNION ALL SELECT 'ratings', COUNT(*) FROM landing.ratings ORDER BY 1;"
docker compose exec -T clickhouse clickhouse-client --query "SELECT 'cur_stock_daily_price' AS table_name, count() AS rows FROM olap.cur_stock_daily_price UNION ALL SELECT 'mart_monthly_stock_summary', count() FROM olap.mart_monthly_stock_summary"
docker compose exec -T clickhouse clickhouse-client --query "SELECT symbol, year, month, count() AS c FROM olap.mart_monthly_stock_summary GROUP BY symbol, year, month HAVING c > 1 ORDER BY c DESC"
docker compose exec -T dbt dbt test --target olap --select path:models/olap path:tests/olap
```

## Resultado de referencia (última verificación)

- `docker compose ps`: servicios core en `Up` y healthchecks en verde.
- `abctl local status`: clúster `airbyte-abctl` detectado (`deployed`), acceso sugerido `http://localhost:8000`.
- Conteos landing:
  - `bank_basic_info=12`
  - `stock_daily_price=3012`
  - `bank_fundamentals=30`
  - `holders=120`
  - `ratings=2334`
- Conteos OLAP:
  - `olap.cur_stock_daily_price=3012`
  - `olap.mart_monthly_stock_summary=144`
- Duplicados de grano en mart (`symbol,year,month`): 0 filas.
- `dbt test --target dev` (subset holders): `PASS=2`.
- `dbt test --target olap`: `PASS=25`.

## Validación de idempotencia (2 corridas)

1. Ejecutar un DagRun manual de `bank_market_pipeline`.
2. Esperar estado `success`.
3. Ejecutar de nuevo el DagRun.
4. Repetir los comandos de conteo y duplicados.
5. Confirmar:
   - sin duplicados lógicos en `cur_*` y `mart_monthly_stock_summary`;
   - `dbt test --target olap` permanece en verde.

## Nota sobre `stg_*`

Las tablas `stg_*` son append/versionadas y pueden crecer físicamente entre corridas. Para contraste operativo usar:

```powershell
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.stg_stock_daily_price"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM olap.stg_stock_daily_price FINAL"
```
