# Data Mapping: Source -> Landing -> OLAP

Este documento describe el mapeo campo-a-campo para las tablas solicitadas.

## Convenciones generales

- **source_system**: identificador del origen (`yahoo_finance`, `sec_filings`, etc.).
- **batch_id**: identificador único de corrida de ingesta (ej. `20260412T020000Z`).
- **ingested_at**: timestamp UTC de carga técnica.
- En **Landing (PostgreSQL)** se conserva el grano de negocio y se fuerza unicidad con PK compuesta.
- En **OLAP Staging (ClickHouse)** se permite versionamiento técnico por `ingested_at`.
- En **OLAP Curated (ClickHouse)** se deduplica con `argMax(..., ingested_at)` por llave de negocio.

---

## 1) bank_basic_info

**Grano de negocio:** un registro por `symbol + snapshot_date`.

| Source field | Landing (`landing.bank_basic_info`) | OLAP Staging (`olap.stg_bank_basic_info`) | OLAP Curated (`olap.cur_bank_basic_info`) | Transformación |
|---|---|---|---|---|
| ticker | symbol | symbol | symbol | Uppercase + trim |
| longName | company_name | company_name | company_name | Directo |
| industry | industry | industry | industry | Directo |
| sector | sector | sector | sector | Directo |
| fullTimeEmployees | employee_count | employee_count | employee_count | Cast a BIGINT/Int64 |
| city | city | city | city | Directo |
| phone | phone | phone | phone | Directo |
| state | state | state | state | Directo |
| country | country | country | country | Directo |
| website | website | website | website | Directo |
| address1 + address2 | address | address | address | Concatenación segura |
| marketCap | market_cap | market_cap | market_cap | Cast decimal |
| financialCurrency | currency | currency | currency | Directo |
| exchange | exchange | exchange | exchange | Directo |
| extraction_date | snapshot_date | snapshot_date | snapshot_date | Cast a DATE |
| system_name | source_system | source_system | source_system | Directo |
| run_id | batch_id | batch_id | batch_id | Directo |
| load_ts | ingested_at | ingested_at | ingested_at | UTC timestamp |

---

## 2) stock_daily_price

**Grano de negocio:** un registro por `symbol + price_date`.

| Source field | Landing (`landing.stock_daily_price`) | OLAP Staging (`olap.stg_stock_daily_price`) | OLAP Curated (`olap.cur_stock_daily_price`) | Transformación |
|---|---|---|---|---|
| ticker | symbol | symbol | symbol | Uppercase + trim |
| Date | price_date | price_date | price_date | Cast a DATE |
| Open | open_price | open_price | open_price | Cast decimal(18,6) |
| High | high_price | high_price | high_price | Cast decimal(18,6) |
| Low | low_price | low_price | low_price | Cast decimal(18,6) |
| Close | close_price | close_price | close_price | Cast decimal(18,6) |
| Adj Close | adjusted_close | adjusted_close | adjusted_close | Cast decimal(18,6) |
| Volume | volume | volume | volume | Cast BIGINT/UInt64 |
| system_name | source_system | source_system | source_system | Directo |
| run_id | batch_id | batch_id | batch_id | Directo |
| load_ts | ingested_at | ingested_at | ingested_at | UTC timestamp |

---

## 3) bank_fundamentals

**Grano de negocio:** un registro por `symbol + statement_date + period_type`.

| Source field | Landing (`landing.bank_fundamentals`) | OLAP Staging (`olap.stg_bank_fundamentals`) | OLAP Curated (`olap.cur_bank_fundamentals`) | Transformación |
|---|---|---|---|---|
| ticker | symbol | symbol | symbol | Uppercase + trim |
| statementDate | statement_date | statement_date | statement_date | Cast a DATE |
| periodType | period_type | period_type | period_type | annual/quarterly/ttm |
| totalAssets | total_assets | total_assets | total_assets | Cast decimal(22,2) |
| totalDebt | total_debt | total_debt | total_debt | Cast decimal(22,2) |
| investedCapital | invested_capital | invested_capital | invested_capital | Cast decimal(22,2) |
| shareIssued | shares_issued | shares_issued | shares_issued | Cast decimal(22,2) |
| currency | currency | currency | currency | Directo |
| system_name | source_system | source_system | source_system | Directo |
| run_id | batch_id | batch_id | batch_id | Directo |
| load_ts | ingested_at | ingested_at | ingested_at | UTC timestamp |

---

## 4) holders

**Grano de negocio:** un registro por `symbol + holder_type + holder_name + holdings_date`.

| Source field | Landing (`landing.holders`) | OLAP Staging (`olap.stg_holders`) | OLAP Curated (`olap.cur_holders`) | Transformación |
|---|---|---|---|---|
| ticker | symbol | symbol | symbol | Uppercase + trim |
| holderCategory | holder_type | holder_type | holder_type | institution/insider/etc. |
| Holder | holder_name | holder_name | holder_name | Trim |
| Date Reported | holdings_date | holdings_date | holdings_date | Cast a DATE |
| Shares | shares | shares | shares | Cast decimal(22,2) |
| Value | market_value | market_value | market_value | Cast decimal(22,2) |
| pctOutstanding | pct_outstanding | pct_outstanding | pct_outstanding | Decimal(9,6), rango 0-1 |
| system_name | source_system | source_system | source_system | Directo |
| run_id | batch_id | batch_id | batch_id | Directo |
| load_ts | ingested_at | ingested_at | ingested_at | UTC timestamp |

---

## 5) ratings

**Grano de negocio:** un registro por `symbol + rating_date + firm_name`.

| Source field | Landing (`landing.ratings`) | OLAP Staging (`olap.stg_ratings`) | OLAP Curated (`olap.cur_ratings`) | Transformación |
|---|---|---|---|---|
| ticker | symbol | symbol | symbol | Uppercase + trim |
| Date | rating_date | rating_date | rating_date | Cast a DATE |
| Firm | firm_name | firm_name | firm_name | Trim |
| To Grade | to_grade | to_grade | to_grade | Directo |
| From Grade | from_grade | from_grade | from_grade | Directo |
| Action | rating_action | rating_action | rating_action | upgrade/downgrade/reiterate |
| recommendationMean | recommendation_score | recommendation_score | recommendation_score | Cast decimal(8,4) |
| system_name | source_system | source_system | source_system | Directo |
| run_id | batch_id | batch_id | batch_id | Directo |
| load_ts | ingested_at | ingested_at | ingested_at | UTC timestamp |

---

## Políticas de deduplicación

1. **Landing (PostgreSQL):**
   - Claves primarias compuestas por grano de negocio para prevenir duplicados duros.
   - Cargas incrementales por `INSERT ... ON CONFLICT (...) DO UPDATE`.

2. **OLAP Staging (ClickHouse):**
   - `ReplacingMergeTree(ingested_at)` para mantener historial técnico de versiones.
   - `ORDER BY` comienza por `symbol` para eficiencia en filtros por símbolo.

3. **OLAP Curated (ClickHouse):**
   - Consolidación por llave de negocio con `argMax(col, ingested_at)` y `max(ingested_at)`.
   - Una fila vigente por llave de negocio en consumo analítico.