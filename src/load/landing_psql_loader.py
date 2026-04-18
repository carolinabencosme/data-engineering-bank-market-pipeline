from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.extract.yahoo_finance_client import LandingRecord

LOGGER = logging.getLogger(__name__)


def scratch_batch_path(scratch_dir: str, batch_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", batch_id)
    return Path(scratch_dir) / f"{safe}.json"


def serialize_landing_records(records: Sequence[LandingRecord]) -> str:
    rows = [asdict(r) for r in records]
    return json.dumps(rows, default=_json_default)


def deserialize_landing_records(payload: str) -> list[LandingRecord]:
    rows = json.loads(payload)
    out: list[LandingRecord] = []
    for row in rows:
        rd = row.get("record_date")
        record_date: date | None
        if rd is None:
            record_date = None
        elif isinstance(rd, str):
            record_date = date.fromisoformat(rd)
        else:
            raise ValueError(f"Unexpected record_date type: {type(rd)!r}")
        lt = row["load_timestamp"]
        load_ts = datetime.fromisoformat(lt) if isinstance(lt, str) else lt
        if load_ts.tzinfo is None:
            load_ts = load_ts.replace(tzinfo=timezone.utc)
        out.append(
            LandingRecord(
                dataset=row["dataset"],
                symbol=row["symbol"],
                record_date=record_date,
                payload=row["payload"],
                source=row.get("source", "yahoo_finance"),
                load_timestamp=load_ts,
            )
        )
    return out


def _json_default(obj: Any) -> str:
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)!r} is not JSON serializable")


def _yahoo_raw(value: Any) -> Any:
    if isinstance(value, Mapping) and "raw" in value:
        return value.get("raw")
    return value


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    raw = _yahoo_raw(value)
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    raw = _yahoo_raw(value)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    raw = _yahoo_raw(value)
    if raw is None:
        return None
    try:
        ts = int(raw)
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()
    except (TypeError, ValueError, OSError):
        return None


def upsert_landing_records(
    connection: Any,
    records: Sequence[LandingRecord],
    *,
    batch_id: str,
    snapshot_date: date,
    source_system: str,
    ingested_at: datetime,
) -> dict[str, int]:
    """
    Upsert normalized landing rows derived from Yahoo ``LandingRecord`` payloads.

    Idempotency follows each table's natural key (see ``V001__create_landing_tables.sql``);
    ``batch_id`` and ``ingested_at`` are refreshed on conflict so downstream watermarking works.
    """
    if ingested_at.tzinfo is None:
        ingested_at = ingested_at.replace(tzinfo=timezone.utc)

    counts = {
        "bank_basic_info": 0,
        "stock_daily_price": 0,
        "bank_fundamentals": 0,
        "holders": 0,
        "ratings": 0,
        "skipped": 0,
    }
    by_symbol: dict[str, defaultdict[str, int]] = {
        "bank_basic_info": defaultdict(int),
        "stock_daily_price": defaultdict(int),
        "bank_fundamentals": defaultdict(int),
        "holders": defaultdict(int),
        "ratings": defaultdict(int),
    }

    basics_sql = """
        INSERT INTO landing.bank_basic_info (
            symbol, company_name, industry, sector, employee_count,
            city, phone, state, country, website, address,
            market_cap, currency, exchange, snapshot_date,
            ingested_at, source_system, batch_id
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (symbol, snapshot_date) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            industry = EXCLUDED.industry,
            sector = EXCLUDED.sector,
            employee_count = EXCLUDED.employee_count,
            city = EXCLUDED.city,
            phone = EXCLUDED.phone,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            website = EXCLUDED.website,
            address = EXCLUDED.address,
            market_cap = EXCLUDED.market_cap,
            currency = EXCLUDED.currency,
            exchange = EXCLUDED.exchange,
            ingested_at = EXCLUDED.ingested_at,
            source_system = EXCLUDED.source_system,
            batch_id = EXCLUDED.batch_id;
    """

    price_sql = """
        INSERT INTO landing.stock_daily_price (
            symbol, price_date, open_price, high_price, low_price, close_price,
            adjusted_close, volume, ingested_at, source_system, batch_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (symbol, price_date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            adjusted_close = EXCLUDED.adjusted_close,
            volume = EXCLUDED.volume,
            ingested_at = EXCLUDED.ingested_at,
            source_system = EXCLUDED.source_system,
            batch_id = EXCLUDED.batch_id;
    """

    fundamentals_sql = """
        INSERT INTO landing.bank_fundamentals (
            symbol, statement_date, period_type,
            total_assets, total_debt, invested_capital, shares_issued,
            currency, ingested_at, source_system, batch_id
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (symbol, statement_date, period_type) DO UPDATE SET
            total_assets = EXCLUDED.total_assets,
            total_debt = EXCLUDED.total_debt,
            invested_capital = EXCLUDED.invested_capital,
            shares_issued = EXCLUDED.shares_issued,
            currency = EXCLUDED.currency,
            ingested_at = EXCLUDED.ingested_at,
            source_system = EXCLUDED.source_system,
            batch_id = EXCLUDED.batch_id;
    """

    holders_sql = """
        INSERT INTO landing.holders (
            symbol, holder_type, holder_name, holdings_date,
            shares, market_value, pct_outstanding,
            ingested_at, source_system, batch_id
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (symbol, holder_type, holder_name, holdings_date) DO UPDATE SET
            shares = EXCLUDED.shares,
            market_value = EXCLUDED.market_value,
            pct_outstanding = EXCLUDED.pct_outstanding,
            ingested_at = EXCLUDED.ingested_at,
            source_system = EXCLUDED.source_system,
            batch_id = EXCLUDED.batch_id;
    """

    ratings_sql = """
        INSERT INTO landing.ratings (
            symbol, rating_date, firm_name, to_grade, from_grade, rating_action,
            recommendation_score, ingested_at, source_system, batch_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (symbol, rating_date, firm_name) DO UPDATE SET
            to_grade = EXCLUDED.to_grade,
            from_grade = EXCLUDED.from_grade,
            rating_action = EXCLUDED.rating_action,
            recommendation_score = EXCLUDED.recommendation_score,
            ingested_at = EXCLUDED.ingested_at,
            source_system = EXCLUDED.source_system,
            batch_id = EXCLUDED.batch_id;
    """

    try:
        with connection.cursor() as cur:
            for record in records:
                p = record.payload
                if record.dataset == "basics":
                    sym = (record.symbol or "").strip().upper()[:20]
                    if not sym:
                        LOGGER.warning(json.dumps({"event": "landing_skip", "reason": "missing_symbol", "dataset": "basics"}))
                        counts["skipped"] += 1
                        continue
                    if not any(
                        p.get(k) not in (None, "", 0)
                        for k in ("company_name", "industry", "sector", "country", "website", "fullTimeEmployees")
                    ):
                        LOGGER.warning(
                            json.dumps(
                                {
                                    "event": "landing_skip",
                                    "reason": "hollow_basics_payload",
                                    "dataset": "basics",
                                    "symbol": sym,
                                },
                                default=str,
                            )
                        )
                        counts["skipped"] += 1
                        continue
                    company = p.get("company_name")
                    company_name = str(company).strip() if company not in (None, "") else None
                    exch = p.get("exchange")
                    curr = p.get("currency")
                    cur.execute(
                        basics_sql,
                        (
                            sym,
                            company_name,
                            p.get("industry"),
                            p.get("sector"),
                            _to_int(p.get("fullTimeEmployees")),
                            p.get("city"),
                            p.get("phone"),
                            p.get("state"),
                            p.get("country"),
                            p.get("website"),
                            p.get("address1"),
                            _to_decimal(p.get("market_cap")),
                            (str(curr)[:10] if curr not in (None, "") else None),
                            (str(exch)[:20] if exch not in (None, "") else None),
                            snapshot_date,
                            ingested_at,
                            source_system,
                            batch_id,
                        ),
                    )
                    counts["bank_basic_info"] += cur.rowcount
                    by_symbol["bank_basic_info"][sym] += cur.rowcount
                elif record.dataset == "daily_prices":
                    sym = (record.symbol or "").strip().upper()[:20]
                    if not sym:
                        counts["skipped"] += 1
                        continue
                    if record.record_date is None:
                        counts["skipped"] += 1
                        continue
                    o = _to_decimal(p.get("open"))
                    h = _to_decimal(p.get("high"))
                    lo = _to_decimal(p.get("low"))
                    c = _to_decimal(p.get("close"))
                    v_raw = _to_int(p.get("volume"))
                    if o is None or h is None or lo is None or c is None or v_raw is None:
                        counts["skipped"] += 1
                        continue
                    v = int(v_raw)
                    cur.execute(
                        price_sql,
                        (
                            sym,
                            record.record_date,
                            o,
                            h,
                            lo,
                            c,
                            None,
                            v,
                            ingested_at,
                            source_system,
                            batch_id,
                        ),
                    )
                    counts["stock_daily_price"] += cur.rowcount
                    by_symbol["stock_daily_price"][sym] += cur.rowcount
                elif record.dataset == "fundamentals":
                    sym = (record.symbol or "").strip().upper()[:20]
                    if not sym:
                        counts["skipped"] += 1
                        continue
                    for row in _fundamental_rows(sym, p, snapshot_date):
                        if row.get("statement_date") is None:
                            counts["skipped"] += 1
                            continue
                        if row.get("period_type") is None or str(row["period_type"]).strip() == "":
                            counts["skipped"] += 1
                            continue
                        cur.execute(
                            fundamentals_sql,
                            (
                                row["symbol"],
                                row["statement_date"],
                                row["period_type"],
                                row["total_assets"],
                                row["total_debt"],
                                row["invested_capital"],
                                row["shares_issued"],
                                row["currency"],
                                ingested_at,
                                source_system,
                                batch_id,
                            ),
                        )
                        counts["bank_fundamentals"] += cur.rowcount
                        by_symbol["bank_fundamentals"][sym] += cur.rowcount
                elif record.dataset == "holders":
                    sym = (record.symbol or "").strip().upper()[:20]
                    if not sym:
                        counts["skipped"] += 1
                        continue
                    rows = list(_holder_rows(record, snapshot_date))
                    if not rows:
                        counts["skipped"] += 1
                        continue
                    for row in rows:
                        if not row.get("holder_name"):
                            counts["skipped"] += 1
                            continue
                        if row.get("holdings_date") is None:
                            counts["skipped"] += 1
                            continue
                        cur.execute(
                            holders_sql,
                            (
                                row["symbol"],
                                row["holder_type"],
                                row["holder_name"],
                                row["holdings_date"],
                                row["shares"],
                                row["market_value"],
                                row["pct_outstanding"],
                                ingested_at,
                                source_system,
                                batch_id,
                            ),
                        )
                        counts["holders"] += cur.rowcount
                        by_symbol["holders"][sym] += cur.rowcount
                elif record.dataset == "ratings":
                    sym = (record.symbol or "").strip().upper()[:20]
                    if not sym:
                        counts["skipped"] += 1
                        continue
                    rd = record.record_date or _to_date(p.get("epochGradeDate"))
                    if rd is None:
                        counts["skipped"] += 1
                        continue
                    firm = (p.get("firm") or "").strip() or "UNKNOWN_FIRM"
                    to_g = p.get("to_grade")
                    from_g = p.get("from_grade")
                    act = p.get("action")
                    rec_score = _to_decimal(p.get("recommendation_score"))
                    cur.execute(
                        ratings_sql,
                        (
                            sym,
                            rd,
                            firm[:500] if isinstance(firm, str) else "UNKNOWN_FIRM",
                            str(to_g)[:50] if to_g is not None else None,
                            str(from_g)[:50] if from_g is not None else None,
                            str(act)[:30] if act is not None else None,
                            rec_score,
                            ingested_at,
                            source_system,
                            batch_id,
                        ),
                    )
                    counts["ratings"] += cur.rowcount
                    by_symbol["ratings"][sym] += cur.rowcount
                else:
                    counts["skipped"] += 1

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    by_symbol_out = {table: dict(syms) for table, syms in by_symbol.items() if syms}
    LOGGER.info(
        json.dumps(
            {
                "event": "landing_upsert_complete",
                "batch_id": batch_id,
                "counts": counts,
                "rows_upserted_by_symbol": by_symbol_out,
            },
            default=str,
        )
    )
    return counts


def _fundamental_rows(
    symbol: str,
    payload: Mapping[str, Any],
    snapshot_date: date,
) -> list[dict[str, Any]]:
    sym = symbol[:20]
    ta_top = _to_decimal(payload.get("totalAssets"))
    td_top = _to_decimal(payload.get("totalDebt"))
    ic_top = _to_decimal(payload.get("investedCapital"))
    sh_top = _to_decimal(payload.get("sharesOutstanding"))
    statements = payload.get("balanceSheetStatements") or []
    out: list[dict[str, Any]] = []
    for stmt in statements:
        if not isinstance(stmt, Mapping):
            continue
        ed = _to_date(stmt.get("endDate"))
        if ed is None:
            continue
        ta = _to_decimal(stmt.get("totalAssets")) or ta_top
        td = _to_decimal(stmt.get("totalDebt")) or td_top
        ic = ic_top
        sh = sh_top
        out.append(
            {
                "symbol": sym,
                "statement_date": ed,
                "period_type": "quarterly",
                "total_assets": ta,
                "total_debt": td,
                "invested_capital": ic,
                "shares_issued": sh,
                "currency": None,
            }
        )
    if not out and any(x is not None for x in (ta_top, td_top, ic_top, sh_top)):
        out.append(
            {
                "symbol": sym,
                "statement_date": snapshot_date,
                "period_type": "ttm",
                "total_assets": ta_top,
                "total_debt": td_top,
                "invested_capital": ic_top,
                "shares_issued": sh_top,
                "currency": None,
            }
        )
    return out


def _holder_rows(record: LandingRecord, snapshot_date: date) -> list[dict[str, Any]]:
    p = record.payload
    sym = record.symbol[:20]
    out: list[dict[str, Any]] = []

    if p.get("majorHoldersBreakdown") is not None and p.get("holder") is None:
        name = "_major_holders_breakdown"
        out.append(
            {
                "symbol": sym,
                "holder_type": "major",
                "holder_name": name,
                "holdings_date": snapshot_date,
                "shares": None,
                "market_value": None,
                "pct_outstanding": None,
            }
        )
        return out

    hd = record.record_date
    if hd is None:
        return []
    holder = (p.get("holder") or "").strip() or "UNKNOWN_HOLDER"
    htype = (p.get("holder_type") or "unknown").strip().lower()[:30]
    if htype == "fund":
        htype = "mutual_fund"
    out.append(
        {
            "symbol": sym,
            "holder_type": htype,
            "holder_name": holder[:2000],
            "holdings_date": hd,
            "shares": _to_decimal(p.get("shares")),
            "market_value": _to_decimal(p.get("value")),
            "pct_outstanding": None,
        }
    )
    return out


def pipeline_source_system() -> str:
    return os.getenv("PIPELINE_SOURCE_SYSTEM", "yahoo_finance")


def pipeline_scratch_dir() -> str:
    return os.getenv("PIPELINE_XCOM_SCRATCH_DIR", "/opt/airflow/dags/_pipeline_scratch")
