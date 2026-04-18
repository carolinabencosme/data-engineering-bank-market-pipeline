"""
Fallback extraction via yfinance when Yahoo ``quoteSummary`` HTTP fails or returns no rows.

Loaded lazily so local tooling without yfinance still imports ``yahoo_finance_client``.
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date, datetime, timezone
from typing import Any

LOGGER = logging.getLogger(__name__)


def yfinance_fallback_enabled() -> bool:
    raw = os.getenv("PIPELINE_YFINANCE_FALLBACK", "1").strip().lower()
    return raw in ("1", "true", "yes", "on")


def yfinance_package_available() -> bool:
    try:
        import yfinance  # noqa: F401

        return True
    except ImportError:
        return False


def _nan_to_none(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except TypeError:
        pass
    return value


def _raw_value(value: Any) -> dict[str, Any] | None:
    """Shape compatible with ``landing_psql_loader._to_decimal`` / ``_to_int``."""
    v = _nan_to_none(value)
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return None
    try:
        fv = float(v)
        if fv.is_integer() and abs(fv) <= 9e15:
            return {"raw": int(fv)}
        return {"raw": fv}
    except (TypeError, ValueError, OverflowError):
        return None


def extract_dataset_via_yfinance(symbol: str, dataset: str) -> list[Any]:
    from src.extract.yahoo_finance_client import LandingRecord

    if not yfinance_package_available():
        LOGGER.warning(
            json.dumps(
                {
                    "event": "yfinance_unavailable",
                    "symbol": symbol,
                    "dataset": dataset,
                    "message": "yfinance package not installed; install yfinance in the Airflow image",
                },
                default=str,
            )
        )
        return []

    import yfinance as yf

    sym = symbol.strip().upper()
    if not sym:
        return []

    ticker = yf.Ticker(sym)
    info = getattr(ticker, "info", None) or {}
    if not isinstance(info, dict):
        info = {}

    if dataset == "basics":
        company = info.get("longName") or info.get("shortName") or info.get("displayName")
        fields = {
            "company_name": company,
            "industry": info.get("industry"),
            "sector": info.get("sector"),
            "fullTimeEmployees": info.get("fullTimeEmployees"),
            "city": info.get("city"),
            "phone": info.get("phone"),
            "state": info.get("state"),
            "country": info.get("country"),
            "website": info.get("website"),
            "address1": info.get("address1"),
            "exchange": info.get("exchange"),
            "currency": info.get("currency"),
            "market_cap": info.get("marketCap"),
        }
        if not any(v not in (None, "", 0) for v in fields.values()):
            LOGGER.warning(
                json.dumps(
                    {
                        "event": "yfinance_basics_empty",
                        "symbol": sym,
                        "message": "yfinance info contained no usable profile fields",
                    },
                    default=str,
                )
            )
            return []
        return [LandingRecord(dataset="basics", symbol=sym, record_date=None, payload=fields)]

    if dataset == "fundamentals":
        balance: list[dict[str, Any]] = []
        qbs = getattr(ticker, "quarterly_balance_sheet", None)
        if qbs is not None and hasattr(qbs, "empty") and not qbs.empty:
            import pandas as pd

            for col in qbs.columns:
                ts = pd.Timestamp(col)
                end_raw = int(ts.timestamp())
                row_for_stmt: dict[str, Any] = {"endDate": {"raw": end_raw}}

                def cell(label: str) -> Any:
                    if label not in qbs.index:
                        return None
                    return _nan_to_none(qbs.loc[label, col])

                ta = cell("Total Assets")
                td = cell("Total Debt")
                if ta is None and td is None:
                    continue
                if ta is not None:
                    row_for_stmt["totalAssets"] = _raw_value(ta)
                if td is not None:
                    row_for_stmt["totalDebt"] = _raw_value(td)
                balance.append(row_for_stmt)

        fields = {
            "totalAssets": _raw_value(info.get("totalAssets")),
            "totalDebt": _raw_value(info.get("totalDebt")),
            "investedCapital": _raw_value(info.get("investedCapital")),
            "sharesOutstanding": _raw_value(
                info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
            ),
            "balanceSheetStatements": balance,
        }
        if not balance and not any(
            fields[k] is not None for k in ("totalAssets", "totalDebt", "investedCapital", "sharesOutstanding")
        ):
            LOGGER.warning(
                json.dumps(
                    {
                        "event": "yfinance_fundamentals_empty",
                        "symbol": sym,
                        "message": "no quarterly balance sheet rows and no headline metrics in info",
                    },
                    default=str,
                )
            )
            return []
        return [LandingRecord(dataset="fundamentals", symbol=sym, record_date=None, payload=fields)]

    if dataset == "holders":
        records: list[Any] = []
        import pandas as pd

        def frame_to_records(df: Any, holder_type: str) -> None:
            if df is None or getattr(df, "empty", True):
                return
            df2 = df.reset_index()
            cols = {c.lower(): c for c in df2.columns}

            def pick(*names: str) -> str | None:
                for n in names:
                    if n.lower() in cols:
                        return cols[n.lower()]
                return None

            c_holder = pick("Holder", "holder")
            c_shares = pick("Shares", "shares")
            c_date = pick("Date Reported", "date reported", "date")
            c_value = pick("Value", "value")
            if not c_holder or not c_date:
                return
            for _, row in df2.iterrows():
                holder_name = row.get(c_holder)
                if holder_name is None or (isinstance(holder_name, float) and pd.isna(holder_name)):
                    continue
                rd_raw = row.get(c_date)
                rd: date | None
                if rd_raw is None or (isinstance(rd_raw, float) and pd.isna(rd_raw)):
                    continue
                if isinstance(rd_raw, datetime):
                    rd = rd_raw.date()
                else:
                    rd = pd.Timestamp(rd_raw).date()
                shares = row.get(c_shares) if c_shares else None
                val = row.get(c_value) if c_value else None
                records.append(
                    LandingRecord(
                        dataset="holders",
                        symbol=sym,
                        record_date=rd,
                        payload={
                            "holder": str(holder_name).strip(),
                            "shares": _raw_value(shares) if shares is not None and not pd.isna(shares) else None,
                            "value": _raw_value(val) if val is not None and not pd.isna(val) else None,
                            "holder_type": holder_type,
                        },
                    )
                )

        frame_to_records(getattr(ticker, "institutional_holders", None), "institution")
        frame_to_records(getattr(ticker, "mutualfund_holders", None), "mutual_fund")

        if not records:
            LOGGER.warning(
                json.dumps(
                    {
                        "event": "yfinance_holders_empty",
                        "symbol": sym,
                        "message": "no institutional or mutual fund holders from yfinance",
                    },
                    default=str,
                )
            )
        return records

    if dataset == "ratings":
        import pandas as pd

        ud = getattr(ticker, "upgrades_downgrades", None)
        if ud is None or getattr(ud, "empty", True):
            getter = getattr(ticker, "get_upgrades_downgrades", None)
            if callable(getter):
                try:
                    ud = getter()
                except Exception:
                    ud = None
        if ud is None or getattr(ud, "empty", True):
            LOGGER.warning(
                json.dumps(
                    {"event": "yfinance_ratings_empty", "symbol": sym, "message": "no upgrades_downgrades DataFrame"},
                    default=str,
                )
            )
            return []

        def _series_get_ci(series: Any, *names: str) -> Any:
            """Case-insensitive lookup on a Series (column names vary by yfinance/Yahoo version)."""
            mapping = {str(k).lower(): k for k in series.index}
            for n in names:
                lk = n.lower()
                if lk in mapping:
                    return series[mapping[lk]]
            return None

        out: list[Any] = []

        # yfinance usually exposes grade dates as the DataFrame index (DatetimeIndex), not a "Date" column.
        if isinstance(ud.index, pd.DatetimeIndex):
            for ts, row in ud.iterrows():
                rd = pd.Timestamp(ts).date()
                firm = _series_get_ci(row, "Firm", "firm")
                if firm is None or (isinstance(firm, float) and pd.isna(firm)):
                    firm = "UNKNOWN_FIRM"
                to_g = _series_get_ci(row, "To Grade", "tograde", "to_grade", "ToGrade")
                from_g = _series_get_ci(row, "From Grade", "fromgrade", "from_grade", "FromGrade")
                act = _series_get_ci(row, "Action", "action")
                score_raw = _series_get_ci(row, "recommendation", "rating", "score")
                score = (
                    float(score_raw)
                    if score_raw is not None and not (isinstance(score_raw, float) and pd.isna(score_raw))
                    else None
                )
                out.append(
                    LandingRecord(
                        dataset="ratings",
                        symbol=sym,
                        record_date=rd,
                        payload={
                            "firm": str(firm).strip() or "UNKNOWN_FIRM",
                            "to_grade": None
                            if to_g is None or (isinstance(to_g, float) and pd.isna(to_g))
                            else to_g,
                            "from_grade": None
                            if from_g is None or (isinstance(from_g, float) and pd.isna(from_g))
                            else from_g,
                            "action": None if act is None or (isinstance(act, float) and pd.isna(act)) else act,
                            "epochGradeDate": {
                                "raw": int(
                                    datetime(rd.year, rd.month, rd.day, tzinfo=timezone.utc).timestamp()
                                )
                            },
                            "recommendation_score": score,
                        },
                    )
                )
            return out

        df = ud.reset_index()
        cols = {str(c).lower(): c for c in df.columns}

        def col(*names: str) -> str | None:
            for n in names:
                if n.lower() in cols:
                    return cols[n.lower()]
            return None

        c_date = col("date", "grade date", "index")
        if not c_date and len(df.columns) > 0:
            # Unnamed index becomes 'index' or level_0 in some pandas versions
            first = df.columns[0]
            if pd.api.types.is_datetime64_any_dtype(df[first]):
                c_date = first
        c_firm = col("firm", "to firm")
        c_to = col("tograde", "to grade", "to_grade")
        c_from = col("fromgrade", "from grade", "from_grade")
        c_action = col("action")
        c_score = col("recommendation", "rating", "score")
        if not c_date:
            LOGGER.warning(
                json.dumps(
                    {
                        "event": "yfinance_ratings_parse_skip",
                        "symbol": sym,
                        "message": "could not resolve date column after reset_index",
                        "columns": list(df.columns),
                    },
                    default=str,
                )
            )
            return []

        for _, row in df.iterrows():
            dr = row.get(c_date)
            if dr is None or (isinstance(dr, float) and pd.isna(dr)):
                continue
            rd = dr.date() if isinstance(dr, datetime) else pd.Timestamp(dr).date()
            firm = row.get(c_firm) if c_firm else None
            if firm is None or (isinstance(firm, float) and pd.isna(firm)):
                firm = "UNKNOWN_FIRM"
            to_g = row.get(c_to) if c_to else None
            from_g = row.get(c_from) if c_from else None
            act = row.get(c_action) if c_action else None
            score_raw = row.get(c_score) if c_score else None
            score = float(score_raw) if score_raw is not None and not pd.isna(score_raw) else None
            out.append(
                LandingRecord(
                    dataset="ratings",
                    symbol=sym,
                    record_date=rd,
                    payload={
                        "firm": str(firm).strip() or "UNKNOWN_FIRM",
                        "to_grade": None if to_g is None or (isinstance(to_g, float) and pd.isna(to_g)) else to_g,
                        "from_grade": None
                        if from_g is None or (isinstance(from_g, float) and pd.isna(from_g))
                        else from_g,
                        "action": None if act is None or (isinstance(act, float) and pd.isna(act)) else act,
                        "epochGradeDate": {
                            "raw": int(datetime(rd.year, rd.month, rd.day, tzinfo=timezone.utc).timestamp())
                        },
                        "recommendation_score": score,
                    },
                )
            )
        return out

    return []
