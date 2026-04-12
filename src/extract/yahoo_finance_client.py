from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

import requests


DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_SOURCE = "yahoo_finance"

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtractionWindow:
    start_date: date = DEFAULT_START_DATE
    end_date: date = DEFAULT_END_DATE

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")

    @property
    def period1(self) -> int:
        return int(datetime(self.start_date.year, self.start_date.month, self.start_date.day, tzinfo=timezone.utc).timestamp())

    @property
    def period2(self) -> int:
        # Yahoo chart period2 is exclusive: add one day.
        end_plus_one = self.end_date.toordinal() + 1
        end_dt = datetime.fromordinal(end_plus_one).replace(tzinfo=timezone.utc)
        return int(end_dt.timestamp())


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    retryable_status_codes: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class BatchConfig:
    batch_size: int = 25
    pause_between_batches_seconds: float = 1.5

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")


@dataclass(frozen=True)
class LandingRecord:
    dataset: str
    symbol: str
    record_date: date | None
    payload: dict[str, Any]
    source: str = DEFAULT_SOURCE
    load_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def version_hash(self) -> str:
        normalized = json.dumps(self.payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class YahooFinanceClient:
    def __init__(
        self,
        retry_config: RetryConfig | None = None,
        batch_config: BatchConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.retry_config = retry_config or RetryConfig()
        self.batch_config = batch_config or BatchConfig()
        self.session = session or requests.Session()
        self.base_url = "https://query1.finance.yahoo.com"

    # -----------------------------
    # Public extraction entrypoints
    # -----------------------------
    def get_us_listed_bank_universe(self, limit: int = 5000) -> list[str]:
        payload = {
            "offset": 0,
            "size": min(limit, 250),
            "sortField": "intradaymarketcap",
            "sortType": "DESC",
            "quoteType": "EQUITY",
            "query": {
                "operator": "AND",
                "operands": [
                    {"operator": "EQ", "operands": ["region", "us"]},
                    {"operator": "EQ", "operands": ["sector", "Financial Services"]},
                    {
                        "operator": "OR",
                        "operands": [
                            {"operator": "BTWN", "operands": ["exchange", "NMS", "NYQ"]},
                            {"operator": "EQ", "operands": ["exchange", "ASE"]},
                        ],
                    },
                ],
            },
        }
        data = self._request_json("POST", f"{self.base_url}/v1/finance/screener", json_body=payload)
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        symbols = [row.get("symbol") for row in quotes if row.get("symbol") and "BANK" in str(row.get("industry", "")).upper()]
        self._log("universe_extracted", batch=1, symbol_count=len(symbols), status="ok")
        return symbols

    def extract_basics(self, symbols: Sequence[str]) -> list[LandingRecord]:
        return self._extract_quote_summary_dataset(
            symbols=symbols,
            dataset="basics",
            modules=["assetProfile"],
            mapper=self._map_basics,
        )

    def extract_fundamentals(self, symbols: Sequence[str]) -> list[LandingRecord]:
        return self._extract_quote_summary_dataset(
            symbols=symbols,
            dataset="fundamentals",
            modules=["defaultKeyStatistics", "financialData", "balanceSheetHistoryQuarterly"],
            mapper=self._map_fundamentals,
        )

    def extract_holders(self, symbols: Sequence[str]) -> list[LandingRecord]:
        return self._extract_quote_summary_dataset(
            symbols=symbols,
            dataset="holders",
            modules=["institutionOwnership", "fundOwnership", "majorHoldersBreakdown"],
            mapper=self._map_holders,
        )

    def extract_ratings(self, symbols: Sequence[str]) -> list[LandingRecord]:
        return self._extract_quote_summary_dataset(
            symbols=symbols,
            dataset="ratings",
            modules=["upgradeDowngradeHistory"],
            mapper=self._map_ratings,
        )

    def extract_daily_prices(
        self,
        symbols: Sequence[str],
        window: ExtractionWindow | None = None,
        interval: str = "1d",
    ) -> list[LandingRecord]:
        window = window or ExtractionWindow()
        all_records: list[LandingRecord] = []
        for batch_number, batch in enumerate(_chunked(symbols, self.batch_config.batch_size), start=1):
            self._log("daily_prices_batch_start", batch=batch_number, symbol_count=len(batch), status="started")
            for symbol in batch:
                try:
                    endpoint = f"{self.base_url}/v8/finance/chart/{symbol}"
                    params = {
                        "interval": interval,
                        "period1": window.period1,
                        "period2": window.period2,
                        "events": "history",
                    }
                    data = self._request_json("GET", endpoint, params=params)
                    all_records.extend(self._map_prices(symbol, data, window))
                    self._log("daily_prices_symbol", symbol=symbol, batch=batch_number, status="ok")
                except Exception as exc:  # noqa: BLE001
                    self._log("daily_prices_symbol", symbol=symbol, batch=batch_number, status="error", error=str(exc))
            if self.batch_config.pause_between_batches_seconds > 0:
                time.sleep(self.batch_config.pause_between_batches_seconds)
        return all_records

    # -----------------------
    # Generic batch extraction
    # -----------------------
    def _extract_quote_summary_dataset(
        self,
        symbols: Sequence[str],
        dataset: str,
        modules: Sequence[str],
        mapper: Any,
    ) -> list[LandingRecord]:
        all_records: list[LandingRecord] = []
        for batch_number, batch in enumerate(_chunked(symbols, self.batch_config.batch_size), start=1):
            self._log(f"{dataset}_batch_start", batch=batch_number, symbol_count=len(batch), status="started")
            for symbol in batch:
                try:
                    endpoint = f"{self.base_url}/v10/finance/quoteSummary/{symbol}"
                    params = {"modules": ",".join(modules)}
                    data = self._request_json("GET", endpoint, params=params)
                    records = mapper(symbol, data)
                    all_records.extend(records)
                    self._log(f"{dataset}_symbol", symbol=symbol, batch=batch_number, status="ok", rows=len(records))
                except Exception as exc:  # noqa: BLE001
                    self._log(f"{dataset}_symbol", symbol=symbol, batch=batch_number, status="error", error=str(exc))
            if self.batch_config.pause_between_batches_seconds > 0:
                time.sleep(self.batch_config.pause_between_batches_seconds)
        return all_records

    # ----------------
    # Mapping functions
    # ----------------
    def _map_basics(self, symbol: str, payload: Mapping[str, Any]) -> list[LandingRecord]:
        result = _first_result(payload, root="quoteSummary")
        profile = result.get("assetProfile", {})
        fields = {
            "industry": profile.get("industry"),
            "sector": profile.get("sector"),
            "fullTimeEmployees": profile.get("fullTimeEmployees"),
            "city": profile.get("city"),
            "phone": profile.get("phone"),
            "state": profile.get("state"),
            "country": profile.get("country"),
            "website": profile.get("website"),
            "address1": profile.get("address1"),
        }
        return [LandingRecord(dataset="basics", symbol=symbol, record_date=None, payload=fields)]

    def _map_fundamentals(self, symbol: str, payload: Mapping[str, Any]) -> list[LandingRecord]:
        result = _first_result(payload, root="quoteSummary")
        stats = result.get("defaultKeyStatistics", {})
        financial_data = result.get("financialData", {})
        balance = result.get("balanceSheetHistoryQuarterly", {}).get("balanceSheetStatements", [])
        fields = {
            "totalAssets": _raw(financial_data.get("totalAssets")),
            "totalDebt": _raw(financial_data.get("totalDebt")),
            "investedCapital": _raw(financial_data.get("investedCapital")),
            "sharesOutstanding": _raw(stats.get("sharesOutstanding")),
            "balanceSheetStatements": balance,
        }
        return [LandingRecord(dataset="fundamentals", symbol=symbol, record_date=None, payload=fields)]

    def _map_holders(self, symbol: str, payload: Mapping[str, Any]) -> list[LandingRecord]:
        result = _first_result(payload, root="quoteSummary")
        records: list[LandingRecord] = []

        for row in result.get("institutionOwnership", {}).get("ownershipList", []):
            records.append(
                LandingRecord(
                    dataset="holders",
                    symbol=symbol,
                    record_date=_date_from_ts(_raw(row.get("reportDate"))),
                    payload={
                        "holder": row.get("organization"),
                        "shares": _raw(row.get("position")),
                        "value": _raw(row.get("value")),
                        "holder_type": "institution",
                    },
                )
            )

        for row in result.get("fundOwnership", {}).get("ownershipList", []):
            records.append(
                LandingRecord(
                    dataset="holders",
                    symbol=symbol,
                    record_date=_date_from_ts(_raw(row.get("reportDate"))),
                    payload={
                        "holder": row.get("organization"),
                        "shares": _raw(row.get("position")),
                        "value": _raw(row.get("value")),
                        "holder_type": "fund",
                    },
                )
            )

        if not records:
            records.append(
                LandingRecord(
                    dataset="holders",
                    symbol=symbol,
                    record_date=None,
                    payload={"majorHoldersBreakdown": result.get("majorHoldersBreakdown", {})},
                )
            )
        return records

    def _map_ratings(self, symbol: str, payload: Mapping[str, Any]) -> list[LandingRecord]:
        result = _first_result(payload, root="quoteSummary")
        history = result.get("upgradeDowngradeHistory", {}).get("history", [])
        records: list[LandingRecord] = []
        for row in history:
            records.append(
                LandingRecord(
                    dataset="ratings",
                    symbol=symbol,
                    record_date=_date_from_ts(_raw(row.get("epochGradeDate"))),
                    payload={
                        "to_grade": row.get("toGrade"),
                        "from_grade": row.get("fromGrade"),
                        "action": row.get("action"),
                        "firm": row.get("firm"),
                    },
                )
            )
        return records

    def _map_prices(self, symbol: str, payload: Mapping[str, Any], window: ExtractionWindow) -> list[LandingRecord]:
        result = _first_result(payload, root="chart")
        timestamps = result.get("timestamp", [])
        indicators = result.get("indicators", {}).get("quote", [{}])[0]

        opens = indicators.get("open", [])
        highs = indicators.get("high", [])
        lows = indicators.get("low", [])
        closes = indicators.get("close", [])
        volumes = indicators.get("volume", [])

        rows: list[LandingRecord] = []
        for idx, ts in enumerate(timestamps):
            row_date = _date_from_ts(ts)
            if row_date is None or row_date < window.start_date or row_date > window.end_date:
                continue
            rows.append(
                LandingRecord(
                    dataset="daily_prices",
                    symbol=symbol,
                    record_date=row_date,
                    payload={
                        "open": _safe_index(opens, idx),
                        "high": _safe_index(highs, idx),
                        "low": _safe_index(lows, idx),
                        "close": _safe_index(closes, idx),
                        "volume": _safe_index(volumes, idx),
                    },
                )
            )
        return rows

    # -----------------------
    # HTTP + observability
    # -----------------------
    def _request_json(
        self,
        method: str,
        url: str,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.retry_config.timeout_seconds,
                    headers={"User-Agent": "bank-market-pipeline/1.0"},
                )
                if response.status_code in self.retry_config.retryable_status_codes:
                    raise requests.HTTPError(f"Retryable status code: {response.status_code}", response=response)
                response.raise_for_status()
                data = response.json()
                _validate_yahoo_response(data)
                return data
            except (requests.RequestException, ValueError, AssertionError) as exc:
                last_exc = exc
                if attempt >= self.retry_config.max_attempts:
                    break
                delay = min(
                    self.retry_config.max_delay_seconds,
                    self.retry_config.base_delay_seconds * math.pow(2, attempt - 1),
                )
                self._log(
                    "http_retry",
                    status="retrying",
                    attempt=attempt,
                    max_attempts=self.retry_config.max_attempts,
                    delay_seconds=delay,
                    url=url,
                    error=str(exc),
                )
                time.sleep(delay)
        raise RuntimeError(f"Yahoo Finance request failed after retries: {url}") from last_exc

    def _log(self, event: str, **kwargs: Any) -> None:
        entry = {"event": event, "timestamp": datetime.now(timezone.utc).isoformat(), **kwargs}
        LOGGER.info(json.dumps(entry, ensure_ascii=False, default=str))


class LandingRepository:
    """Persist raw datasets in landing tables with technical keys for idempotency."""

    def __init__(self, connection: Any) -> None:
        """
        Parameters
        ----------
        connection:
            DB-API compatible connection (psycopg/psycopg2).
        """
        self.connection = connection

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS landing_yahoo_raw (
            dataset TEXT NOT NULL,
            symbol TEXT NOT NULL,
            record_date DATE NULL,
            load_timestamp TIMESTAMPTZ NOT NULL,
            source TEXT NOT NULL,
            version_hash TEXT NOT NULL,
            payload JSONB NOT NULL,
            PRIMARY KEY (dataset, symbol, record_date, source, version_hash)
        );
        """
        with self.connection.cursor() as cur:
            cur.execute(ddl)
        self.connection.commit()

    def upsert_records(self, records: Iterable[LandingRecord]) -> int:
        insert_sql = """
        INSERT INTO landing_yahoo_raw (
            dataset,
            symbol,
            record_date,
            load_timestamp,
            source,
            version_hash,
            payload
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (dataset, symbol, record_date, source, version_hash)
        DO NOTHING;
        """
        written = 0
        with self.connection.cursor() as cur:
            for record in records:
                cur.execute(
                    insert_sql,
                    (
                        record.dataset,
                        record.symbol,
                        record.record_date,
                        record.load_timestamp,
                        record.source,
                        record.version_hash,
                        json.dumps(record.payload, ensure_ascii=False, default=str),
                    ),
                )
                written += cur.rowcount
        self.connection.commit()
        return written


def _chunked(items: Sequence[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def _first_result(payload: Mapping[str, Any], root: str) -> Mapping[str, Any]:
    result = payload.get(root, {}).get("result", [])
    return result[0] if result else {}


def _raw(value: Any) -> Any:
    if isinstance(value, Mapping) and "raw" in value:
        return value.get("raw")
    return value


def _safe_index(values: Sequence[Any], index: int) -> Any:
    return values[index] if index < len(values) else None


def _date_from_ts(ts: Any) -> date | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
    except (TypeError, ValueError, OSError):
        return None


def _validate_yahoo_response(payload: Mapping[str, Any]) -> None:
    if not isinstance(payload, Mapping):
        raise ValueError("Invalid Yahoo response payload type")

    for root in ("quoteSummary", "chart", "finance"):
        section = payload.get(root)
        if isinstance(section, Mapping):
            error = section.get("error")
            if error:
                raise ValueError(f"Yahoo API error: {error}")