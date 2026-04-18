from __future__ import annotations

import logging
import os
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.extract.yahoo_finance_client import BatchConfig, ExtractionWindow, LandingRecord, YahooFinanceClient
from src.load.landing_psql_loader import (
    deserialize_landing_records,
    pipeline_scratch_dir,
    pipeline_source_system,
    scratch_batch_path,
    serialize_landing_records,
    upsert_landing_records,
)
from src.load.olap_clickhouse_loader import (
    LANDING_INGESTED_AT_WATERMARK_KEY as WATERMARK_VARIABLE_KEY,
    clickhouse_staging_total_rows,
    sync_landing_incremental_to_clickhouse,
)
from src.validate.olap_pipeline_gates import (
    validate_cur_daily_implies_monthly_mart,
    validate_landing_stock_implies_cur_daily,
)

LOGGER = logging.getLogger(__name__)

DAG_ID = "bank_market_pipeline"
LANDING_TO_OLAP_DAG_ID = "bank_market_landing_to_olap"
LANDING_CONN_ID = os.getenv("AIRFLOW__PIPELINE__LANDING_CONN_ID", "postgres_landing")
OLAP_CONN_ID = os.getenv("AIRFLOW__PIPELINE__OLAP_CONN_ID", "clickhouse_olap")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/infra/dbt")
DBT_LOG_PATH = os.getenv("DBT_LOG_PATH", "/tmp/dbt/logs")
DBT_TARGET_PATH = os.getenv("DBT_TARGET_PATH", "/tmp/dbt/target")
DBT_PACKAGES_INSTALL_PATH = os.getenv("DBT_PACKAGES_INSTALL_PATH", "/tmp/dbt/dbt_packages")

LANDING_TABLES = (
    "landing.bank_basic_info",
    "landing.stock_daily_price",
    "landing.bank_fundamentals",
    "landing.holders",
    "landing.ratings",
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": [os.getenv("AIRFLOW_ALERT_EMAIL", "alerts@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": int(os.getenv("AIRFLOW_TASK_RETRIES", "2")),
    "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_TASK_RETRY_DELAY_MINUTES", "5"))),
    "execution_timeout": timedelta(minutes=int(os.getenv("AIRFLOW_TASK_TIMEOUT_MINUTES", "45"))),
}

DBT_RUNTIME_ENV = {
    "DBT_LOG_FORMAT": "json",
    "DBT_LOG_PATH": DBT_LOG_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
    "DBT_PACKAGES_INSTALL_PATH": DBT_PACKAGES_INSTALL_PATH,
}


def _dbt_deps_command() -> str:
    return (
        "set -euo pipefail; "
        "mkdir -p \"$DBT_LOG_PATH\" \"$DBT_TARGET_PATH\" \"$DBT_PACKAGES_INSTALL_PATH\"; "
        "echo '[dbt_prepare_deps] dbt deps'; "
        f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )


def _dbt_validation_command() -> str:
    return (
        "set -euo pipefail; "
        "mkdir -p \"$DBT_LOG_PATH\" \"$DBT_TARGET_PATH\" \"$DBT_PACKAGES_INSTALL_PATH\"; "
        "_ts_start=$(date +%s); "
        "echo '[dbt_validation] dbt run/test target=dev'; "
        f"dbt run --target dev --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        "--select path:models/staging path:models/marts; "
        f"dbt test --target dev --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        "--select path:models/staging path:models/marts path:tests --exclude path:tests/olap; "
        "_ts_end=$(date +%s); "
        "echo \"[dbt_validation] duration_seconds=$((_ts_end - _ts_start))\""
    )


def _monthly_summary_command() -> str:
    return (
        "set -euo pipefail; "
        "mkdir -p \"$DBT_LOG_PATH\" \"$DBT_TARGET_PATH\" \"$DBT_PACKAGES_INSTALL_PATH\"; "
        "_ts_start=$(date +%s); "
        "echo '[monthly_summary] dbt run/test ClickHouse mart (target=olap)'; "
        f"dbt run --target olap --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        "--select path:models/olap; "
        f"dbt test --target olap --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} "
        "--select path:models/olap path:tests/olap; "
        "_ts_end=$(date +%s); "
        "echo \"[monthly_summary] duration_seconds=$((_ts_end - _ts_start))\""
    )


def _run_structured_logs(task_name: str, message: str, **extra: object) -> None:
    LOGGER.info("[%s] %s | %s", task_name, message, extra)


def _snapshot_date_for_run(context: dict) -> date:
    """Fecha de negocio (UTC) asociada al DagRun; usada como snapshot_date en landing."""
    logical = context["logical_date"]
    if hasattr(logical, "in_timezone"):
        return logical.in_timezone("UTC").date()
    if isinstance(logical, datetime):
        if logical.tzinfo is not None:
            return logical.astimezone(timezone.utc).date()
        return logical.date()
    if isinstance(logical, date):
        return logical
    raise AirflowFailException(f"logical_date no soportado: {type(logical)!r}")


def _parse_env_date(name: str, default: date) -> date:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return date.fromisoformat(raw)


def _symbols_for_run(client: YahooFinanceClient) -> list[str]:
    raw = os.getenv("PIPELINE_YAHOO_SYMBOLS", "").strip()
    if raw:
        return sorted({s.strip().upper() for s in raw.split(",") if s.strip()})
    limit = max(1, int(os.getenv("PIPELINE_BANK_UNIVERSE_LIMIT", "25")))
    symbols = client.get_us_listed_bank_universe(limit=5000)
    return symbols[:limit]


def _extract_market_data(**context: dict) -> str:
    """
    Extracción (solo lectura Yahoo). Persistencia ocurre en ``load_landing`` vía
    archivo scratch en disco para no serializar grandes volúmenes en XCom.
    """
    batch_id = f"{DAG_ID}-{context['dag_run'].run_id}"
    snapshot_date = _snapshot_date_for_run(context)

    scratch_root = pipeline_scratch_dir()
    Path(scratch_root).mkdir(parents=True, exist_ok=True)
    scratch_file = scratch_batch_path(scratch_root, batch_id)

    batch_size = max(1, int(os.getenv("PIPELINE_YAHOO_BATCH_SIZE", "25")))
    pause = float(os.getenv("PIPELINE_YAHOO_BATCH_PAUSE_SECONDS", "1.5"))
    client = YahooFinanceClient(batch_config=BatchConfig(batch_size=batch_size, pause_between_batches_seconds=pause))

    symbols = _symbols_for_run(client)
    if not symbols:
        raise AirflowFailException(
            "No hay símbolos para extraer. Defina PIPELINE_YAHOO_SYMBOLS o amplíe PIPELINE_BANK_UNIVERSE_LIMIT."
        )

    window = ExtractionWindow(
        start_date=_parse_env_date("PIPELINE_EXTRACT_START_DATE", date(2024, 1, 1)),
        end_date=_parse_env_date("PIPELINE_EXTRACT_END_DATE", date(2025, 12, 31)),
    )

    records: list[LandingRecord] = []
    records.extend(client.extract_basics(symbols))
    records.extend(client.extract_daily_prices(symbols, window=window))
    records.extend(client.extract_fundamentals(symbols))
    records.extend(client.extract_holders(symbols))
    records.extend(client.extract_ratings(symbols))

    scratch_file.write_text(serialize_landing_records(records), encoding="utf-8")

    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="landing_scratch_path", value=str(scratch_file))
    context["ti"].xcom_push(key="snapshot_date", value=snapshot_date.isoformat())

    dataset_counts = dict(Counter(r.dataset for r in records))
    _run_structured_logs(
        "extract_market_data",
        "Extracción Yahoo finalizada; payload serializado para carga landing.",
        batch_id=batch_id,
        snapshot_date=str(snapshot_date),
        symbol_count=len(symbols),
        record_count=len(records),
        dataset_counts=dataset_counts,
        scratch_path=str(scratch_file),
    )
    return batch_id


def _load_to_landing(**context: dict) -> None:
    """Carga idempotente a ``landing.*`` (UPSERT por llave natural + batch_id / ingested_at)."""
    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="extract_market_data", key="batch_id")
    scratch_path = ti.xcom_pull(task_ids="extract_market_data", key="landing_scratch_path")
    snapshot_raw = ti.xcom_pull(task_ids="extract_market_data", key="snapshot_date")
    if not batch_id or not scratch_path or not snapshot_raw:
        raise AirflowFailException("Faltan batch_id, landing_scratch_path o snapshot_date desde extracción.")

    path = Path(scratch_path)
    if not path.is_file():
        raise AirflowFailException(f"No existe archivo de extracción: {path}")

    records = deserialize_landing_records(path.read_text(encoding="utf-8"))
    snapshot_date = date.fromisoformat(snapshot_raw)
    ingested_at = datetime.now(timezone.utc)
    source_system = pipeline_source_system()

    hook = PostgresHook(postgres_conn_id=LANDING_CONN_ID)
    conn = hook.get_conn()
    try:
        counts = upsert_landing_records(
            conn,
            records,
            batch_id=batch_id,
            snapshot_date=snapshot_date,
            source_system=source_system,
            ingested_at=ingested_at,
        )
    finally:
        conn.close()

    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        LOGGER.warning("No se pudo eliminar scratch %s: %s", path, exc)

    _run_structured_logs(
        "load_landing",
        "Carga landing completada (UPSERT + batch_id).",
        batch_id=batch_id,
        counts=counts,
    )


def _check_new_rows_since_watermark(**context: dict) -> None:
    hook = PostgresHook(postgres_conn_id=LANDING_CONN_ID)
    previous_watermark_raw = Variable.get(WATERMARK_VARIABLE_KEY, default_var="1970-01-01T00:00:00+00:00")
    previous_watermark = datetime.fromisoformat(previous_watermark_raw)

    query = "\nUNION ALL\n".join(
        [f"SELECT MAX(ingested_at) AS max_ingested_at FROM {table}" for table in LANDING_TABLES]
    )
    final_query = f"SELECT MAX(max_ingested_at) FROM ({query}) landing_maxes"
    max_ingested_at = hook.get_first(final_query)[0]

    if max_ingested_at is None:
        raise AirflowSkipException("No hay filas en landing todavía; se omiten transformaciones.")

    max_ingested_at = max_ingested_at.astimezone(timezone.utc)
    context["ti"].xcom_push(key="latest_ingested_at", value=max_ingested_at.isoformat())

    if max_ingested_at > previous_watermark:
        context["ti"].xcom_push(key="olap_full_landing_resync", value=False)
        _run_structured_logs(
            "check_new_landing_rows",
            "Se detectaron datos nuevos en landing.",
            previous_watermark=previous_watermark.isoformat(),
            latest_ingested_at=max_ingested_at.isoformat(),
        )
        return

    # Watermark is current, but OLAP staging may still be empty (e.g. new ClickHouse volume or pre-fix DAG runs).
    try:
        stg_rows = clickhouse_staging_total_rows()
    except Exception as exc:
        LOGGER.warning("check_new_landing_rows: no se pudo leer ClickHouse (%s); se aplica lógica estándar.", exc)
        stg_rows = 1

    if stg_rows == 0:
        context["ti"].xcom_push(key="olap_full_landing_resync", value=True)
        _run_structured_logs(
            "check_new_landing_rows",
            "Watermark al día pero staging OLAP vacío; se fuerza resync completo desde landing.",
            previous_watermark=previous_watermark.isoformat(),
            latest_ingested_at=max_ingested_at.isoformat(),
        )
        return

    raise AirflowSkipException(
        "No existen filas nuevas en landing desde watermark "
        f"({previous_watermark.isoformat()}); se omiten dbt/OLAP/resumen."
    )


def _load_olap_incremental(**context: dict) -> None:
    latest_ingested_at = context["ti"].xcom_pull(task_ids="check_new_landing_rows", key="latest_ingested_at")
    if not latest_ingested_at:
        raise AirflowFailException("No se pudo recuperar latest_ingested_at para carga OLAP.")

    full_resync = context["ti"].xcom_pull(task_ids="check_new_landing_rows", key="olap_full_landing_resync")
    if full_resync is True:
        previous_watermark = "1970-01-01T00:00:00+00:00"
    else:
        previous_watermark = Variable.get(WATERMARK_VARIABLE_KEY, default_var="1970-01-01T00:00:00+00:00")
    counts = sync_landing_incremental_to_clickhouse(
        landing_conn_id=LANDING_CONN_ID,
        previous_watermark_raw=previous_watermark,
    )
    _run_structured_logs(
        "load_olap_incremental",
        "Carga OLAP incremental completada (PostgreSQL landing -> ClickHouse olap.stg_*, rebuild cur_*).",
        latest_ingested_at=latest_ingested_at,
        olap_conn_id=OLAP_CONN_ID,
        inserted_rows=counts,
    )


def _validate_landing_vs_cur_daily(**context: dict) -> None:
    """Falla si landing tiene precios diarios pero ClickHouse cur_stock_daily_price está vacío."""
    stats = validate_landing_stock_implies_cur_daily(landing_conn_id=LANDING_CONN_ID)
    _run_structured_logs("validate_landing_vs_cur_daily", "Gate OLAP post-carga OK.", **stats)


def _validate_cur_vs_monthly_mart(**context: dict) -> None:
    """Falla si cur tiene datos en ventana del mart pero la tabla mensual en ClickHouse está vacía."""
    stats = validate_cur_daily_implies_monthly_mart()
    _run_structured_logs("validate_cur_vs_monthly_mart", "Gate mart mensual OK.", **stats)


def _commit_watermark(**context: dict) -> None:
    latest_ingested_at = context["ti"].xcom_pull(task_ids="check_new_landing_rows", key="latest_ingested_at")
    if not latest_ingested_at:
        raise AirflowFailException("No existe latest_ingested_at para actualizar watermark.")

    Variable.set(WATERMARK_VARIABLE_KEY, latest_ingested_at)
    _run_structured_logs(
        "commit_watermark",
        "Watermark actualizado correctamente.",
        watermark=latest_ingested_at,
    )


with DAG(
    dag_id=DAG_ID,
    description="Pipeline bancario: extracción, landing, validación dbt, OLAP y resumen mensual incremental.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["bank", "market", "landing", "dbt", "olap"],
) as dag:
    extract_market_data = PythonOperator(
        task_id="extract_market_data",
        python_callable=_extract_market_data,
        pool="default_pool",
    )

    load_landing = PythonOperator(
        task_id="load_landing",
        python_callable=_load_to_landing,
        pool="default_pool",
    )

    check_new_landing_rows = PythonOperator(
        task_id="check_new_landing_rows",
        python_callable=_check_new_rows_since_watermark,
        pool="default_pool",
    )

    dbt_prepare_deps = BashOperator(
        task_id="dbt_prepare_deps",
        bash_command=_dbt_deps_command(),
        append_env=True,
        env=DBT_RUNTIME_ENV,
    )

    dbt_validation = BashOperator(
        task_id="dbt_validation",
        bash_command=_dbt_validation_command(),
        append_env=True,
        env=DBT_RUNTIME_ENV,
    )

    load_olap = PythonOperator(
        task_id="load_olap",
        python_callable=_load_olap_incremental,
        pool="default_pool",
    )

    validate_olap_curated = PythonOperator(
        task_id="validate_olap_curated",
        python_callable=_validate_landing_vs_cur_daily,
        pool="default_pool",
    )

    monthly_summary = BashOperator(
        task_id="monthly_summary",
        bash_command=_monthly_summary_command(),
        append_env=True,
        env=DBT_RUNTIME_ENV,
    )

    validate_monthly_mart = PythonOperator(
        task_id="validate_monthly_mart",
        python_callable=_validate_cur_vs_monthly_mart,
        pool="default_pool",
    )

    commit_watermark = PythonOperator(
        task_id="commit_watermark",
        python_callable=_commit_watermark,
        trigger_rule="all_success",
        pool="default_pool",
    )

    extract_market_data >> load_landing >> check_new_landing_rows
    (
        check_new_landing_rows
        >> dbt_prepare_deps
        >> dbt_validation
        >> load_olap
        >> validate_olap_curated
        >> monthly_summary
        >> validate_monthly_mart
        >> commit_watermark
    )

with DAG(
    dag_id=LANDING_TO_OLAP_DAG_ID,
    description="Sincroniza landing existente a OLAP/mart cuando hay datos nuevos en landing (sin extracción Yahoo).",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=os.getenv("AIRFLOW__PIPELINE__LANDING_TO_OLAP_CRON", "*/30 * * * *"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["bank", "market", "landing", "olap", "sync"],
) as landing_to_olap_dag:
    check_new_landing_rows = PythonOperator(
        task_id="check_new_landing_rows",
        python_callable=_check_new_rows_since_watermark,
        pool="default_pool",
    )

    dbt_prepare_deps = BashOperator(
        task_id="dbt_prepare_deps",
        bash_command=_dbt_deps_command(),
        append_env=True,
        env=DBT_RUNTIME_ENV,
    )

    dbt_validation = BashOperator(
        task_id="dbt_validation",
        bash_command=_dbt_validation_command(),
        append_env=True,
        env=DBT_RUNTIME_ENV,
    )

    load_olap = PythonOperator(
        task_id="load_olap",
        python_callable=_load_olap_incremental,
        pool="default_pool",
    )

    validate_olap_curated = PythonOperator(
        task_id="validate_olap_curated",
        python_callable=_validate_landing_vs_cur_daily,
        pool="default_pool",
    )

    monthly_summary = BashOperator(
        task_id="monthly_summary",
        bash_command=_monthly_summary_command(),
        append_env=True,
        env=DBT_RUNTIME_ENV,
    )

    validate_monthly_mart = PythonOperator(
        task_id="validate_monthly_mart",
        python_callable=_validate_cur_vs_monthly_mart,
        pool="default_pool",
    )

    commit_watermark = PythonOperator(
        task_id="commit_watermark",
        python_callable=_commit_watermark,
        trigger_rule="all_success",
        pool="default_pool",
    )

    (
        check_new_landing_rows
        >> dbt_prepare_deps
        >> dbt_validation
        >> load_olap
        >> validate_olap_curated
        >> monthly_summary
        >> validate_monthly_mart
        >> commit_watermark
    )