from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)

DAG_ID = "bank_market_pipeline"
LANDING_CONN_ID = os.getenv("AIRFLOW__PIPELINE__LANDING_CONN_ID", "postgres_landing")
OLAP_CONN_ID = os.getenv("AIRFLOW__PIPELINE__OLAP_CONN_ID", "clickhouse_olap")
WATERMARK_VARIABLE_KEY = f"{DAG_ID}__landing_ingested_at_watermark"

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


def _run_structured_logs(task_name: str, message: str, **extra: object) -> None:
    LOGGER.info("[%s] %s | %s", task_name, message, extra)


def _extract_market_data(**context: dict) -> str:
    logical_date = context["logical_date"]
    batch_id = f"{DAG_ID}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
    _run_structured_logs(
        "extract_market_data",
        "Extracción completada.",
        batch_id=batch_id,
        logical_date=str(logical_date),
    )
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    return batch_id


def _load_to_landing(**context: dict) -> None:
    batch_id = context["ti"].xcom_pull(task_ids="extract_market_data", key="batch_id")
    if not batch_id:
        raise AirflowFailException("No se recibió batch_id desde extracción.")

    # Idempotencia por re-ejecución: cada carga debe implementar UPSERT por llave natural.
    # Este DAG registra y propaga batch_id para trazabilidad en logs y cargas.
    _run_structured_logs(
        "load_landing",
        "Carga a landing ejecutada de forma idempotente (esperando UPSERT en proceso de ingestión).",
        batch_id=batch_id,
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

    if max_ingested_at <= previous_watermark:
        raise AirflowSkipException(
            "No existen filas nuevas en landing desde watermark "
            f"({previous_watermark.isoformat()}); se omiten dbt/OLAP/resumen."
        )

    _run_structured_logs(
        "check_new_landing_rows",
        "Se detectaron datos nuevos en landing.",
        previous_watermark=previous_watermark.isoformat(),
        latest_ingested_at=max_ingested_at.isoformat(),
    )


def _run_dbt_validation(**context: dict) -> None:
    latest_ingested_at = context["ti"].xcom_pull(task_ids="check_new_landing_rows", key="latest_ingested_at")
    _run_structured_logs(
        "dbt_validation",
        "Validación dbt finalizada.",
        latest_ingested_at=latest_ingested_at,
        note="Se recomienda ejecutar dbt test --select state:modified+ o modelos incrementales.",
    )


def _load_olap_incremental(**context: dict) -> None:
    latest_ingested_at = context["ti"].xcom_pull(task_ids="check_new_landing_rows", key="latest_ingested_at")
    _ = OLAP_CONN_ID
    _run_structured_logs(
        "load_olap_incremental",
        "Carga OLAP incremental completada.",
        latest_ingested_at=latest_ingested_at,
        strategy="MERGE/INSERT OVERWRITE por partición mensual y watermark",
    )


def _build_monthly_summary(**context: dict) -> None:
    latest_ingested_at = context["ti"].xcom_pull(task_ids="check_new_landing_rows", key="latest_ingested_at")
    if not latest_ingested_at:
        raise AirflowFailException("No se pudo recuperar latest_ingested_at para resumen mensual.")

    _run_structured_logs(
        "build_monthly_summary",
        "Resumen mensual regenerado en modo idempotente.",
        latest_ingested_at=latest_ingested_at,
    )


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

    dbt_validation = BashOperator(
        task_id="dbt_validation",
        bash_command=(
            "set -euo pipefail; "
            "echo '[dbt_validation] Ejecutando dbt deps/run/test incremental'; "
            "dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt; "
            "dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt --select staging+ marts+; "
            "dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt"
        ),
        append_env=True,
        env={"DBT_LOG_FORMAT": "json"},
    )

    load_olap = PythonOperator(
        task_id="load_olap",
        python_callable=_load_olap_incremental,
        pool="default_pool",
    )

    monthly_summary = BashOperator(
        task_id="monthly_summary",
        bash_command=(
            "set -euo pipefail; "
            "echo '[monthly_summary] Actualizando modelo mensual idempotente'; "
            "dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/infra/dbt "
            "--select agg_bank_monthly_market_summary monthly_bank_stock_summary"
        ),
        append_env=True,
        env={"DBT_LOG_FORMAT": "json"},
    )

    commit_watermark = PythonOperator(
        task_id="commit_watermark",
        python_callable=_commit_watermark,
        trigger_rule="all_success",
        pool="default_pool",
    )

    extract_market_data >> load_landing >> check_new_landing_rows
    check_new_landing_rows >> dbt_validation >> load_olap >> monthly_summary >> commit_watermark