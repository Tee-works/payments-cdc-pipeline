"""
### Reconcile payments CDC

Runs at 02:00 UTC and compares yesterday's transactions in the Postgres
warehouse against the MySQL source: row counts and amount totals per status.

A mismatch fails the task. Retries give late events time to arrive before
anyone is alerted; a mismatch that survives three retries is a real problem.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from cdc import reconcile
from cdc.config import Settings


def reconcile_closed_day(data_interval_start: datetime, **_: object) -> None:
    day = data_interval_start.date()
    mismatches = reconcile.reconcile_day(Settings.from_env(), day)
    report = reconcile.format_report(mismatches)
    if mismatches:
        raise ValueError(f"{day}: {report}")
    print(f"{day}: {report}")


with DAG(
    dag_id="reconcile_payments_cdc",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10)},
    tags=["cdc", "reconciliation", "payments"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="reconcile_closed_day", python_callable=reconcile_closed_day)
