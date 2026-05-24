"""
DAG : Transformations dbt (RAW -> STAGING -> MARTS)
Schedule : après chaque ingestion (toutes les 6h)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="velib_dbt_transform",
    description="Transformations dbt RAW -> STAGING -> MARTS",
    schedule="30 */6 * * *",
    start_date=datetime(2026, 5, 18, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "owner": "data-engineering",
    },
    tags=["velib", "dbt", "transform"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/velib_dbt && dbt run --profiles-dir /opt/airflow/velib_dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/velib_dbt && dbt test --profiles-dir /opt/airflow/velib_dbt",
    )

    dbt_run >> dbt_test
