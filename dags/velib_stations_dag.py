"""
DAG : Ingestion référentiel stations Vélib
Schedule : toutes les heures
Source   : opendata.paris.fr — velib-emplacement-des-stations
Stockage : GCS bucket_velib_paris/raw/stations/YYYY/MM/DD/
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from velib.api_client import fetch_stations
from velib.gcs_client import stations_gcs_path, upload_json


def on_failure(context) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    ts = context["logical_date"]
    exception = context.get("exception")
    print(
        f"❌ ÉCHEC DAG={dag_id} TASK={task_id} "
        f"ts={ts.isoformat()} error={exception}"
    )


def ingest_stations(**context) -> None:
    ts: datetime = context["logical_date"]
    records = fetch_stations()

    if not records:
        raise ValueError("API a retourné 0 stations — run annulé")

    payload = {"ingested_at": ts.isoformat(), "total": len(records), "records": records}
    uri = upload_json(payload, stations_gcs_path(ts))

    print(f"✅ {len(records)} stations → {uri}")


with DAG(
    dag_id="velib_stations_ingestion",
    description="Ingestion référentiel stations Vélib toutes les heures",
    schedule="0 * * * *",
    start_date=datetime(2026, 5, 18, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "owner": "data-engineering",
        "on_failure_callback": on_failure,
    },
    tags=["velib", "ingestion", "raw"],
) as dag:

    PythonOperator(
        task_id="fetch_and_upload_stations",
        python_callable=ingest_stations,
    )
