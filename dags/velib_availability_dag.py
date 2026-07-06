"""
DAG : Ingestion disponibilité Vélib en temps réel
Schedule : toutes les heures
Source   : opendata.paris.fr — velib-disponibilite-en-temps-reel
Stockage : GCS bucket_velib_paris/raw/availability/YYYY/MM/DD/HH/
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from velib.api_client import fetch_availability
from velib.gcs_client import availability_gcs_path, upload_json


def on_failure(context) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    ts = context["logical_date"]
    exception = context.get("exception")
    print(
        f"❌ ÉCHEC DAG={dag_id} TASK={task_id} "
        f"ts={ts.isoformat()} error={exception}"
    )


def ingest_availability(**context) -> None:
    ts: datetime = context["logical_date"]
    records = fetch_availability()

    if not records:
        raise ValueError("API a retourné 0 stations — run annulé")

    payload = {"ingested_at": ts.isoformat(), "total": len(records), "records": records}
    uri = upload_json(payload, availability_gcs_path(ts))

    print(f"✅ {len(records)} stations → {uri}")


with DAG(
    dag_id="velib_availability_ingestion",
    description="Ingestion disponibilité Vélib toutes les heures",
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
        task_id="fetch_and_upload_availability",
        python_callable=ingest_availability,
    )
