"""
DAG : Ingestion disponibilité Vélib en temps réel
Schedule : toutes les 30 minutes
Source   : opendata.paris.fr — velib-disponibilite-en-temps-reel
Stockage : GCS bucket_velib_paris/raw/availability/YYYY/MM/DD/HH/
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from velib.api_client import fetch_availability
from velib.gcs_client import availability_gcs_path, upload_json


def ingest_availability(**context) -> None:
    ts: datetime = context["logical_date"]
    records = fetch_availability()

    payload = {"ingested_at": ts.isoformat(), "total": len(records), "records": records}
    uri = upload_json(payload, availability_gcs_path(ts))

    print(f"✅ {len(records)} stations → {uri}")


with DAG(
    dag_id="velib_availability_ingestion",
    description="Ingestion disponibilité Vélib toutes les 30min",
    schedule="*/1 * * * *",
    start_date=datetime(2026, 5, 18, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": 30,
        "owner": "data-engineering",
    },
    tags=["velib", "ingestion", "raw"],
) as dag:

    PythonOperator(
        task_id="fetch_and_upload_availability",
        python_callable=ingest_availability,
    )
