"""
DAG : Ingestion référentiel stations Vélib
Schedule : toutes les 30 minutes
Source   : opendata.paris.fr — velib-emplacement-des-stations
Stockage : GCS bucket_velib_paris/raw/stations/YYYY/MM/DD/
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from velib.api_client import fetch_stations
from velib.gcs_client import stations_gcs_path, upload_json


def ingest_stations(**context) -> None:
    ts: datetime = context["logical_date"]
    records = fetch_stations()

    payload = {"ingested_at": ts.isoformat(), "total": len(records), "records": records}
    uri = upload_json(payload, stations_gcs_path(ts))

    print(f"✅ {len(records)} stations → {uri}")


with DAG(
    dag_id="velib_stations_ingestion",
    description="Ingestion référentiel stations Vélib toutes les 30min",
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
        task_id="fetch_and_upload_stations",
        python_callable=ingest_stations,
    )
