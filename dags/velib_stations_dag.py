"""
DAG : Ingestion référentiel stations Vélib
Schedule : une fois par jour (données quasi-statiques)
Source   : opendata.paris.fr — velib-emplacement-des-stations
Stockage : data/raw/stations/YYYY/MM/DD/ (local dev) → GCS en prod
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from velib.api_client import fetch_stations

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "stations"


def ingest_stations(**context) -> None:
    ts: datetime = context["logical_date"]
    records = fetch_stations()

    partition = RAW_DIR / ts.strftime("%Y") / ts.strftime("%m") / ts.strftime("%d")
    partition.mkdir(parents=True, exist_ok=True)

    filepath = partition / f"stations_{ts.strftime('%Y%m%d')}.json"
    filepath.write_text(
        json.dumps({"ingested_at": ts.isoformat(), "total": len(records), "records": records}, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"✅ {len(records)} stations → {filepath}")


with DAG(
    dag_id="velib_stations_ingestion",
    description="Ingestion référentiel stations Vélib (quotidien)",
    schedule="0 */12 * * *",
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
        task_id="fetch_and_save_stations",
        python_callable=ingest_stations,
    )
