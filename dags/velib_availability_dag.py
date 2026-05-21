"""
DAG : Ingestion disponibilité Vélib en temps réel
Schedule : toutes les 60 secondes
Source   : opendata.paris.fr — velib-disponibilite-en-temps-reel
Stockage : data/raw/availability/YYYY/MM/DD/HH/ (local dev) → GCS en prod
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from velib.api_client import fetch_availability

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "availability"


def ingest_availability(**context) -> None:
    ts: datetime = context["logical_date"]
    records = fetch_availability()

    partition = RAW_DIR / ts.strftime("%Y") / ts.strftime("%m") / ts.strftime("%d") / ts.strftime("%H")
    partition.mkdir(parents=True, exist_ok=True)

    filepath = partition / f"availability_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    filepath.write_text(
        json.dumps({"ingested_at": ts.isoformat(), "total": len(records), "records": records}, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"✅ {len(records)} stations → {filepath}")


with DAG(
    dag_id="velib_availability_ingestion",
    description="Ingestion disponibilité Vélib toutes les 60s",
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
        task_id="fetch_and_save_availability",
        python_callable=ingest_availability,
    )
