from __future__ import annotations

import json
from datetime import datetime

from google.cloud import storage

BUCKET_NAME = "bucket_velib_paris"


def upload_json(data: dict, gcs_path: str) -> str:
    """Upload un dict JSON dans GCS et retourne l'URI gs://..."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json",
    )
    return f"gs://{BUCKET_NAME}/{gcs_path}"


def availability_gcs_path(ts: datetime) -> str:
    return (
        f"raw/availability/{ts.strftime('%Y/%m/%d/%H')}"
        f"/availability_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    )


def stations_gcs_path(ts: datetime) -> str:
    return f"raw/stations/{ts.strftime('%Y/%m/%d')}/stations_{ts.strftime('%Y%m%d')}.json"
