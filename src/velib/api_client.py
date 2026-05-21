from __future__ import annotations

from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_AVAILABILITY = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
    "/velib-disponibilite-en-temps-reel/records"
)
BASE_STATIONS = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
    "/velib-emplacement-des-stations/records"
)

PAGE_SIZE = 100


def fetch_all(base_url: str, timeout: int = 30) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    offset = 0
    total: int | None = None

    while total is None or offset < total:
        resp = requests.get(
            base_url,
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=timeout,
            verify=False,
        )
        resp.raise_for_status()
        data = resp.json()

        if total is None:
            total = data["total_count"]

        batch: list[dict[str, Any]] = data.get("results", [])
        if not batch:
            break

        results.extend(batch)
        offset += len(batch)

    return results


def fetch_availability() -> list[dict[str, Any]]:
    return fetch_all(BASE_AVAILABILITY)


def fetch_stations() -> list[dict[str, Any]]:
    return fetch_all(BASE_STATIONS)
