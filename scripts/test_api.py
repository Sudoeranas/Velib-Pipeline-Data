"""Validation rapide des deux endpoints OpenData Paris."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from velib.api_client import fetch_availability, fetch_stations


def test(nom: str, records: list) -> None:
    print(f"\n{'=' * 50}")
    print(f"Test : {nom}")
    print(f"{'=' * 50}")
    print(f"✅ Total récupéré : {len(records)}")
    print(f"\n📍 Premier enregistrement :")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    print(f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    test("Disponibilité temps réel", fetch_availability())
    test("Emplacements des stations", fetch_stations())
