"""Manual backfill of all historical eCO2mix national data from ODRE.

* ``download_definitive_history``: ``eco2mix-national-cons-def`` (definitive data
  2012-2025 + consolidated current year), one file per year in
  ``<energy_output_dir>/eco2mix-national-cons-def/YYYY.json``. ~15 calls, ~5 min, ~400 MB.
* ``download_realtime_window``: every day still kept in ``eco2mix-national-tr``
  (current + previous two months), one file per day, same layout as
  ``ingest_energy_current`` so it fills the gap up to today.

RTE appends consolidated months to cons-def over time, so the two datasets
overlap; prefer cons-def where both have a slot.

Every file is also pushed to the same relative path under
``/Volumes/energy/energy/energy_volume``.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lib.eco2mix import (
    HISTORY_DATASET,
    HISTORY_FIRST_YEAR,
    LOCAL_TZ,
    REALTIME_DATASET,
    TZ,
    export_records,
    output_dir,
    realtime_day_path,
    write_and_push,
)


def ingest_history(**_) -> list[str]:
    """Download the definitive/consolidated history, one file per year."""
    paths = []
    for year in range(HISTORY_FIRST_YEAR, pendulum.now(TZ).year + 1):
        records = export_records(HISTORY_DATASET, where=f"year(date_heure) = {year}")
        if not records:
            continue
        paths.append(write_and_push(records, output_dir() / REALTIME_DATASET / f"{year}.json"))
    return paths


def ingest_realtime_window(**_) -> list[str]:
    """Download every day still present in the real-time dataset, one file per day."""
    tomorrow = pendulum.now(TZ).add(days=1).date().isoformat()
    # Skip tomorrow's forecast-only slots (text fields can't use <, so filter on date_heure).
    records = export_records(REALTIME_DATASET, where=f"date_heure < date'{tomorrow}'")
    by_day: dict[str, list[dict]] = {}
    for record in records:
        by_day.setdefault(record["date"], []).append(record)
    return [write_and_push(rows, realtime_day_path(day)) for day, rows in sorted(by_day.items())]


with DAG(
    dag_id="ingest_energy_history",
    description="Backfill all historical eCO2mix national data from ODRE",
    default_args={
        "owner": "khalil",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    start_date=datetime(2026, 11, 1, tzinfo=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["energy", "eco2mix", "backfill"],
) as dag:
    PythonOperator(task_id="download_definitive_history", python_callable=ingest_history)
    PythonOperator(task_id="download_realtime_window", python_callable=ingest_realtime_window)
