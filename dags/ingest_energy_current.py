"""Every 15 minutes, refresh today's eCO2mix real-time data from ODRE.

Each run re-downloads the whole current Paris day from ``eco2mix-national-tr``
(96 slots, ~90 KB) into a new snapshot file
``<energy_output_dir>/eco2mix-national-tr/YYYY-MM-DD/YYYY-MM-DDTHH-MM-SS.json``
(folder = data day, file name = run time in Paris), so runs never overwrite each other.
Measured values lag ~30-45 min, so for the first ``LOOKBACK`` after midnight the
run also snapshots yesterday to capture its last slots.
Each file is mirrored to ``/Volumes/energy/energy/energy_volume/eco2mix-national-tr/``.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.types import DagRunType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lib.eco2mix import LOCAL_TZ, TZ, ingest_realtime_day

LOOKBACK = timedelta(hours=2)


def run_time(dag_run, data_interval_end: pendulum.DateTime, logical_date: pendulum.DateTime) -> pendulum.DateTime:
    """Paris time this run represents.

    Scheduled runs: the end of their 15-min interval (= when they fire).
    Manual runs (UI trigger / ``airflow tasks test``): the logical date picked,
    which defaults to the trigger time.
    """
    if dag_run is not None and dag_run.run_type == DagRunType.SCHEDULED:
        return data_interval_end.in_timezone(TZ)
    return logical_date.in_timezone(TZ)


def ingest_current(dag_run, data_interval_end, logical_date, **_) -> list[str]:
    now = run_time(dag_run, data_interval_end, logical_date)
    days = sorted({(now - LOOKBACK).date(), now.date()})
    return [ingest_realtime_day(day, captured_at=now) for day in days]


with DAG(
    dag_id="ingest_energy_current",
    description="Refresh today's eCO2mix real-time data from ODRE every 15 minutes",
    default_args={
        "owner": "khalil",
        "depends_on_past": False,
        # Short retries: the next run is only 15 minutes away anyway.
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    start_date=datetime(2026, 11, 1, tzinfo=LOCAL_TZ),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    tags=["energy", "eco2mix", "ingest"],
) as dag:
    PythonOperator(task_id="download_current_day", python_callable=ingest_current)
