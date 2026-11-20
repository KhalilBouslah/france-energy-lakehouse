"""Helpers to download French eCO2mix electricity data from the ODRE Opendatasoft API.

API notes (ODRE Explore API v2.1):
* ``/records`` is capped at limit<=100 and offset+limit<=10000, so it cannot page
  through the history. ``/exports/{json,csv,parquet}`` has no row cap and accepts
  the same ``where`` / ``timezone`` / ``order_by`` parameters, so it is used here.
* ``eco2mix-national-tr`` (real-time, 15-min) only keeps the current and previous
  two months, and holds slots up to tomorrow 23:45 (future slots have null
  ``consommation``). Measured values are published with a ~30-45 min delay.
* ``eco2mix-national-cons-def`` (definitive + consolidated) covers 2012 up to the
  start of the real-time window; most measures are only filled on :00/:30 slots.
* Text fields (e.g. ``date``) only support ``=`` in ``where``; use ``date_heure``
  for ranges.

Output files keep the ``{"total_count": n, "results": [...]}`` shape of the
``/records`` endpoint, so a Spark reader doing ``explode("results")`` works as-is.
"""

from __future__ import annotations

import functools
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

import pendulum
import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_BASE = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
REALTIME_DATASET = "eco2mix-national-tr"
HISTORY_DATASET = "eco2mix-national-cons-def"
HISTORY_FIRST_YEAR = 2012
TZ = "Europe/Paris"
LOCAL_TZ = pendulum.timezone(TZ)
REQUEST_TIMEOUT = (10, 300)  # (connect, read) seconds
VOLUME_ROOT = "/Volumes/energy/energy/energy_volume"


def output_dir() -> Path:
    """Root output folder, overridable with the ``energy_output_dir`` Airflow Variable."""
    from airflow.models import Variable

    default = PROJECT_ROOT / "output" / "energy"
    return Path(Variable.get("energy_output_dir", default_var=str(default)))


@functools.lru_cache(maxsize=1)
def _workspace_client():
    from airflow.models import Variable
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=Variable.get("databricks_profile", default_var=None))


def volume_path_for(local_path: Path) -> str:
    """Mirror a file under ``output_dir()`` to the same relative path under ``VOLUME_ROOT``."""
    relative = Path(local_path).resolve().relative_to(output_dir().resolve())
    return f"{VOLUME_ROOT}/{relative.as_posix()}"


def push_to_volume(local_path: Path | str, volume_path: str | None = None, overwrite: bool = True) -> str:
    """Upload a local file to a Unity Catalog volume and return its volume path.

    ``volume_path`` defaults to ``VOLUME_ROOT/<file name>``; a relative path is
    resolved under ``VOLUME_ROOT``. Auth uses the Databricks SDK default chain
    (``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` env vars, else ``~/.databrickscfg``);
    set the ``databricks_profile`` Airflow Variable to pick a named profile.
    """
    local_path = Path(local_path)
    if volume_path is None:
        volume_path = f"{VOLUME_ROOT}/{local_path.name}"
    elif not volume_path.startswith("/Volumes/"):
        volume_path = f"{VOLUME_ROOT}/{volume_path.lstrip('/')}"

    with local_path.open("rb") as handle:
        _workspace_client().files.upload(volume_path, handle, overwrite=overwrite)
    logger.info("Uploaded %s to %s", local_path, volume_path)
    return volume_path


def write_and_push(records: list[dict], path: Path) -> str:
    """Write records locally, then mirror the file to the volume; return the volume path."""
    return push_to_volume(write_records(records, path), volume_path_for(path))


def export_records(dataset: str, where: str | None = None) -> list[dict]:
    """Return all records of ``dataset`` matching ``where`` via the export endpoint."""
    params = {"timezone": TZ, "order_by": "date_heure"}
    if where:
        params["where"] = where
    url = f"{API_BASE}/{dataset}/exports/json"
    logger.info("GET %s params=%s", url, params)
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    records = response.json()
    if not isinstance(records, list):
        raise ValueError(f"Unexpected response from {url}: {str(records)[:500]}")
    return records


def write_records(records: list[dict], path: Path) -> Path:
    """Atomically write records as ``{"total_count", "results"}`` JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump({"total_count": len(records), "results": records}, handle, ensure_ascii=False)
    os.replace(tmp_path, path)
    logger.info("Wrote %d records to %s", len(records), path)
    return path


def realtime_day_path(day: date | str) -> Path:
    day_str = day if isinstance(day, str) else day.isoformat()
    return output_dir() / REALTIME_DATASET / f"{day_str}.json"


def realtime_snapshot_path(day: date, captured_at: datetime) -> Path:
    """One file per run: ``<day>/<capture time>.json`` so 15-min refreshes never overwrite each other.

    The folder is the data day; the file name is the Paris capture time (a refresh
    of yesterday just after midnight carries today's date). Colons are replaced by
    ``-`` because Spark/Hadoop reject ``:`` in file names.
    """
    stamp = captured_at.astimezone(LOCAL_TZ).strftime("%Y-%m-%dT%H-%M-%S")
    return output_dir() / REALTIME_DATASET / f"{stamp}.json"


def ingest_realtime_day(day: date, captured_at: datetime | None = None) -> str:
    """Download one Paris day of the real-time dataset and push it to the volume.

    Without ``captured_at`` the daily file ``<day>.json`` is overwritten; with it,
    a new snapshot file is written (see ``realtime_snapshot_path``).
    """
    records = export_records(REALTIME_DATASET, where=f'date = "{day.isoformat()}"')
    if not records:
        raise ValueError(
            f"No {REALTIME_DATASET} records for {day}: the real-time dataset only "
            "keeps about the last 2-3 months."
        )
    measured = [r for r in records if r.get("consommation") is not None]
    last = measured[-1]["heure"] if measured else "none"
    logger.info("%s: %d slots, %d measured (last measured slot %s)", day, len(records), len(measured), last)
    path = realtime_day_path(day) if captured_at is None else realtime_snapshot_path(day, captured_at)
    return write_and_push(records, path)
