# France Energy Lakehouse

A live data lakehouse on **Databricks** built on France's national electricity data (RTE eCO2mix).
New data is collected every 15 minutes, landed as raw files, ingested incrementally, and upserted into a Delta table that always keeps the **best available version** of each 15-minute slot, as RTE replaces real-time estimates with consolidated and then definitive values.

![Live page of the dashboard: consumption now, CO2 intensity, 7-day production mix, consumption vs forecasts](docs/dashboard_live.png)

---

## The problem this solves

RTE publishes the French electricity mix (consumption, production by source, CO2 intensity, cross-border exchanges) every 15 minutes. The same time slot changes value over time:

| Status (`nature`) | When it arrives | Quality |
| --- | --- | --- |
| Données temps réel | Within minutes | Estimate from telemetry |
| Données consolidées | Mid next month | Checked and completed |
| Données définitives | Second half of next year | Final, all meters verified |

The API also publishes the rest of today's slots in advance, with only forecasts and every actual value empty.

A naive "insert new rows" pipeline keeps the first version it sees forever: estimates are never corrected, and empty forecast rows never get their real values. This project solves that with an incremental, status-aware upsert.

---

## Architecture

![Architecture: the ODRÉ API feeds two Airflow DAGs, which land raw JSON files in a Unity Catalog Volume; an hourly Databricks job (Auto Loader, cleaning, dedup, Delta MERGE) upserts them into a Delta table that serves the AI/BI dashboard](docs/architecture.png)

| Stage | Where it runs | What it does |
| --- | --- | --- |
| **Collect** | Airflow, outside Databricks | Two DAGs call the ODRÉ API: `ingest_energy_history` (manual, one-off) and `ingest_energy_current` (every 15 minutes) |
| **Land** | Unity Catalog Volume | Raw JSON files in the API's `results` envelope, uploaded with the Databricks SDK |
| **Process** | Databricks job, hourly, serverless | Auto Loader picks up new files, then clean, dedup per `date_heure`, Delta `MERGE` |
| **Store** | Delta table `energy.energy.fr_enrgy` | One row per 15-minute slot, always the best available version |
| **Serve** | AI/BI dashboard | SQL datasets that convert to Paris time and aggregate by day and month |

<details>
<summary>Text version of the diagram</summary>

```mermaid
flowchart LR
    A["RTE eCO2mix API<br/>(ODRÉ open data)"] -->|"history, once"| B["Airflow DAG<br/>history"]
    A -->|"every 15 min"| C["Airflow DAG<br/>real-time"]
    subgraph Databricks["Databricks (serverless, Unity Catalog)"]
        D[("Volume<br/>raw JSON files")]
        E["Hourly job (PySpark)<br/>Auto Loader → clean → dedup → MERGE"]
        F[("Delta table<br/>key: date_heure (UTC)")]
        G["AI/BI dashboard<br/>Paris time"]
        D --> E --> F --> G
    end
    B --> D
    C --> D
```

</details>

## Tech stack

| Layer | Tool |
| --- | --- |
| Source | RTE eCO2mix via the ODRÉ Explore API (real-time dataset + consolidated/definitive history) |
| Orchestration | Apache Airflow (2 DAGs), Python `requests` for the API, Databricks SDK to upload files to the Volume |
| Storage | Databricks Unity Catalog: a Volume for raw files, Delta Lake for the clean table |
| Processing | PySpark on Databricks (serverless): Auto Loader, window functions, Delta `MERGE` |
| Serving | Databricks AI/BI dashboard on SQL datasets |
| Monitoring | Python logging + Delta `MERGE` operation metrics |

---

## How it works

1. **Collect.** Two Airflow DAGs call the ODRÉ Explore API. They use the `/exports/json` endpoint, because `/records` is capped at 10,000 rows.
   - **`ingest_energy_history`** (manual trigger only) runs two tasks. `download_definitive_history` downloads the definitive and consolidated dataset (`eco2mix-national-cons-def`) one year at a time, from 2012 to the current year. `download_realtime_window` downloads every day still kept in the real-time dataset (`eco2mix-national-tr`, about the last 2-3 months).
   - **`ingest_energy_current`** runs every 15 minutes and re-downloads the whole current Paris day from `eco2mix-national-tr`, so a missed run is filled by the next one. During the first 2 hours after midnight it also re-downloads the previous day, to capture its last slots, which RTE publishes with a 30-45 minute delay.
2. **Land.** Each file is written locally first (under the Airflow Variable `energy_output_dir`, default `output/energy/`), then uploaded with the Databricks SDK to the same relative path in the Volume `/Volumes/energy/energy/energy_volume/`. All files land in the `eco2mix-national-tr/` folder, as `{"total_count": n, "results": [...]}`:

   | File name | Written by |
   | --- | --- |
   | `YYYY-MM-DD_YYYY-MM-DDTHH-MM-SS.json` (data day, then Paris capture time) | `ingest_energy_current`, a new file per day downloaded on every run |
   | `YYYY-MM-DD.json` | `ingest_energy_history`, real-time window, one file per day |
   | `YYYY.json` | `ingest_energy_history`, definitive and consolidated history, one file per year |

   Collection runs outside Databricks because Databricks Free Edition restricts outbound internet access.
3. **Ingest incrementally.** Auto Loader (`cloudFiles`, `trigger(availableNow=True)`) reads only the files added since the last run, using a checkpoint, and infers the schema.
4. **Clean each batch.**
   - Unwraps API envelopes (`results` / `records`).
   - Converts non-numeric sentinels such as `ND` to `NULL` with `try_cast`.
   - Drops rows without a key.
   - Keeps one row per `date_heure`: best status first, then rows with actual values, then the newest file.
5. **Upsert.** Delta `MERGE` on `date_heure`. An existing slot is updated only if the incoming version is at least as final and doesn't replace actual values with an empty forecast row. New slots are inserted.
6. **Serve.** The SQL datasets in `sql/gold/` convert `date_heure` to Paris time and compute, per day, energy by source, renewable share, CO2 intensity, peak demand, forecast error, cross-border exchanges and data completeness for the dashboard.

### The upsert rule

```python
# Dedup inside the batch: one row per slot, best version first
order = [nature_rank("nature"),               # définitives > consolidées > temps réel
         F.col("consommation").isNull(),      # rows with real values before forecast-only rows
         F.col("_file_time").desc()]          # newest file wins a tie

# MERGE: update only when the new version is at least as final,
# and never replace real values with an empty forecast row
update_condition = """
  rank(s.nature) <= rank(t.nature)
  AND NOT (s.consommation IS NULL AND t.consommation IS NOT NULL)
"""
```

---

## Key design decisions

- **The key is `date_heure` as a UTC timestamp, not `date` + `heure`.** `date` and `heure` are Paris local time: on the last Sunday of October the hour from 02:00 to 03:00 happens twice, so a local-time key would collide. `date_heure` stays unique. Paris time is used only for display.
- **Status-aware MERGE.** A better value always replaces an estimate, never the reverse. This is what makes the table trustworthy months later.
- **Forecast rows are handled explicitly.** Future slots arrive with only `prevision_j` / `prevision_j1` filled. The tie-break and the `NULL` guard stop them from overwriting real values.
- **Incremental, not full reload.** The first version reprocessed every file on each run. Auto Loader with a checkpoint reads only new files, so cost no longer grows with the history.
- **Collect every 15 minutes, process hourly.** This stays well within the ODRÉ API quota (50,000 calls per month) and the Free Edition compute quota.
- **Secrets stay out of the code.** The DAGs authenticate with the Databricks SDK's default credential chain (`DATABRICKS_HOST` / `DATABRICKS_TOKEN` environment variables, else a `~/.databrickscfg` profile), so no token is stored in the repository.

---

## Data model

**Table:** `energy.energy.fr_enrgy` (Delta, Unity Catalog)

| Column(s) | Description |
| --- | --- |
| `date_heure` | **Key.** Start of the 15-minute slot, UTC timestamp |
| `date`, `heure` | Same slot in Paris local time (informational) |
| `nature` | Data status: temps réel, consolidées or définitives |
| `consommation` | Actual consumption, MW |
| `prevision_j1`, `prevision_j` | Day-ahead and intraday consumption forecasts, MW |
| `nucleaire`, `eolien`, `solaire`, `hydraulique`, `bioenergies`, `gaz`, `fioul`, `charbon` (+ sub-types) | Production by source, MW |
| `ech_comm_*`, `ech_physiques` | Commercial exchanges by border and physical exchanges, MW (negative = France exports) |
| `taux_co2` | CO2 intensity of production, gCO2/kWh |

## Dashboard

| Page | Shows |
| --- | --- |
| Live | Consumption now, CO2 intensity, renewable share, last update; production mix; last 7 days by source; actual vs forecast |
| Trends | Daily consumption, renewable share, monthly CO2 intensity, monthly energy by source, peak demand |
| Forecasts & exchanges | Day-ahead and intraday forecast error; daily exchanges by country |
| Data quality | Minutes since last real value; rows per day vs the expected 96; rows by status per month |

Datasets behind the pages:

| Dataset | SQL file (`sql/gold/`) | Used on |
| --- | --- | --- |
| `ds_daily` | `Energy_Consumption_and_CO2_Emissions_Overview.sql` | Trends: daily consumption (GWh), peak (MW), CO2 intensity, renewable share, energy by source |
| `ds_forecast_error` | `Energy_Consumption_Prediction_Accuracy_Analysis.sql` | Forecasts & exchanges: day-ahead and intraday forecast error (%) |
| `ds_exchanges` | `Energy_Consumption_Trends_in_European_Countries.sql` | Forecasts & exchanges: daily exchanges by country (GWh) |
| `ds_quality` | `Energy_Consumption_Analysis_by_Day.sql` | Data quality: slots per day, rows by status |
| `ds_live`, `ds_latest` | Not in this repository yet | Live page, and "minutes since last real value" on Data quality |

**Trends:** daily consumption, renewable share, CO2 intensity by month, daily peak

![Trends page](docs/dashboard_trends.png)

**Forecasts & exchanges:** RTE's day-ahead and intraday forecast error, and daily cross-border exchanges (France is a net exporter on most days)

![Forecasts and exchanges page](docs/dashboard_forecasts_exchanges.png)

**Data quality:** 15-minute slots per day against the expected 96, and rows by data status per month

![Data quality page](docs/dashboard_data_quality.png)

---

## Repository structure

```
.
├── dags/
│   ├── eco2mix.py                    # shared helpers, imported as lib.eco2mix: ODRÉ API download + upload to the Volume
│   ├── ingest_energy_history.py      # DAG ingest_energy_history: manual, one-off historical load
│   └── ingest_energy_current.py      # DAG ingest_energy_current: every 15 minutes
├── databricks/
│   └── Upsert_records.ipynb          # Auto Loader + dedup + MERGE
├── sql/
│   └── gold/                         # the dashboard's SQL datasets
│       ├── Energy_Consumption_and_CO2_Emissions_Overview.sql    # ds_daily
│       ├── Energy_Consumption_Prediction_Accuracy_Analysis.sql  # ds_forecast_error
│       ├── Energy_Consumption_Trends_in_European_Countries.sql  # ds_exchanges
│       ├── Energy_Consumption_Analysis_by_Day.sql               # ds_quality
│       └── AI_Dashboard_prompt.md                               # prompt used to build the dashboard
├── docs/                             # screenshots and diagrams
└── README.md
```

## Getting started

**Prerequisites:** a Databricks workspace (Free Edition works for personal, non-commercial use), Apache Airflow 2.4+ (the DAGs use `schedule=` and Airflow 2 import paths such as `airflow.operators.python`), Python 3.10+.

1. **Create the catalog, schema and Volume** in Databricks:
   ```sql
   CREATE CATALOG IF NOT EXISTS energy;
   CREATE SCHEMA  IF NOT EXISTS energy.energy;
   CREATE VOLUME  IF NOT EXISTS energy.energy.energy_volume;
   ```
2. **Install the DAGs.** The DAGs import the helpers as `lib.eco2mix` and add the folder two levels above the DAG file to the Python path, so deploy them with this layout:
   ```
   <project>/
   ├── lib/
   │   └── eco2mix.py                # copy of dags/eco2mix.py
   ├── output/energy/                # default local output (created on first run)
   └── <airflow>/
       └── dags/                     # ingest_energy_history.py, ingest_energy_current.py
   ```
   Then install the Python dependencies on the Airflow workers: `pip install databricks-sdk requests` (`pendulum` ships with Airflow).
3. **Configure Databricks access for Airflow:** on the Airflow workers, either set the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables, or create a profile in `~/.databrickscfg` and set the Airflow Variable `databricks_profile` to its name. Optionally set the Airflow Variable `energy_output_dir` to change where files are written locally. Never commit the token.
4. **Load the history:** trigger `ingest_energy_history` once (it has no schedule). The full history is about 15 API calls, ~5 minutes and ~400 MB.
5. **Start the real-time DAG:** unpause `ingest_energy_current`. It runs every 15 minutes and doesn't backfill missed intervals (`catchup=False`).
6. **Deploy the ingestion notebook** (`databricks/Upsert_records.ipynb`) as a Databricks job scheduled hourly, with a failure email.
7. **Create the dashboard:** add each query from `sql/gold/*.sql` as a dataset in an AI/BI dashboard (`sql/gold/AI_Dashboard_prompt.md` is the prompt used to build it).

**Check that it works:**
```sql
SELECT nature, count(*) AS n_rows, min(date_heure) AS first_slot, max(date_heure) AS last_slot
FROM energy.energy.fr_enrgy
GROUP BY nature;
```

---

## Limitations

- Databricks Free Edition is for non-commercial use, with daily compute quotas; this project is a portfolio demo.
- If Airflow runs on a laptop, collection pauses while it's off. Gaps within the current day are filled by the next run; for older gaps, trigger the history DAG, which reloads every day the real-time dataset still keeps (about the last 2-3 months).
- The first real-time → consolidated transitions for live data appear mid-month, when RTE publishes the previous month's consolidated data.

## Roadmap

- [ ] Load the full 2012-today history into the table
- [ ] Data quality checks (duplicate keys, 15-minute gaps in UTC, negative values) with a quarantine table
- [ ] pytest for the upsert rule + GitHub Actions
- [x] Gold tables (daily mix, forecast error, exchanges) instead of dashboard queries
- [ ] Rename the table to `energy.silver.eco2mix` and add a raw Bronze table

---

## Data source

Data: **RTE eCO2mix**, published on [ODRÉ (Open Data Réseaux Énergies)](https://odre.opendatasoft.com/):
- [eCO2mix national real-time](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/)
- [eCO2mix national consolidated and definitive](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-cons-def/)

See each dataset's page for its licence terms. This project is not affiliated with RTE.

## Author

**Khalil Bouslah** · Data engineer · Open to Databricks and data platform work.
