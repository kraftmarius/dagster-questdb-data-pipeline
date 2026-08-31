# dagster-questdb-data-pipeline

Time-series data pipeline template built with
[Dagster](https://dagster.io) and [QuestDB](https://questdb.com).

Out of the box it ingests hourly weather telemetry from the
[Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api)
into QuestDB and computes in-engine daily rollups — a complete
**API → raw asset → rollup asset → time-series database** flow ready
to be repurposed.

## What's inside

- **Dagster** definitions using the modern `dg` (Dagster Grid) project layout
  with `load_from_defs_folder` auto-discovery
- **QuestDB 10.0.1** as local Docker Compose infrastructure (no code needed
  to stand it up)
- **Two configurable resources**:
  - `WeatherApiResource` — Open-Meteo historical API client
  - `QuestDbResource` — QuestDB client with `ingest_dataframe()` for
    high-throughput DataFrame ingestion via the official Python client
- **Schema provisioning** — packaged DDL applied via `just db-init` (auto-run
  by `just dev`); idempotent `CREATE TABLE IF NOT EXISTS`
- **Daily partitions** on all assets for backfillable, idempotent runs
- **Env-driven configuration** — no secrets in code; everything is sourced
  from `.env`
- **`just` task runner** — one command to boot the full dev environment
- **Typed API responses** — Pydantic models with WMO-based range validation
  (temperature, humidity, pressure, wind speed) fail fast on out-of-bounds data
- **In-engine daily rollup** — `SAMPLE BY 1d ALIGN TO CALENDAR` aggregates
  24 hourly rows into daily statistics directly in QuestDB (no Python
  compute), driven by a typed `ROLLUP_PROJECTIONS` domain model
- **Data integrity checks** — blocking `asset_check` on each asset verifies
  row count, metric bounds, and mathematical consistency (min ≤ avg ≤ max)
- **Quality gates**: `ruff` (lint + format), `ty` (type check), `dg check defs`

## Architecture

```
┌──────────────────────┐      ┌───────────────────────────────┐      ┌──────────────────────────────┐      ┌────────────────┐
│  Open-Meteo Archive  │ ───► │          weather_raw          │ ───► │      weather_daily_rollup    │ ───► │     QuestDB    │
│   API (hourly data)  │      │  @dg.asset · daily partitions │      │  @dg.asset · in-engine SQL   │      │  (REST :9000)  │
└──────────────────────┘      └───────────────────────────────┘      └──────────────────────────────┘      └────────────────┘
```

Both assets declare `kinds={"questdb", "sql"}` (rollup) or
`kinds={"questdb"}` (raw), so the Dagster UI attributes compute
to the time-series database rather than the Python worker.

| Port | Protocol | Purpose | Exposed by default |
|------|----------|---------|--------------------|
| 8812 | PostgreSQL wire | SQL clients (`psql`, DBeaver, …) | No |
| 9000 | HTTP | REST API + Web Console (used by `QuestDbResource`) | Yes — host port set by `QDB_PORT` (default `9000`) |
| 9003 | HTTP | Min health server (container healthcheck) | No (container-internal) |
| 9009 | TCP | InfluxDB line protocol | No |

Only the REST API is published to the host. To expose another port,
add a mapping under `ports` in `infra/databases/compose.yaml`
(e.g. `"8812:8812"` for SQL clients).

## Prerequisites

| Tool | Purpose |
|------|---------|
| [uv](https://docs.astral.sh/uv/) | Environment & dependency management (Python 3.13 pinned via `.python-version`) |
| [just](https://github.com/casey/just) | Task runner |
| Docker (Compose v2) | QuestDB infrastructure |

## Getting started

```bash
# 1. Create your local environment file (working defaults included)
cp .env.sample .env

# 2. Sync the Python environment (from uv.lock)
just init

# 3. Start everything (QuestDB + Dagster dev server)
just dev
```

The sample ships pre-configured, see `.env.sample`. To point the pipeline at a real site, set
`LATITUDE`/`LONGITUDE` in `.env`.

`just dev` starts the infrastructure with `--wait` (healthchecked), provisions
the database schema, launches `dg dev`, and **automatically tears down
containers on exit** (Ctrl-C). Data is not persisted across runs by design.

| Service | URL |
|---------|-----|
| Dagster UI | http://localhost:3000 |
| QuestDB Web Console | http://localhost:9000 |

### Running the sample pipeline

In the Dagster UI, select an asset (`weather_raw` or `weather_daily_rollup`),
pick a partition, and materialize. Each asset has a blocking
`integrity_check`: the raw check validates row count (24) and WMO metric
bounds; the rollup check validates mathematical consistency
(min ≤ avg ≤ max, diurnal range) and WMO bounds. Verify the tables via
the QuestDB console:

```sql
SELECT * FROM weather_raw ORDER BY timestamp DESC LIMIT 42;
SELECT * FROM weather_daily_rollup ORDER BY timestamp DESC LIMIT 30;
```

### WMO Validation Thresholds

Validation thresholds are derived from the [World Meteorological Organization (WMO)
World Weather & Climate Extremes Archive](https://wmo.int/files/records-of-weather-and-climate-extremes-table):

| Metric | Bounds | Reference Record |
|--------|--------|------------------|
| Temperature | [-95.0, +65.0] °C | -89.2 °C (Vostok), +56.7 °C (Death Valley) |
| Sea Level Pressure | [850.0, 1100.0] hPa | 870.0 hPa (Typhoon Tip), 1084.8 hPa (Tosontsengel) |
| Wind Speed | [0.0, 500.0] km/h | 408 km/h non-tornadic gust (Barrow Island) |
| Relative Humidity | [0.0, 100.0] % | Thermodynamic physical limits |

## Configuration

All configuration is sourced from `.env` (auto-loaded by `just`):

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPOSE_PROJECT_NAME` | `dagster_questdb_data_pipeline` | Prefix for Docker networks/volumes |
| `LATITUDE` | `0` | Telemetry coordinate — latitude (see `.env.sample`) |
| `LONGITUDE` | `0` | Telemetry coordinate — longitude (see `.env.sample`) |
| `QDB_HOST` | `localhost` | QuestDB host (Dagster resource) |
| `QDB_PORT` | `9000` | QuestDB REST port (container + resource) |
| `QDB_PG_USER` | `admin` | QuestDB user (container + resource) |
| `QDB_PG_PASSWORD` | `admin` | QuestDB password (container + resource) |
| `QDB_TELEMETRY_ENABLED` | `false` | QuestDB anonymous telemetry |
| `QDB_PG_READONLY_USER_ENABLED` | `true` | Provision a read-only PG user |
| `QDB_PG_READONLY_USER` | `readonly` | Read-only user (analytics access) |
| `QDB_PG_READONLY_PASSWORD` | `readonly` | Read-only password |

Defaults are local-dev only. Change all credentials before any non-local use.

## Project layout

```
├── infra/
│   ├── compose.yaml                # Root compose (includes databases)
│   └── databases/
│       └── compose.yaml            # QuestDB 10.0.1
├── src/dagster_questdb_data_pipeline/
│   ├── definitions.py              # @definitions entry point (defs/ auto-discovery)
│   ├── defs/
│   │   ├── assets/
│   │   │   └── weather/
│   │   │       ├── raw.py              # weather_raw asset + integrity check
│   │   │       └── daily_rollup.py     # weather_daily_rollup asset + integrity check
│   │   └── resources/
│   │       ├── weather_api.py      # WeatherApiResource
│   │       └── questdb.py          # QuestDbResource
│   ├── models/
│   │   └── weather.py              # Pydantic models, WMO_BOUNDS, ROLLUP_PROJECTIONS, table constants
│   └── schema/
│       ├── __main__.py             # CLI entrypoint (`python -m …schema`)
│       └── ddl/
│           ├── weather_raw.sql             # QuestDB DDL (hourly, partitioned, WAL, dedup)
│           └── weather_daily_rollup.sql    # QuestDB DDL (daily, partitioned, WAL, dedup)
├── tests/
├── justfile
├── pyproject.toml
└── uv.lock
```

Dagster definitions live in `defs/` and are auto-discovered by
`load_from_defs_folder`. `pyproject.toml` pre-wires `components.*` as `dg`
registry modules for future multi-project (workspace) expansion.

## Tasks

Run `just list` for the full reference.

| Task | Description |
|------|-------------|
| `just init` | Sync environment with lockfile (`uv sync --all-groups`) |
| `just dev` | Start infrastructure, provision schema, then Dagster dev server (auto-cleanup on exit) |
| `just db-init` | Provision QuestDB tables from packaged DDL (private helper; auto-run by `just dev`) |
| `just lint` | `ruff check` + `ruff format --check` + `ty check` + `dg check defs` |
| `just fmt` | `ruff format` |
| `just fix` | Format + auto-fix lint and type issues |
| `just upgrade` | Upgrade all dependencies (`uv lock --upgrade`) |
| `just infra up` / `down` / `restart` / `update` | Manage infrastructure stack |
| `just infra databases logs` | Tail QuestDB logs |

## Quality

CI-friendly quality gates, all enforced by `just lint`:

- **ruff** — lint + format (line length 100, `py313`)
- **ty** — static type checking
- **dg check defs** — Dagster definitions load & validate

## Extending

- **New source:** subclass `ConfigurableResource` in `defs/resources/`,
  register it in `resources()`
- **New asset:** add a module under `defs/` (auto-discovered), add a DDL file
  under `schema/ddl/`, then use `QuestDbResource.ingest_dataframe()` for writes
- **New infrastructure:** add a compose file under `infra/*` and include it
  from `infra/compose.yaml`
- **Workspace expansion:** the `dg` registry is pre-wired for
  `dagster_questdb_data_pipeline.components.*`

## Renaming the project

This is a template — rename it to your project and run `rm -rf .venv && just init`. The name appears in **two forms** that must stay consistent:

- **Module** (snake_case) — `dagster_questdb_data_pipeline`: the Python package name used for imports, `root_module`, and `COMPOSE_PROJECT_NAME`.
- **Distribution** (kebab-case) — `dagster-questdb-data-pipeline`: the project directory name and the README title.

### Why you must delete `.venv`

uv writes Python **console scripts** (`dg`, `dagster`, `dagster-webserver`, and your CLI command) as text files with a **hardcoded absolute shebang** to the venv interpreter. Renaming the project directory orphans that path, so the scripts fail to spawn:

```
error: Failed to spawn: `dg`
  Caused by: No such file or directory (os error 2)
```

`uv sync` will not repair this — the installed packages are still valid, only the path moved. Deleting `.venv` and re-syncing regenerates every shebang, **but `rm -rf .venv && just init` must run at the final directory path** (after the project root is renamed); syncing earlier just bakes the stale path back in.
