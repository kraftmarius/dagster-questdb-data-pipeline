# dagster-questdb-data-pipeline

Time-series data pipeline template built with
[Dagster](https://dagster.io) and [QuestDB](https://questdb.com).

Out of the box it ingests hourly weather telemetry from the
[Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api)
into QuestDB through a daily-partitioned Dagster asset — a complete
**API → asset → time-series database** flow ready to be repurposed.

## What's inside

- **Dagster** definitions using the modern `dg` (Dagster Grid) project layout
  with `load_from_defs_folder` auto-discovery
- **QuestDB 10.0.1** as local Docker Compose infrastructure (no code needed
  to stand it up)
- **Two configurable resources**:
  - `WeatherApiResource` — Open-Meteo historical API client
  - `QuestDbResource` — QuestDB client with `ingest_dataframe()` for
    high-throughput DataFrame ingestion via the official Python client
- **Daily partitions** on the sample asset for backfillable, idempotent runs
- **Env-driven configuration** — no secrets in code; everything is sourced
  from `.env`
- **`just` task runner** — one command to boot the full dev environment
- **Typed API responses** — Pydantic models with WMO-based range validation
  (temperature, humidity, pressure, wind speed) fail fast on out-of-bounds data
- **Data integrity check** — a blocking `asset_check` verifies row count and
  metric bounds in QuestDB after each ingestion
- **Quality gates**: `ruff` (lint + format), `ty` (type check), `dg check defs`

## Architecture

```
┌──────────────────────┐      ┌───────────────────────────────┐      ┌────────────────┐
│  Open-Meteo Archive  │ ───► │          weather_raw          │ ───► │     QuestDB    │
│   API (hourly data)  │      │  @dg.asset · daily partitions │      │  (REST :9000)  │
└──────────────────────┘      └───────────────────────────────┘      └────────────────┘
```

The asset declares `kinds={"questdb"}`, so the Dagster UI attributes compute
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

`just dev` starts the infrastructure with `--wait` (healthchecked), launches
`dg dev`, and **automatically tears down containers on exit** (Ctrl-C). Data
is not persisted across runs by design.

| Service | URL |
|---------|-----|
| Dagster UI | http://localhost:3000 |
| QuestDB Web Console | http://localhost:9000 |

### Running the sample pipeline

In the Dagster UI, select the `weather_raw` asset, pick a partition, and
materialize. Run metadata includes `table`, `row_count`, and `target_date`.
A blocking `weather_raw_integrity_check` runs automatically after ingestion,
validating row count (24) and WMO metric bounds. Verify the table via the
QuestDB console:

```sql
SELECT * FROM weather_raw ORDER BY timestamp DESC LIMIT 42;
```

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
│   │   │   └── weather_raw.py      # weather_raw asset + integrity check
│   │   └── resources/
│   │       ├── weather_api.py      # WeatherApiResource
│   │       └── questdb.py          # QuestDbResource
│   └── models/
│       └── weather.py              # Pydantic models (OpenMeteoResponse, HourlyWeatherData)
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
| `just dev` | Start infrastructure + Dagster dev server (auto-cleanup on exit) |
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
- **New asset:** add a module under `defs/` (auto-discovered), use
  `QuestDbResource.ingest_dataframe()` for writes
- **New infrastructure:** add a compose file under `infra/*` and include it
  from `infra/compose.yaml`
- **Workspace expansion:** the `dg` registry is pre-wired for
  `dagster_questdb_data_pipeline.components.*`

## Renaming the project

This is a template — rename it to your product. The name exists in **two forms** that must stay consistent:

- **Module** (snake_case) — `dagster_questdb_data_pipeline`: Python package, `root_module`, ruff `known-first-party`, `registry_modules`, `[project] name`, and `COMPOSE_PROJECT_NAME`.
- **Distribution** (kebab-case) — `dagster-questdb-data-pipeline`: the README title and the root package name in `uv.lock`.

### Rename checklist

| Location | Current value |
|----------|---------------|
| `pyproject.toml` → `[project] name` | `dagster_questdb_data_pipeline` |
| `pyproject.toml` → `[tool.ruff.lint.isort] known-first-party` | `["dagster_questdb_data_pipeline"]` |
| `pyproject.toml` → `[tool.dg.project] root_module` | `dagster_questdb_data_pipeline` |
| `pyproject.toml` → `[tool.dg.project] registry_modules` | `dagster_questdb_data_pipeline.components.*` |
| `src/<module>/` | `src/dagster_questdb_data_pipeline/` |
| `src/<module>/defs/assets/weather_raw.py` | `from dagster_questdb_data_pipeline.defs.resources.questdb import …` |
| `justfile` → `COMPOSE_PROJECT_NAME` default | `dagster_questdb_data_pipeline` |
| `.env` / `.env.sample` → `COMPOSE_PROJECT_NAME` | `dagster_questdb_data_pipeline` |
| `README.md` | title, config table, layout tree |

`uv.lock` (root `[[package]] name`) **regenerates** on the next sync — do not hand-edit.

### Procedure

```bash
# 1. Rename the project directory itself (run from the parent directory)
mv dagster-questdb-data-pipeline <new_project_dir>
cd <new_project_dir>

# 2. Rename the package directory
git mv src/dagster_questdb_data_pipeline src/<new_module_name>

# 3. Replace every name reference (a repo-wide search/replace of the two forms
#    above covers pyproject.toml, assets.py, justfile, .env.sample, README.md)

# 4. Regenerate the lockfile and venv — MUST run last, at the final path
rm -rf .venv
just init
```

### Why you must delete `.venv`

uv writes Python **console scripts** (`dg`, `dagster`, `dagster-webserver`, and your CLI command) as text files with a **hardcoded absolute shebang** to the venv interpreter. Renaming the project directory orphans that path, so the scripts fail to spawn:

```
error: Failed to spawn: `dg`
  Caused by: No such file or directory (os error 2)
```

`uv sync` will not repair this — the installed packages are still valid, only the path moved. Deleting `.venv` and re-syncing regenerates every shebang, **but it must run at the final directory path** (after step 1); syncing earlier just bakes the stale path back in.
