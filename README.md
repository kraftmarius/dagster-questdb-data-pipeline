# dagster-questdb-boilerplate

Starter boilerplate for building time-series data pipelines with
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
- **Quality gates**: `ruff` (lint + format), `ty` (type check), `dg check defs`

## Architecture

```
┌─────────────────────┐      ┌───────────────────────────────┐      ┌──────────────┐
│  Open-Meteo Archive │ ───► │  weather_telemetry_raw        │ ───► │    QuestDB   │
│  API (hourly data)  │      │  @dg.asset · daily partitions │      │  (REST :9000)│
└─────────────────────┘      └───────────────────────────────┘      └──────────────┘
```

The asset declares `compute_kind="qwp"` (QuestDB Writer Protocol), so the
Dagster UI attributes compute to the time-series database rather than the
Python worker.

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

In the Dagster UI, select the `weather_telemetry_raw` asset (group
`telemetry_ingest`), pick a partition, and materialize. Run metadata includes
`record_count`, `ingested_count`, `target_date`, and a Markdown data preview.
Verify the table via the QuestDB console:

```sql
SELECT * FROM weather_telemetry_raw ORDER BY timestamp DESC LIMIT 42;
```

## Configuration

All configuration is sourced from `.env` (auto-loaded by `just`):

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPOSE_PROJECT_NAME` | `dagster_questdb_boilerplate` | Prefix for Docker networks/volumes |
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
├── src/dagster_questdb_boilerplate/
│   ├── definitions.py              # @definitions entry point (defs/ auto-discovery)
│   └── defs/
│       ├── assets.py               # weather_telemetry_raw (daily partitions)
│       └── resources.py            # WeatherApiResource, QuestDbResource
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

- **New source:** subclass `ConfigurableResource` in `defs/resources.py`,
  register it in `resources()`
- **New asset:** add a module under `defs/` (auto-discovered), use
  `QuestDbResource.ingest_dataframe()` for writes
- **New infrastructure:** add a compose file under `infra/*` and include it
  from `infra/compose.yaml`
- **Workspace expansion:** the `dg` registry is pre-wired for
  `dagster_questdb_boilerplate.components.*`
