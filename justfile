mod infra

set dotenv-load := true

export COMPOSE_PROJECT_NAME := env_var_or_default('COMPOSE_PROJECT_NAME', 'dagster_questdb_data_pipeline')

# List recipes
list:
    @just --list --unsorted

# Synchronize environment with lockfile
init:
    @uv sync --all-groups

# Upgrade all dependencies and update lockfile
upgrade:
    @uv lock --upgrade

# Start the development environment (data not persisted)
dev:
    #!/usr/bin/env bash
    cleanup() {
      just infra down --volumes || true
    }

    trap cleanup EXIT SIGINT SIGTERM

    echo "=== Starting infrastructure... ==="
    just infra up --wait

    echo "=== Starting Dagster dev server... ==="
    uv run dg dev

# Run all quality checks
lint:
    @uv run ruff check
    @uv run ruff format --check
    @uv run ty check
    @uv run dg check defs

# Format source code
fmt:
    @uv run ruff format

# Automatically fix lint and format issues
fix:
    @uv run ruff check --fix
    @uv run ty check --fix
    @just fmt
