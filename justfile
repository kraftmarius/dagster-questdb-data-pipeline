set dotenv-load := true

# List recipes
list:
    @just --list --unsorted

# Synchronize environment with lockfile
init:
    @uv sync --all-groups

# Upgrade all dependencies and update lockfile
upgrade:
    @uv lock --upgrade

# Run project
run:
    @uv run dagster-questdb-boilerplate

# Run dagster dev server
dg-dev:
    @uv run dagster dev

# Run all quality checks
lint:
    @uv run ruff check
    @uv run ruff format --check
    @uv run ty check

# Format source code
fmt:
    @uv run ruff format

# Automatically fix lint and format issues
fix: fmt
    @uv run ruff check --fix
    @uv run ty check --fix
