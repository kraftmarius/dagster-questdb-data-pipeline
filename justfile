set dotenv-load := true

# List recipes
list:
    @just --list --unsorted

#Initialize project
init:
    @uv sync

# Upgrade all packages
upgrade:
    @uv lock --upgrade

# Run project
run:
    @uv run dagster-questdb-boilerplate

# Run all quality checks and fail on any warning
lint:
    @uvx ruff check
    @uvx ruff format --check

# Format code according to style guidelines
fmt:
    @uvx ruff format

# Quick fix for formatting and linting issues
fix:
    @uvx ruff check --fix
    @just fmt