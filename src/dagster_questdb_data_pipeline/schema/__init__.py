"""Database schema loader and DDL script management."""

from importlib.resources import files
from typing import Final

SCHEMA_PACKAGE: Final[str] = "dagster_questdb_data_pipeline.schema"
DDL_SUBDIR: Final[str] = "ddl"


def load_schema(filename: str) -> str:
    """Load raw SQL text from a packaged schema DDL file."""

    return files(SCHEMA_PACKAGE).joinpath(DDL_SUBDIR, filename).read_text(encoding="utf-8")


def load_all_schemas() -> list[tuple[str, str]]:
    """Return all SQL schema files sorted lexicographically by filename."""

    ddl_dir = files(SCHEMA_PACKAGE).joinpath(DDL_SUBDIR)
    schema_files = sorted([file.name for file in ddl_dir.iterdir() if file.name.endswith(".sql")])

    return [(filename, load_schema(filename)) for filename in schema_files]
