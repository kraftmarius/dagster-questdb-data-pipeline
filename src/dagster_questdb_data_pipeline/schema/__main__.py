"""CLI entrypoint for bootstrapping database schemas against QuestDB."""

import os
import sys

import questdb as qdb

from dagster_questdb_data_pipeline.schema import load_all_schemas


def bootstrap_schema() -> None:
    """Execute all packaged SQL DDL schema files against target QuestDB instance."""

    host = os.getenv("QDB_HOST", "localhost")
    port = int(os.getenv("QDB_PORT", "9000"))
    username = os.getenv("QDB_PG_USER")
    password = os.getenv("QDB_PG_PASSWORD")

    schemas = load_all_schemas()

    if not schemas:
        print("No schema DDL files found.")

        return

    print(f"Connecting to QuestDB at {host}:{port}...")

    try:
        with qdb.connect(host=host, port=port, username=username, password=password) as db:
            for filename, sql in schemas:
                print(f"Applying schema: {filename}...", end=" ", flush=True)
                db.query(sql)
                print("OK")

    except (qdb.QuestDBError, OSError) as e:
        print(f"\nSchema provisioning failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("All schemas successfully applied.")


if __name__ == "__main__":
    bootstrap_schema()
