import dagster as dg
import pandas as pd
import questdb as qdb
from pydantic import Field


class QuestDbResource(dg.ConfigurableResource):
    """Resource for interacting with QuestDB."""

    host: str = Field(
        default="localhost",
        description="Hostname of the QuestDB server.",
    )
    port: int = Field(
        default=9000,
        description="Port of the QuestDB server.",
    )
    username: str = Field(
        description="Username for connecting to QuestDB.",
    )
    password: str = Field(
        description="Password for connecting to QuestDB.",
    )

    def connect(self) -> qdb.QuestDB:
        """Connect to QuestDB."""

        return qdb.connect(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )

    def ingest_dataframe(self, table_name: str, df: pd.DataFrame) -> int:
        """Ingest records into a QuestDB table via pandas DataFrame."""

        if df.empty:
            return 0

        with self.connect() as db, db.sender() as sender:
            sender.dataframe(df, table_name=table_name, at="timestamp")

        return len(df)
