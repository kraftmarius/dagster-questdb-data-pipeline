from datetime import date
from typing import Any

import dagster as dg
import httpx2
import pandas as pd
import questdb as qdb
from pydantic import Field


class WeatherApiResource(dg.ConfigurableResource):
    """Resource for fetching historical weather telemetry from Open-Meteo."""

    base_url: str = Field(
        default="https://archive-api.open-meteo.com/v1/archive",
        description="Base URL for the Open-Meteo Historical Weather API.",
    )
    timeout_seconds: float = Field(
        default=30.0,
        description="HTTP request timeout in seconds.",
    )
    default_latitude: float = Field(
        description="Latitude coordinate for telemetry extraction.",
    )
    default_longitude: float = Field(
        description="Longitude coordinate for telemetry extraction.",
    )

    def fetch_hourly(
        self,
        start_date: date,
        end_date: date,
        latitude: float | None = None,
        longitude: float | None = None,
        metrics: list[str] | None = None,
    ) -> dict[str, Any]:
        """Fetch hourly time-series telemetry data for a defined time window."""

        lat = latitude if latitude is not None else self.default_latitude
        lon = longitude if longitude is not None else self.default_longitude

        selected_metrics = metrics or [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
        ]

        params: dict[str, Any] = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(selected_metrics),
            "timezone": "UTC",
        }

        with httpx2.Client(timeout=self.timeout_seconds) as client:
            response = client.get(self.base_url, params=params)
            response.raise_for_status()

            return response.json()


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

        with self.connect() as questdb, questdb.sender() as sender:
            sender.dataframe(df, table_name=table_name, at="timestamp")

        return len(df)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "weather_api": WeatherApiResource(
                default_latitude=dg.EnvVar("LATITUDE").get_value(),
                default_longitude=dg.EnvVar("LONGITUDE").get_value(),
            ),
            "questdb": QuestDbResource(
                host=dg.EnvVar("QDB_HOST").get_value(),
                port=dg.EnvVar().int("QDB_PORT"),
                username=dg.EnvVar("QDB_PG_USER").get_value(),
                password=dg.EnvVar("QDB_PG_PASSWORD").get_value(),
            ),
        },
    )
