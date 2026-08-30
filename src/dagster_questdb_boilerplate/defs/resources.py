from datetime import date
from typing import Any

import dagster as dg
import httpx2
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


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "weather_api": WeatherApiResource(
                default_latitude=dg.EnvVar("LATITUDE").get_value(),
                default_longitude=dg.EnvVar("LONGITUDE").get_value(),
            )
        },
    )
