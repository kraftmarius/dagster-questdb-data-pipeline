from datetime import date
from typing import Any

import dagster as dg
import httpx2
from pydantic import Field

from dagster_questdb_data_pipeline.models.weather import HourlyWeatherData, OpenMeteoResponse


class WeatherApiResource(dg.ConfigurableResource):
    """Resource for fetching historical weather metrics from Open-Meteo."""

    base_url: str = Field(
        default="https://archive-api.open-meteo.com/v1/archive",
        description="Base URL for the Open-Meteo Historical Weather API.",
    )
    timeout_seconds: float = Field(
        default=30.0,
        description="HTTP request timeout in seconds.",
    )
    default_latitude: float = Field(
        description="Latitude coordinate.",
    )
    default_longitude: float = Field(
        description="Longitude coordinate.",
    )

    def fetch_hourly(
        self,
        start_date: date,
        end_date: date,
        latitude: float | None = None,
        longitude: float | None = None,
        metrics: list[str] | None = None,
    ) -> OpenMeteoResponse:
        lat = latitude if latitude is not None else self.default_latitude
        lon = longitude if longitude is not None else self.default_longitude

        selected_metrics = metrics or HourlyWeatherData.metric_names()

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

            return OpenMeteoResponse.model_validate_json(response.text)
