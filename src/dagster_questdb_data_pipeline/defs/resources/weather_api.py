from datetime import date
from typing import Any, Final

import dagster as dg
import httpx2
from pydantic import Field

from dagster_questdb_data_pipeline.models.weather import HourlyWeatherData, OpenMeteoResponse

HISTORICAL_URL: Final[str] = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL: Final[str] = "https://api.open-meteo.com/v1/forecast"


class WeatherApiResource(dg.ConfigurableResource):
    """Resource for fetching weather metrics from Open-Meteo."""

    timeout_seconds: float = Field(
        default=30.0,
        description="HTTP request timeout in seconds.",
    )
    latitude: float = Field(
        description="Latitude coordinate.",
    )
    longitude: float = Field(
        description="Longitude coordinate.",
    )

    def fetch_historical_hourly(
        self,
        start_date: date,
        end_date: date,
    ) -> OpenMeteoResponse:
        selected_metrics = HourlyWeatherData.metric_names()

        params: dict[str, Any] = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(selected_metrics),
            "timezone": "GMT",
        }

        with httpx2.Client(timeout=self.timeout_seconds) as client:
            response = client.get(HISTORICAL_URL, params=params)
            response.raise_for_status()

            return OpenMeteoResponse.model_validate_json(response.text)

    def fetch_forecast_hourly(
        self,
    ) -> OpenMeteoResponse:
        selected_metrics = HourlyWeatherData.metric_names()

        params: dict[str, Any] = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "forecast_days": 1,
            "hourly": ",".join(selected_metrics),
            "timezone": "GMT",
        }

        with httpx2.Client(timeout=self.timeout_seconds) as client:
            response = client.get(FORECAST_URL, params=params)
            response.raise_for_status()

            return OpenMeteoResponse.model_validate_json(response.text)
