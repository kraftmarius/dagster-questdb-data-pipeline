from dataclasses import dataclass
from typing import Final, Self

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

WEATHER_RAW_TABLE: Final[str] = "weather_raw"
WEATHER_DAILY_ROLLUP_TABLE: Final[str] = "weather_daily_rollup"


@dataclass(frozen=True)
class MetricBound:
    """Physically plausible range for a weather metric, derived from WMO world records."""

    min_value: float
    max_value: float
    unit: str


# WMO-derived validation thresholds (single source of truth).
WMO_BOUNDS = {
    "temperature_2m": MetricBound(min_value=-95.0, max_value=65.0, unit="°C"),
    "relative_humidity_2m": MetricBound(min_value=0.0, max_value=100.0, unit="%"),
    "pressure_msl": MetricBound(min_value=850.0, max_value=1100.0, unit="hPa"),
    "wind_speed_10m": MetricBound(min_value=0.0, max_value=500.0, unit="km/h"),
}


@dataclass(frozen=True)
class RollupProjection:
    """Specification of an in-engine SQL rollup projection."""

    expression: str
    alias: str
    unit: str


ROLLUP_PROJECTIONS: Final[tuple[RollupProjection, ...]] = (
    RollupProjection("avg(temperature_2m)", "avg_temperature_2m", "°C"),
    RollupProjection("min(temperature_2m)", "min_temperature_2m", "°C"),
    RollupProjection("max(temperature_2m)", "max_temperature_2m", "°C"),
    RollupProjection(
        "(max(temperature_2m) - min(temperature_2m))", "diurnal_temperature_range", "°C"
    ),
    RollupProjection("avg(relative_humidity_2m)", "avg_relative_humidity_2m", "%"),
    RollupProjection("avg(pressure_msl)", "avg_pressure_msl", "hPa"),
    RollupProjection("max(wind_speed_10m)", "max_wind_speed_10m", "km/h"),
)


class HourlyWeatherData(BaseModel):
    """Hourly weather metrics from the Open-Meteo Archive API."""

    time: list[str] = Field(description="ISO-8601 timestamps (UTC).")
    temperature_2m: list[float] = Field(description="Air temperature in Celsius.")
    relative_humidity_2m: list[float] = Field(description="Relative humidity in percent.")
    pressure_msl: list[float] = Field(description="Atmospheric pressure at sea level in hPa.")
    wind_speed_10m: list[float] = Field(description="Wind speed in km/h.")

    @classmethod
    def metric_names(cls) -> list[str]:
        """Return all weather metric column names excluding timestamp."""

        return [field for field in cls.model_fields if field != "time"]

    @model_validator(mode="after")
    def validate_column_lengths_and_ranges(self) -> Self:
        """Enforce column length consistency and WMO range bounds on all metrics."""

        expected_len = len(self.time)

        for metric in self.metric_names():
            values = getattr(self, metric)
            if len(values) != expected_len:
                raise ValueError(
                    f"Column '{metric}' length ({len(values)}) does not match expected length ({expected_len})."
                )

            bounds = WMO_BOUNDS.get(metric)
            if bounds:
                for val in values:
                    if not (bounds.min_value <= val <= bounds.max_value):
                        raise ValueError(
                            f"Metric '{metric}' value {val} violates WMO bounds "
                            f"[{bounds.min_value}, {bounds.max_value}] {bounds.unit}."
                        )

        return self

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to a pandas DataFrame, renaming `time` to `timestamp` (UTC)."""

        df = pd.DataFrame(self.model_dump())
        df["timestamp"] = pd.to_datetime(df.pop("time"), utc=True)

        return df


class OpenMeteoResponse(BaseModel):
    """Top-level response from the Open-Meteo Archive API."""

    model_config = ConfigDict(extra="ignore")

    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    hourly: HourlyWeatherData
