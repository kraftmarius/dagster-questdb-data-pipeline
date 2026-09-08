import operator
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal, Self, TypedDict

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

WEATHER_GROUP_NAME: Final[str] = "weather"
WEATHER_RAW_TABLE: Final[str] = "weather_raw"
WEATHER_DAILY_ROLLUP_TABLE: Final[str] = "weather_daily_rollup"
WEATHER_FORECAST_TABLE: Final[str] = "weather_forecast"


class WeatherMetric(StrEnum):
    """Canonical column names for all weather metrics (Single Source of Truth)."""

    TEMPERATURE_2M = "temperature_2m"
    RELATIVE_HUMIDITY_2M = "relative_humidity_2m"
    PRESSURE_MSL = "pressure_msl"
    WIND_SPEED_10M = "wind_speed_10m"


DIURNAL_RANGE_ALIAS: Final[str] = "diurnal_temperature_range"


@dataclass(frozen=True)
class MetricBound:
    """Physically plausible range for a weather metric, derived from WMO world records."""

    min_value: float
    max_value: float
    unit: str


# WMO-derived validation thresholds.
WMO_BOUNDS: Final[Mapping[WeatherMetric, MetricBound]] = MappingProxyType(
    {
        WeatherMetric.TEMPERATURE_2M: MetricBound(min_value=-95.0, max_value=65.0, unit="°C"),
        WeatherMetric.RELATIVE_HUMIDITY_2M: MetricBound(min_value=0.0, max_value=100.0, unit="%"),
        WeatherMetric.PRESSURE_MSL: MetricBound(min_value=850.0, max_value=1100.0, unit="hPa"),
        WeatherMetric.WIND_SPEED_10M: MetricBound(min_value=0.0, max_value=500.0, unit="km/h"),
    }
)


@dataclass(frozen=True)
class RollupProjection:
    """Specification of an in-engine SQL rollup projection."""

    expression: str
    alias: str
    unit: str


ROLLUP_PROJECTIONS: Final[tuple[RollupProjection, ...]] = (
    RollupProjection(
        expression=f"avg({WeatherMetric.TEMPERATURE_2M})",
        alias=f"avg_{WeatherMetric.TEMPERATURE_2M}",
        unit=WMO_BOUNDS[WeatherMetric.TEMPERATURE_2M].unit,
    ),
    RollupProjection(
        expression=f"min({WeatherMetric.TEMPERATURE_2M})",
        alias=f"min_{WeatherMetric.TEMPERATURE_2M}",
        unit=WMO_BOUNDS[WeatherMetric.TEMPERATURE_2M].unit,
    ),
    RollupProjection(
        expression=f"max({WeatherMetric.TEMPERATURE_2M})",
        alias=f"max_{WeatherMetric.TEMPERATURE_2M}",
        unit=WMO_BOUNDS[WeatherMetric.TEMPERATURE_2M].unit,
    ),
    RollupProjection(
        expression=f"(max({WeatherMetric.TEMPERATURE_2M}) - min({WeatherMetric.TEMPERATURE_2M}))",
        alias=DIURNAL_RANGE_ALIAS,
        unit=WMO_BOUNDS[WeatherMetric.TEMPERATURE_2M].unit,
    ),
    RollupProjection(
        expression=f"avg({WeatherMetric.RELATIVE_HUMIDITY_2M})",
        alias=f"avg_{WeatherMetric.RELATIVE_HUMIDITY_2M}",
        unit=WMO_BOUNDS[WeatherMetric.RELATIVE_HUMIDITY_2M].unit,
    ),
    RollupProjection(
        expression=f"avg({WeatherMetric.PRESSURE_MSL})",
        alias=f"avg_{WeatherMetric.PRESSURE_MSL}",
        unit=WMO_BOUNDS[WeatherMetric.PRESSURE_MSL].unit,
    ),
    RollupProjection(
        expression=f"max({WeatherMetric.WIND_SPEED_10M})",
        alias=f"max_{WeatherMetric.WIND_SPEED_10M}",
        unit=WMO_BOUNDS[WeatherMetric.WIND_SPEED_10M].unit,
    ),
)


@dataclass(frozen=True)
class AlertRule:
    """Operational threshold for predictive alerting."""

    metric: WeatherMetric
    comparator: Callable[[float, float], bool]
    threshold: float
    unit: str
    severity: Literal["warning", "critical"]
    description: str

    def is_active(self, actual_value: float) -> bool:
        """Evaluates whether the given value violates this alert rule."""

        return self.comparator(actual_value, self.threshold)


ALERT_RULES: tuple[AlertRule, ...] = (
    AlertRule(
        metric=WeatherMetric.WIND_SPEED_10M,
        comparator=operator.gt,
        threshold=60.0,
        unit="km/h",
        severity="warning",
        description="Gale-force wind warning (> 60 km/h)",
    ),
    AlertRule(
        metric=WeatherMetric.TEMPERATURE_2M,
        comparator=operator.lt,
        threshold=0.0,
        unit="°C",
        severity="warning",
        description="Frost warning (< 0 °C)",
    ),
    AlertRule(
        metric=WeatherMetric.TEMPERATURE_2M,
        comparator=operator.gt,
        threshold=38.0,
        unit="°C",
        severity="critical",
        description="Extreme heat anomaly (> 38 °C)",
    ),
)


class ActiveAlert(TypedDict):
    """Structured payload for triggered alerts."""

    timestamp: str
    metric: str
    actual_value: float
    threshold: float
    unit: str
    severity: str
    description: str


class HourlyWeatherData(BaseModel):
    """Hourly weather metrics from the Open-Meteo Archive API."""

    time: list[str] = Field(description="ISO-8601 timestamps (UTC).")
    temperature_2m: list[float] = Field(description="Air temperature in Celsius.")
    relative_humidity_2m: list[float] = Field(description="Relative humidity in percent.")
    pressure_msl: list[float] = Field(description="Atmospheric pressure at sea level in hPa.")
    wind_speed_10m: list[float] = Field(description="Wind speed in km/h.")

    @classmethod
    def metric_names(cls) -> list[str]:
        """Return all weather metric column names strictly driven by WeatherMetric SSOT."""

        return [metric.value for metric in WeatherMetric]

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


# Sanity check: Pydantic fields in HourlyWeatherData do not match WeatherMetric Enum!
assert set(HourlyWeatherData.metric_names()) == {
    f for f in HourlyWeatherData.model_fields if f != "time"
}, "Schema Drift: Pydantic fields in HourlyWeatherData do not match WeatherMetric Enum!"
