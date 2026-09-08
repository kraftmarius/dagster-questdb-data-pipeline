"""Weather metric definitions — canonical names and WMO-derived validation bounds."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final


class WeatherMetric(StrEnum):
    """Canonical column names for all weather metrics (Single Source of Truth)."""

    TEMPERATURE_2M = "temperature_2m"
    RELATIVE_HUMIDITY_2M = "relative_humidity_2m"
    PRESSURE_MSL = "pressure_msl"
    WIND_SPEED_10M = "wind_speed_10m"


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
