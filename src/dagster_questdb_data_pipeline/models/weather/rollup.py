"""In-engine SQL rollup projection specifications."""

from dataclasses import dataclass
from typing import Final

from dagster_questdb_data_pipeline.models.weather.constants import DIURNAL_RANGE_ALIAS
from dagster_questdb_data_pipeline.models.weather.metrics import WMO_BOUNDS, WeatherMetric


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
