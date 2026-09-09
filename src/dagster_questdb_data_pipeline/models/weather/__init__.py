"""Weather domain models — re-exports for backward-compatible import paths."""

from dagster_questdb_data_pipeline.models.weather.alerts import (
    ALERT_RULES,
    ActiveAlert,
    AlertRule,
    WeatherAlertPayload,
)
from dagster_questdb_data_pipeline.models.weather.constants import (
    DIURNAL_RANGE_ALIAS,
    WEATHER_DAILY_ROLLUP_TABLE,
    WEATHER_FORECAST_TABLE,
    WEATHER_GROUP_NAME,
    WEATHER_RAW_TABLE,
)
from dagster_questdb_data_pipeline.models.weather.metrics import (
    WMO_BOUNDS,
    MetricBound,
    WeatherMetric,
)
from dagster_questdb_data_pipeline.models.weather.open_meteo import (
    HourlyWeatherData,
    OpenMeteoResponse,
)
from dagster_questdb_data_pipeline.models.weather.rollup import (
    ROLLUP_PROJECTIONS,
    RollupProjection,
)

__all__ = [
    "ALERT_RULES",
    "DIURNAL_RANGE_ALIAS",
    "ROLLUP_PROJECTIONS",
    "WEATHER_DAILY_ROLLUP_TABLE",
    "WEATHER_FORECAST_TABLE",
    "WEATHER_GROUP_NAME",
    "WEATHER_RAW_TABLE",
    "WMO_BOUNDS",
    "ActiveAlert",
    "AlertRule",
    "HourlyWeatherData",
    "MetricBound",
    "OpenMeteoResponse",
    "RollupProjection",
    "WeatherAlertPayload",
    "WeatherMetric",
]
