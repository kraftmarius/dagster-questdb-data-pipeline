"""Weather domain constants — table names, group name, and aliases."""

from typing import Final

WEATHER_GROUP_NAME: Final[str] = "weather"
WEATHER_RAW_TABLE: Final[str] = "weather_raw"
WEATHER_DAILY_ROLLUP_TABLE: Final[str] = "weather_daily_rollup"
WEATHER_FORECAST_TABLE: Final[str] = "weather_forecast"

DIURNAL_RANGE_ALIAS: Final[str] = "diurnal_temperature_range"
