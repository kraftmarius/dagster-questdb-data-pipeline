from typing import Self

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator


class HourlyWeatherData(BaseModel):
    time: list[str] = Field(description="ISO-8601 timestamps (UTC).")
    temperature_2m: list[float] = Field(description="Air temperature in Celsius.")
    relative_humidity_2m: list[float] = Field(description="Relative humidity in percent.")
    pressure_msl: list[float] = Field(description="Atmospheric pressure at sea level in hPa.")
    wind_speed_10m: list[float] = Field(description="Wind speed in km/h.")

    @model_validator(mode="after")
    def validate_column_lengths_and_ranges(self) -> Self:
        expected_len = len(self.time)

        for field_name in (
            "temperature_2m",
            "relative_humidity_2m",
            "pressure_msl",
            "wind_speed_10m",
        ):
            actual_len = len(getattr(self, field_name))

            if actual_len != expected_len:
                raise ValueError(
                    f"Column '{field_name}' length ({actual_len}) does not match expected length ({expected_len})."
                )

        # Validation thresholds are derived from the World Meteorological Organization (WMO)
        # World Weather & Climate Extremes Archive (https://wmo.int/files/records-of-weather-and-climate-extremes-table)
        for temp in self.temperature_2m:
            if not (-95.0 <= temp <= 65.0):
                raise ValueError(f"Temperature out of bounds [-95, +65] °C: {temp}")

        for humidity in self.relative_humidity_2m:
            if not (0.0 <= humidity <= 100.0):
                raise ValueError(f"Relative humidity out of bounds [0, 100] %: {humidity}")

        for pressure in self.pressure_msl:
            if not (850.0 <= pressure <= 1100.0):
                raise ValueError(f"Sea level pressure out of bounds [850, 1100] hPa: {pressure}")

        for wind in self.wind_speed_10m:
            if not (0.0 <= wind <= 500.0):
                raise ValueError(f"Wind speed out of bounds [0, 500] km/h: {wind}")

        return self

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.model_dump())
        df["timestamp"] = pd.to_datetime(df.pop("time"), utc=True)

        return df


class OpenMeteoResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    hourly: HourlyWeatherData
