CREATE TABLE IF NOT EXISTS weather_daily_rollup (
    timestamp TIMESTAMP,
    avg_temperature_2m DOUBLE,
    min_temperature_2m DOUBLE,
    max_temperature_2m DOUBLE,
    diurnal_temperature_range DOUBLE,
    avg_relative_humidity_2m DOUBLE,
    avg_pressure_msl DOUBLE,
    max_wind_speed_10m DOUBLE
) TIMESTAMP(timestamp)
PARTITION BY
    MONTH WAL DEDUP UPSERT KEYS (timestamp);