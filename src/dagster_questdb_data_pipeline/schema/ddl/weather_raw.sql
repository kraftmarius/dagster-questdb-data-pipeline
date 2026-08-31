CREATE TABLE IF NOT EXISTS weather_raw (
    timestamp TIMESTAMP,
    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,
    pressure_msl DOUBLE,
    wind_speed_10m DOUBLE
) TIMESTAMP(timestamp)
PARTITION BY
    DAY WAL DEDUP UPSERT KEYS (timestamp);