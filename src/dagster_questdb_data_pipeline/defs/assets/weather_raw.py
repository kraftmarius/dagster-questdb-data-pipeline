import dagster as dg

from dagster_questdb_data_pipeline.defs.resources.questdb import QuestDbResource
from dagster_questdb_data_pipeline.defs.resources.weather_api import WeatherApiResource


@dg.asset(
    name="weather_raw",
    description="Fetches raw weather metrics and ingests them into QuestDB.",
    kinds={"questdb"},
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01"),
)
def raw(
    context: dg.AssetExecutionContext,
    weather_api: WeatherApiResource,
    questdb: QuestDbResource,
) -> dg.Output[None]:
    target_date = context.partition_time_window.start.date()

    # Fetch raw weather data from Open-Meteo
    response = weather_api.fetch_hourly(start_date=target_date, end_date=target_date)

    # Convert data to Pandas DataFrame
    df = response.hourly.to_dataframe()

    # Ingest data into QuestDB
    row_count = questdb.ingest_dataframe("weather_raw", df)

    return dg.Output(
        value=None,
        metadata={
            "table": dg.MetadataValue.text("weather_raw"),
            "row_count": dg.MetadataValue.int(row_count),
            "target_date": dg.MetadataValue.text(target_date.isoformat()),
        },
    )


@dg.asset_check(
    name="weather_raw_integrity_check",
    description="Validates that QuestDB partition has exactly 24 hourly rows and plausible metrics against WMO standards.",
    asset="weather_raw",
    blocking=True,
)
def raw_integrity_check(
    context: dg.AssetCheckExecutionContext,
    questdb: QuestDbResource,
) -> dg.AssetCheckResult:
    """Validate persisted hourly weather records in QuestDB.

    Validation thresholds are derived from the World Meteorological Organization (WMO)
    World Weather & Climate Extremes Archive (https://wmo.int/files/records-of-weather-and-climate-extremes-table):
      - Temperature: [-95.0, +65.0] °C (Records: -89.2°C at Vostok, +56.7°C at Death Valley).
      - Sea Level Pressure: [850.0, 1100.0] hPa (Records: 870.0 hPa at Typhoon Tip, 1084.8 hPa at Tosontsengel).
      - Wind Speed: [0.0, 500.0] km/h (Record non-tornadic gust: 408 km/h at Barrow Island).
      - Relative Humidity: [0.0, 100.0] % (Thermodynamic physical limits).

    :param context: Execution context containing partition time window.
    :param questdb: QuestDB resource client.
    :return: AssetCheckResult indicating verification status and quality metrics.
    """

    target_date = context.partition_time_window.start.date()

    sql = """
        SELECT
            count() as row_count,
            min(temperature_2m) as min_temp,
            max(temperature_2m) as max_temp,
            min(relative_humidity_2m) as min_hum,
            max(relative_humidity_2m) as max_hum,
            min(wind_speed_10m) as min_wind,
            max(wind_speed_10m) as max_wind,
            min(pressure_msl) as min_pressure,
            max(pressure_msl) as max_pressure
        FROM weather_raw
        WHERE timestamp IN $1;
    """

    with questdb.connect() as db, db.query(sql, [target_date.isoformat()]) as result:
        df = result.to_pandas()

    if df.empty or int(df.iloc[0]["row_count"]) == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "target_date": dg.MetadataValue.text(target_date.isoformat()),
                "error": dg.MetadataValue.text("Zero rows found for partition in QuestDB."),
            },
        )

    row = df.iloc[0]
    row_count = int(row["row_count"])
    min_temp, max_temp = float(row["min_temp"]), float(row["max_temp"])
    min_hum, max_hum = float(row["min_hum"]), float(row["max_hum"])
    min_wind, max_wind = float(row["min_wind"]), float(row["max_wind"])
    min_press, max_press = float(row["min_pressure"]), float(row["max_pressure"])

    violations: list[str] = []

    if row_count != 24:
        violations.append(f"Incomplete partition: expected 24 rows, got {row_count}.")
    if not (-95.0 <= min_temp and max_temp <= 65.0):
        violations.append(
            f"Temperature violates terrestrial limits [-95, +65] °C: [{min_temp}, {max_temp}]"
        )
    if not (0.0 <= min_hum and max_hum <= 100.0):
        violations.append(f"Humidity violates limits [0, 100] %: [{min_hum}, {max_hum}]")
    if min_wind < 0.0 or max_wind > 500.0:
        violations.append(f"Wind speed violates limits [0, 500] km/h: [{min_wind}, {max_wind}]")
    if not (850.0 <= min_press and max_press <= 1100.0):
        violations.append(
            f"Mean sea level pressure violates limits [850, 1100] hPa: [{min_press:.1f}, {max_press:.1f}]"
        )

    passed = len(violations) == 0

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "target_date": dg.MetadataValue.text(target_date.isoformat()),
            "row_count": dg.MetadataValue.int(row_count),
            "temp_range_celsius": dg.MetadataValue.text(f"[{min_temp:.1f}, {max_temp:.1f}]"),
            "humidity_range_pct": dg.MetadataValue.text(f"[{min_hum:.1f}, {max_hum:.1f}]"),
            "wind_range_kmh": dg.MetadataValue.text(f"[{min_wind:.1f}, {max_wind:.1f}]"),
            "pressure_range_hpa": dg.MetadataValue.text(f"[{min_press:.1f}, {max_press:.1f}]"),
            "violations": dg.MetadataValue.json(violations)
            if violations
            else dg.MetadataValue.text("None"),
        },
    )
