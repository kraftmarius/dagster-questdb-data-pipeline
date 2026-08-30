import dagster as dg

from dagster_questdb_boilerplate.defs.resources import WeatherApiResource


@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01"),
    group_name="telemetry_ingest",
    description="Fetches raw weather metrics",
)
def weather_telemetry_raw(
    context: dg.AssetExecutionContext, weather_api: WeatherApiResource
) -> dg.Output[None]:
    start_date = context.partition_time_window.start.date()
    end_date = context.partition_time_window.end.date()

    payload = weather_api.fetch_hourly(start_date=start_date, end_date=end_date)

    hourly_data = payload.get("hourly", {})
    timestamps = hourly_data.get("time", [])

    record_count = len(timestamps)

    context.log.info(f"Fetched {record_count} telemetry records for {start_date} to {end_date}.")

    return dg.Output(
        value=None,
        metadata={
            "record_count": dg.MetadataValue.int(record_count),
            "start_date": dg.MetadataValue.text(start_date.isoformat()),
            "end_date": dg.MetadataValue.text(end_date.isoformat()),
        },
    )
