import dagster as dg
import pandas as pd

from dagster_questdb_data_pipeline.defs.resources import (
    QuestDbResource,
    WeatherApiResource,
)


@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01"),
    group_name="telemetry_ingest",
    compute_kind="qwp",
    description="Fetches raw weather metrics and ingests them into QuestDB.",
)
def weather_telemetry_raw(
    context: dg.AssetExecutionContext,
    weather_api: WeatherApiResource,
    questdb: QuestDbResource,
) -> dg.Output[None]:
    target_date = context.partition_time_window.start.date()

    # Fetch raw weather data from Open-Meteo
    payload = weather_api.fetch_hourly(start_date=target_date, end_date=target_date)
    context.log.info(f"Payload keys: {list(payload.keys())}")

    hourly_data = payload.get("hourly", {})
    context.log.info(f"Hourly data keys: {list(hourly_data.keys())}")

    # Convert data to Pandas DataFrame
    df = pd.DataFrame(hourly_data)
    df["timestamp"] = pd.to_datetime(df.pop("time"), utc=True)

    record_count = len(df)
    context.log.info(f"Fetched {record_count} telemetry records for {target_date} into QuestDB.")

    # Ingest data into QuestDB
    ingested_count = questdb.ingest_dataframe("weather_telemetry_raw", df)

    return dg.Output(
        value=None,
        metadata={
            "record_count": dg.MetadataValue.int(record_count),
            "ingested_count": dg.MetadataValue.int(ingested_count),
            "target_date": dg.MetadataValue.text(target_date.isoformat()),
            "preview": dg.MetadataValue.md(df.head().to_markdown(index=False)),
        },
    )
