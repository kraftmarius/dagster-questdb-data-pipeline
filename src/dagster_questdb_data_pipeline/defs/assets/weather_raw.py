import dagster as dg

from dagster_questdb_data_pipeline.defs.resources.questdb import QuestDbResource
from dagster_questdb_data_pipeline.defs.resources.weather_api import WeatherApiResource
from dagster_questdb_data_pipeline.models.weather import (
    WEATHER_RAW_TABLE,
    WMO_BOUNDS,
)


@dg.asset(
    name=WEATHER_RAW_TABLE,
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

    response = weather_api.fetch_hourly(start_date=target_date, end_date=target_date)
    df = response.hourly.to_dataframe()

    row_count = questdb.ingest_dataframe(WEATHER_RAW_TABLE, df)

    return dg.Output(
        value=None,
        metadata={
            "table": dg.MetadataValue.text(WEATHER_RAW_TABLE),
            "row_count": dg.MetadataValue.int(row_count),
            "target_date": dg.MetadataValue.text(target_date.isoformat()),
        },
    )


@dg.asset_check(
    name="integrity_check",
    description="Validates that QuestDB partition has exactly 24 hourly rows and plausible metrics against WMO standards.",
    asset=WEATHER_RAW_TABLE,
    blocking=True,
)
def raw_integrity_check(
    context: dg.AssetCheckExecutionContext,
    questdb: QuestDbResource,
) -> dg.AssetCheckResult:
    target_date = context.partition_time_window.start.date()

    # Dynamically compose SQL aggregations strictly from immutable domain whitelist
    metric_aggregations = ",\n            ".join(
        f"min({m}) as min_{m}, max({m}) as max_{m}" for m in WMO_BOUNDS
    )

    sql = f"""
        SELECT
            count() as row_count,
            {metric_aggregations}
        FROM {WEATHER_RAW_TABLE}
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

    violations: list[str] = []
    metadata_ranges: dict[str, dg.MetadataValue] = {
        "target_date": dg.MetadataValue.text(target_date.isoformat()),
        "row_count": dg.MetadataValue.int(row_count),
    }

    if row_count != 24:
        violations.append(f"Incomplete partition: expected 24 rows, got {row_count}.")

    for metric, bounds in WMO_BOUNDS.items():
        min_col = f"min_{metric}"
        max_col = f"max_{metric}"

        actual_min = float(row[min_col])
        actual_max = float(row[max_col])

        metadata_ranges[f"{metric}_range"] = dg.MetadataValue.text(
            f"[{actual_min:.1f}, {actual_max:.1f}] {bounds.unit}"
        )

        if actual_min < bounds.min_value or actual_max > bounds.max_value:
            violations.append(
                f"Metric '{metric}' violates WMO limits [{bounds.min_value}, {bounds.max_value}] {bounds.unit}: "
                f"actual range [{actual_min:.1f}, {actual_max:.1f}]"
            )

    passed = len(violations) == 0
    metadata_ranges["violations"] = (
        dg.MetadataValue.json(violations) if violations else dg.MetadataValue.text("None")
    )

    return dg.AssetCheckResult(
        passed=passed,
        metadata=metadata_ranges,
    )
