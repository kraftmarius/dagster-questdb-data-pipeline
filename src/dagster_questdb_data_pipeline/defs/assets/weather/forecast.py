from datetime import UTC

import dagster as dg
import pandas as pd

from dagster_questdb_data_pipeline.defs.resources.questdb import QuestDbResource
from dagster_questdb_data_pipeline.defs.resources.weather_api import WeatherApiResource
from dagster_questdb_data_pipeline.models.weather import (
    WEATHER_FORECAST_TABLE,
    WEATHER_GROUP_NAME,
    WMO_BOUNDS,
)


@dg.asset(
    name=WEATHER_FORECAST_TABLE,
    description="Ingests rolling hourly weather forecast horizon (+24 hours) into QuestDB.",
    group_name=WEATHER_GROUP_NAME,
    kinds={"questdb"},
    automation_condition=dg.AutomationCondition.on_cron("@hourly"),
)
def forecast(
    weather_api: WeatherApiResource,
    questdb: QuestDbResource,
) -> dg.Output[None]:
    response = weather_api.fetch_forecast_hourly()

    df = response.hourly.to_dataframe()
    df["generated_at"] = pd.Timestamp.now(UTC)

    row_count = questdb.ingest_dataframe(WEATHER_FORECAST_TABLE, df)

    return dg.Output(
        value=None,
        metadata={
            "table": dg.MetadataValue.text(WEATHER_FORECAST_TABLE),
            "dagster/row_count": dg.MetadataValue.int(row_count),
        },
    )


@dg.asset_check(
    name="integrity_check",
    description="Validates that QuestDB forecast horizon has sufficient rows and plausible metrics against WMO standards.",
    asset=WEATHER_FORECAST_TABLE,
    blocking=True,
)
def forecast_integrity_check(
    questdb: QuestDbResource,
) -> dg.AssetCheckResult:
    # Dynamically compose SQL aggregations strictly from immutable domain model
    metric_aggregations = ",\n            ".join(
        f"min({m}) as min_{m}, max({m}) as max_{m}" for m in WMO_BOUNDS
    )

    sql = f"""
        SELECT
            count() as row_count,
            {metric_aggregations}
        FROM {WEATHER_FORECAST_TABLE};
    """

    with questdb.connect() as db, db.query(sql) as result:
        df = result.to_pandas()

    if df.empty or int(df.iloc[0]["row_count"]) == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "error": dg.MetadataValue.text("Zero rows found in forecast table in QuestDB."),
            },
        )

    row = df.iloc[0]
    row_count = int(row["row_count"])

    violations: list[str] = []
    metadata: dict[str, dg.MetadataValue] = {
        "dagster/row_count": dg.MetadataValue.int(row_count),
    }

    if row_count < 24:
        violations.append(f"Incomplete forecast horizon: expected >= 24 rows, got {row_count}.")

    for metric, bounds in WMO_BOUNDS.items():
        min_col = f"min_{metric}"
        max_col = f"max_{metric}"

        actual_min = float(row[min_col])
        actual_max = float(row[max_col])

        metadata[f"{metric}_range"] = dg.MetadataValue.text(
            f"[{actual_min:.1f}, {actual_max:.1f}] {bounds.unit}"
        )

        if actual_min < bounds.min_value or actual_max > bounds.max_value:
            violations.append(
                f"Metric '{metric}' violates WMO limits [{bounds.min_value}, {bounds.max_value}] {bounds.unit}: "
                f"actual range [{actual_min:.1f}, {actual_max:.1f}]"
            )

    passed = len(violations) == 0
    metadata["violations"] = (
        dg.MetadataValue.json(violations) if violations else dg.MetadataValue.text("None")
    )

    return dg.AssetCheckResult(
        passed=passed,
        metadata=metadata,
    )
