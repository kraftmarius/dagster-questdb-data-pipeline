import dagster as dg

from dagster_questdb_data_pipeline.defs.resources.questdb import QuestDbResource
from dagster_questdb_data_pipeline.models.weather import (
    ROLLUP_PROJECTIONS,
    WEATHER_DAILY_ROLLUP_TABLE,
    WEATHER_GROUP_NAME,
    WEATHER_RAW_TABLE,
    WMO_BOUNDS,
)


@dg.asset(
    name=WEATHER_DAILY_ROLLUP_TABLE,
    description="Computes in-engine daily downsampled weather statistics from raw telemetry.",
    group_name=WEATHER_GROUP_NAME,
    kinds={"questdb", "sql"},
    deps={WEATHER_RAW_TABLE},
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01", timezone="UTC"),
    automation_condition=dg.AutomationCondition.eager(),
)
def daily_rollup(
    context: dg.AssetExecutionContext,
    questdb: QuestDbResource,
) -> dg.Output[None]:
    target_date = context.partition_time_window.start.date()

    # Dynamically compose in-engine SQL projections strictly from immutable domain model
    projections_sql = ",\n            ".join(
        f"{proj.expression} AS {proj.alias}" for proj in ROLLUP_PROJECTIONS
    )

    # In-Engine aggregation: QuestDB processes and downsamples the 24 hours directly
    sql = f"""
        INSERT INTO {WEATHER_DAILY_ROLLUP_TABLE}
        SELECT
            timestamp,
            {projections_sql}
        FROM {WEATHER_RAW_TABLE}
        WHERE timestamp IN $1
        SAMPLE BY 1d ALIGN TO CALENDAR;
    """

    with questdb.connect() as db:
        db.query(sql, [target_date.isoformat()])

    context.log.info(f"Successfully computed daily rollup for {target_date}.")

    return dg.Output(
        value=None,
        metadata={
            "table": dg.MetadataValue.text(WEATHER_DAILY_ROLLUP_TABLE),
            "target_date": dg.MetadataValue.text(target_date.isoformat()),
            "dagster/row_count": dg.MetadataValue.int(1),
        },
    )


@dg.asset_check(
    name="integrity_check",
    description="Verifies mathematical consistency of daily rollup aggregations against WMO bounds.",
    asset=WEATHER_DAILY_ROLLUP_TABLE,
    blocking=True,
)
def daily_rollup_integrity_check(
    context: dg.AssetCheckExecutionContext,
    questdb: QuestDbResource,
) -> dg.AssetCheckResult:
    target_date = context.partition_time_window.start.date()

    columns_sql = ",\n            ".join(proj.alias for proj in ROLLUP_PROJECTIONS)

    sql = f"""
        SELECT
            count() as row_count,
            {columns_sql}
        FROM {WEATHER_DAILY_ROLLUP_TABLE}
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
                "error": dg.MetadataValue.text("Zero rollup records found in QuestDB."),
            },
        )

    row = df.iloc[0]
    row_count = int(row["row_count"])

    violations: list[str] = []
    metadata: dict[str, dg.MetadataValue] = {
        "target_date": dg.MetadataValue.text(target_date.isoformat()),
        "dagster/row_count": dg.MetadataValue.int(row_count),
    }

    if row_count != 1:
        violations.append(f"Incomplete rollup: expected 1 row, got {row_count}.")

    t_min = float(row["min_temperature_2m"])
    t_avg = float(row["avg_temperature_2m"])
    t_max = float(row["max_temperature_2m"])
    d_range = float(row["diurnal_temperature_range"])
    avg_hum = float(row["avg_relative_humidity_2m"])

    if not (t_min <= t_avg <= t_max):
        violations.append(
            f"Mathematical violation: !(min <= avg <= max) -> !({t_min:.1f} <= {t_avg:.1f} <= {t_max:.1f})"
        )

    expected_range = round(t_max - t_min, 2)
    if round(d_range, 2) != expected_range or d_range < 0.0:
        violations.append(
            f"Diurnal range mismatch: expected {expected_range:.2f}, got {d_range:.2f}"
        )

    for proj in ROLLUP_PROJECTIONS:
        val = float(row[proj.alias])
        metadata[proj.alias] = dg.MetadataValue.text(f"{val:.2f} {proj.unit}")

    temp_bound = WMO_BOUNDS["temperature_2m"]
    if not (temp_bound.min_value <= t_min and t_max <= temp_bound.max_value):
        violations.append(
            f"Daily temperature extremes exceed WMO bounds [{temp_bound.min_value}, {temp_bound.max_value}] °C: "
            f"[{t_min:.1f}, {t_max:.1f}]"
        )

    hum_bound = WMO_BOUNDS["relative_humidity_2m"]
    if not (hum_bound.min_value <= avg_hum <= hum_bound.max_value):
        violations.append(
            f"Average humidity exceeds WMO bounds [{hum_bound.min_value}, {hum_bound.max_value}] %: {avg_hum:.1f}"
        )

    passed = len(violations) == 0
    metadata["violations"] = (
        dg.MetadataValue.json(violations) if violations else dg.MetadataValue.text("None")
    )

    return dg.AssetCheckResult(
        passed=passed,
        metadata=metadata,
    )
