import dagster as dg

from dagster_questdb_data_pipeline.models.weather import WEATHER_GROUP_NAME


@dg.definitions
def sensors():
    return dg.Definitions(
        sensors=[
            dg.AutomationConditionSensorDefinition(
                "weather_automation_sensor",
                target=dg.AssetSelection.groups(WEATHER_GROUP_NAME),
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
        ],
    )
