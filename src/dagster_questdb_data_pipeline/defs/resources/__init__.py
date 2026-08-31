import dagster as dg

from dagster_questdb_data_pipeline.defs.resources.questdb import QuestDbResource
from dagster_questdb_data_pipeline.defs.resources.weather_api import WeatherApiResource


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "weather_api": WeatherApiResource(
                default_latitude=dg.EnvVar("LATITUDE").get_value(),
                default_longitude=dg.EnvVar("LONGITUDE").get_value(),
            ),
            "questdb": QuestDbResource(
                host=dg.EnvVar("QDB_HOST").get_value(),
                port=dg.EnvVar().int("QDB_PORT"),
                username=dg.EnvVar("QDB_PG_USER").get_value(),
                password=dg.EnvVar("QDB_PG_PASSWORD").get_value(),
            ),
        },
    )
