import dagster as dg

from dagster_questdb_data_pipeline.defs.resources.webhook import WebhookResource
from dagster_questdb_data_pipeline.models.weather import (
    WEATHER_GROUP_NAME,
    WeatherAlertPayload,
)


@dg.asset_sensor(
    name="weather_alert_notifier_sensor",
    asset_key=dg.AssetKey("weather_forecast_alerts"),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def alert_notifier_sensor(
    context: dg.SensorEvaluationContext,
    asset_event: dg.EventLogEntry,
    webhook: WebhookResource,
) -> dg.SkipReason | None:
    if not webhook.is_enabled:
        return dg.SkipReason("Webhook dispatching is disabled (no WEBHOOK_URL configured).")

    dagster_event = asset_event.dagster_event
    if not dagster_event or not dagster_event.asset_materialization:
        return dg.SkipReason("Event does not contain an asset materialization.")

    metadata = dagster_event.materialization.metadata

    status_entry = metadata.get("status")
    if not status_entry or status_entry.value != "ALERTS_ACTIVE":
        return dg.SkipReason("Weather conditions nominal. No alert notification needed.")

    payload_entry = metadata.get("payload")
    if not payload_entry or not isinstance(payload_entry.value, dict):
        return dg.SkipReason("Missing or invalid alert payload in metadata.")

    payload = WeatherAlertPayload.model_validate(payload_entry.value)

    context.log.warning(f"Dispatching {payload.alerts_count} active alert(s) to webhook...")
    webhook.post_alert(payload)
    context.log.info("Alert notification successfully sent.")

    return None


@dg.definitions
def sensors() -> dg.Definitions:
    return dg.Definitions(
        sensors=[
            dg.AutomationConditionSensorDefinition(
                "weather_automation_sensor",
                target=dg.AssetSelection.groups(WEATHER_GROUP_NAME),
                default_status=dg.DefaultSensorStatus.RUNNING,
            ),
            alert_notifier_sensor,
        ],
    )
