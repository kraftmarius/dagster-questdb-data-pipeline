from datetime import UTC, datetime

from pydantic import BaseModel, Field

from dagster_questdb_data_pipeline.models.weather import ActiveAlert


class WeatherAlertPayload(BaseModel):
    """Structured payload dispatched to notification webhooks."""

    event: str = Field(
        default="weather_alert",
        description="Event discriminator.",
    )
    dispatched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the notification was dispatched.",
    )
    alerts_count: int = Field(
        description="Total number of active alerts.",
    )
    alerts: list[ActiveAlert] = Field(
        default_factory=list,
        description="List of triggered active alerts.",
    )
