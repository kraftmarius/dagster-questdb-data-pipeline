"""Operational alert rules and notification payloads."""

import operator
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal, TypedDict

from pydantic import BaseModel, Field

from dagster_questdb_data_pipeline.models.weather.metrics import WeatherMetric


@dataclass(frozen=True)
class AlertRule:
    """Operational threshold for predictive alerting."""

    metric: WeatherMetric
    comparator: Callable[[float, float], bool]
    threshold: float
    unit: str
    severity: Literal["warning", "critical"]
    description: str

    def is_active(self, actual_value: float) -> bool:
        """Evaluates whether the given value violates this alert rule."""

        return self.comparator(actual_value, self.threshold)


ALERT_RULES: Final[tuple[AlertRule, ...]] = (
    AlertRule(
        metric=WeatherMetric.WIND_SPEED_10M,
        comparator=operator.gt,
        threshold=60.0,
        unit="km/h",
        severity="warning",
        description="Gale-force wind warning (> 60 km/h)",
    ),
    AlertRule(
        metric=WeatherMetric.TEMPERATURE_2M,
        comparator=operator.lt,
        threshold=0.0,
        unit="°C",
        severity="warning",
        description="Frost warning (< 0 °C)",
    ),
    AlertRule(
        metric=WeatherMetric.TEMPERATURE_2M,
        comparator=operator.gt,
        threshold=38.0,
        unit="°C",
        severity="critical",
        description="Extreme heat anomaly (> 38 °C)",
    ),
)


class ActiveAlert(TypedDict):
    """Structured payload for triggered alerts."""

    timestamp: str
    metric: str
    actual_value: float
    threshold: float
    unit: str
    severity: str
    description: str


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
