import dagster as dg
import httpx2
from pydantic import Field

from dagster_questdb_data_pipeline.models.weather import WeatherAlertPayload


class WebhookResource(dg.ConfigurableResource):
    """Resource for dispatching alert notifications to external HTTP webhooks."""

    url: str = Field(
        default="",
        description="Target webhook URL. If omitted or empty, dispatching is disabled.",
    )
    timeout_seconds: float = Field(
        default=10.0,
        description="HTTP request timeout in seconds.",
    )

    @property
    def is_enabled(self) -> bool:
        """Returns True if a non-empty webhook URL is configured."""

        return bool(self.url and self.url.strip())

    def post_alert(self, payload: WeatherAlertPayload) -> None:
        """Dispatches payload to the webhook endpoint. Returns False if disabled."""

        if not self.is_enabled:
            return

        with httpx2.Client(timeout=self.timeout_seconds) as client:
            response = client.post(self.url, json=payload.model_dump(mode="json"))
            response.raise_for_status()
