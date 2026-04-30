import json
import logging
import os
from pathlib import Path

from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv

from data import generate_uber_ride_confirmation

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
LOGGER = logging.getLogger(__name__)


def inject_environment():
    """Load project .env values into this process."""
    load_dotenv(dotenv_path=ENV_FILE, override=False)


def _first_env(*names):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_event_hub_config():
    inject_environment()
    connection_string = _first_env(
        "CONNECTION_STRING",
        "EVENT_HUB_CONNECTION_STRING",
        "AZURE_EVENT_HUB_CONNECTION_STRING",
    )
    event_hub_name = _first_env(
        "EVENT_HUBNAME",
        "EVENT_HUB_NAME",
        "EVENTHUB_NAME",
        "AZURE_EVENT_HUB_NAME",
    )
    missing = [
        name
        for name, value in {
            "CONNECTION_STRING": connection_string,
            "EVENT_HUBNAME": event_hub_name,
        }.items()
        if not value
    ]

    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    return connection_string, event_hub_name


def get_event_hub_status():
    """Return non-secret Event Hub configuration details for the UI."""
    inject_environment()
    connection_string = _first_env(
        "CONNECTION_STRING",
        "EVENT_HUB_CONNECTION_STRING",
        "AZURE_EVENT_HUB_CONNECTION_STRING",
    )
    event_hub_name = _first_env(
        "EVENT_HUBNAME",
        "EVENT_HUB_NAME",
        "EVENTHUB_NAME",
        "AZURE_EVENT_HUB_NAME",
    )

    return {
        "configured": bool(connection_string and event_hub_name),
        "event_hub_name": event_hub_name or "Not configured",
        "connection_string_loaded": bool(connection_string),
    }


def send_to_event_hub(ride_data=None):
    """Send one ride confirmation to Azure Event Hubs."""
    producer = None
    try:
        if ride_data is None:
            ride_data = generate_uber_ride_confirmation()

        connection_string, event_hub_name = _get_event_hub_config()
        producer = EventHubProducerClient.from_connection_string(
            connection_string,
            eventhub_name=event_hub_name,
        )
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(ride_data, default=str)))
        producer.send_batch(event_batch)
        return True
    except Exception:
        LOGGER.exception("Error sending ride confirmation to Event Hub")
        return False
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    print("=" * 80)
    print("SINGLE RIDE CONFIRMATION")
    print("=" * 80)
    ride = generate_uber_ride_confirmation()
    print(json.dumps(ride, indent=2))

    print("\n" + "=" * 80)
    print("SENDING SINGLE RIDE TO EVENT HUB")
    result = send_to_event_hub(ride)
    print(f"Single ride sent to Event Hub: {result}")
