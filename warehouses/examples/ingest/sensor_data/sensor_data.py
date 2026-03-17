"""Example extract script: generates fake sensor readings and events.

Demonstrates the extract contract expected by ``elt run``:
  - A module-level ``extract(config, catalog) -> dict[str, pa.Table]`` function.
  - Incremental loading by querying the catalog for the latest timestamp.
"""

import datetime
import random

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

SENSOR_IDS = ["sensor-A", "sensor-B", "sensor-C"]
EVENT_TYPES = ["calibration", "maintenance", "alert", "restart"]


def extract(config: dict, catalog: Catalog) -> dict[str, pa.Table]:
    """Generate fake sensor readings and events.

    :param config: The ``[source.*]`` sections from ``elt.toml`` (plus ``backfill``).
    :param catalog: A connected pyiceberg ``Catalog`` for reading existing state.
    :returns: A mapping of table name to Arrow table.
    """
    backfill = config.get("backfill", False)
    namespace = "examples_sensor_data"

    readings = _generate_readings(catalog, namespace, backfill=backfill)
    events = _generate_events()

    return {
        "readings": readings,
        "events": events,
    }


def _generate_readings(
    catalog: Catalog,
    namespace: str,
    *,
    backfill: bool,
    num_rows: int = 100,
) -> pa.Table:
    """Generate fake temperature/humidity readings.

    If not backfilling, queries the catalog for the latest timestamp and
    generates data after that point.
    """
    start = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    if not backfill:
        latest = _get_latest_timestamp(catalog, namespace, "readings", "timestamp")
        if latest is not None:
            start = latest + datetime.timedelta(hours=1)

    rng = random.Random(42)
    timestamps, sensor_ids, temperatures, humidities = [], [], [], []

    for i in range(num_rows):
        ts = start + datetime.timedelta(hours=i)
        sensor = SENSOR_IDS[i % len(SENSOR_IDS)]
        timestamps.append(ts)
        sensor_ids.append(sensor)
        temperatures.append(round(18.0 + rng.gauss(0, 3), 2))
        humidities.append(round(50.0 + rng.gauss(0, 10), 2))

    return pa.table(
        {
            "sensor_id": pa.array(sensor_ids, type=pa.string()),
            "timestamp": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
            "temperature": pa.array(temperatures, type=pa.float64()),
            "humidity": pa.array(humidities, type=pa.float64()),
        }
    )


def _generate_events(num_rows: int = 20) -> pa.Table:
    """Generate fake operational events (append-only)."""
    rng = random.Random(99)
    base = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)

    event_ids, event_types, occurred_ats, descriptions = [], [], [], []
    for i in range(num_rows):
        event_ids.append(1000 + i)
        etype = rng.choice(EVENT_TYPES)
        event_types.append(etype)
        occurred_ats.append(base + datetime.timedelta(days=i, hours=rng.randint(0, 23)))
        descriptions.append(f"{etype.capitalize()} event for {rng.choice(SENSOR_IDS)}")

    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int64()),
            "event_type": pa.array(event_types, type=pa.string()),
            "occurred_at": pa.array(occurred_ats, type=pa.timestamp("us", tz="UTC")),
            "description": pa.array(descriptions, type=pa.string()),
        }
    )


def _get_latest_timestamp(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    column: str,
) -> datetime.datetime | None:
    """Query an existing Iceberg table for the max value of a timestamp column."""
    try:
        table = catalog.load_table((namespace, table_name))
    except (NoSuchTableError, Exception):
        return None

    running_max = None
    for batch in table.scan(selected_fields=(column,)).to_arrow_batch_reader():
        batch_max = pc.max(batch[column])
        if batch_max.is_valid:
            running_max = (
                batch_max if running_max is None else pc.max([running_max, batch_max])
            )

    return running_max.as_py() if running_max is not None else None
