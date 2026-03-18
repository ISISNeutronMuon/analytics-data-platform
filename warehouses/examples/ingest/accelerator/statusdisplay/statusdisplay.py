"""Example extract script: generates fake sensor readings and events.

Demonstrates the extract contract expected by ``elt run``:
  - A module-level ``extract(config, catalog) -> dict[str, pa.Table]`` function.
  - Incremental loading by querying the catalog for the latest timestamp.
"""

import datetime as dt
import logging
from typing import Iterator, Tuple

import pyarrow as pa
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="statusdisplay__")

    api_base_url: str
    endpoints: list[str]


def extract(
    source_config: SourceConfig, *, backfill: bool = False
) -> Iterator[Tuple[str, pa.Table]]:
    """Pull from the statusdisplay API and returned the requested resources"""
    for endpoint in source_config.endpoints:
        url = source_config.api_base_url + endpoint
        logging.debug(f"Fetching cycle schedule from {url}")
        #        resp = httpx.get(url)
        #        resp.raise_for_status()

        yield (
            endpoint.lstrip("/"),
            pa.Table.from_pylist(
                [
                    {
                        "cycle_name": "2025/1",
                        "started_at": to_utc("2025-01-30T08:30:00"),
                        "ended_at": to_utc("2025-02-28T08:30:00"),
                    },
                    {
                        "cycle_name": "2024/1",
                        "started_at": to_utc("2024-01-30T08:30:00"),
                        "ended_at": to_utc("2024-02-28T08:30:00"),
                    },
                ]
            ),
        )


def to_utc(timestamp: str) -> dt.datetime:
    return dt.datetime.fromisoformat(timestamp).astimezone(dt.UTC)
