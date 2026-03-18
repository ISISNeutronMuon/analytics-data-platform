"""Example extract script: generates fake sensor readings and events.

Demonstrates the extract contract expected by ``elt run``:
  - A module-level ``extract(config, catalog) -> dict[str, pa.Table]`` function.
  - Incremental loading by querying the catalog for the latest timestamp.
"""

import datetime
import random
import logging
from typing import Iterator

import httpx
import pyarrow as pa
import pyarrow.compute as pc
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError


class SourceConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="statusdisplay_")

    api_base_url: str


def extract(source_config: SourceConfig, *, backfill: bool = False) -> Iterator:
    """Pull from the statusdisplay API and"""
    # resp = httpx.get(source_config.api_url)
    # resp.raise_for_status()
    logging.debug(f"Fetching cycle schedule from {source_config.api_base_url}")

    yield (
        "schedule",
        pa.Table.from_pylist(
            [
                {
                    "cycle_name": "2025/1",
                    "started_at": "2025-01-30T08:30:00",
                    "ended_at": "2025-02-30T08:30:00",
                },
                {
                    "cycle_name": "2024/1",
                    "started_at": "2024-01-30T08:30:00",
                    "ended_at": "2024-02-30T08:30:00",
                },
            ]
        ),
    )
