import dataclasses as dc
from collections import namedtuple
from pathlib import Path
from typing import Literal, Iterator, Sequence, Tuple

import pyarrow as pa

DataChunk = Tuple[str, pa.Table]
"""Map a string name to a chunk of data to be loaded into the named table."""

DataItems = Iterator[DataChunk]
"""A collection of DataChunks encapsulating all of the data to load for a given extract call."""

PartitionHint = dict[str, str]
"""Define the configuration of a Table partition where a key represents a column and the mapped
value defines an Iceberg transformation.
"""

SortOrderHint = dict[str, str]
"""Define the sort order on the columns in the Iceberg table.
"""

WriteMode = Literal["append", "merge", "replace"]
"""Catalog write modes.

- Append: Append data to existing records
- Merge: Upsert data into existing records, updating values of any records that exist
- Replace: Before loading, drop the data in the destination then append the new records.
"""

TableItems = dict[str, "TableProperties"]
"""A collection of table properties"""


@dc.dataclass(frozen=True)
class JobManifest:
    """Parsed representation of an ``elt.toml`` file."""

    name: str
    domain: str
    warehouse: str
    job_dir: Path = dc.field(default=Path("."))
    transform: "TransformProperties | None" = None

    @property
    def namespace(self) -> str:
        """The Iceberg namespace for this job: ``{domain}_{name}``."""
        return f"{self.domain}_{self.name}"


@dc.dataclass(frozen=True)
class TableProperties:
    """Configuration for a single table within a job."""

    name: str
    write_mode: WriteMode = "append"
    merge_on: Sequence[str] = ()
    partition: PartitionHint = dc.field(default_factory=dict)
    sort_order: SortOrderHint = dc.field(default_factory=dict)

    watermark_column: str | None = None

    @property
    def has_watermark_column(self) -> bool:
        return self.watermark_column is not None


@dc.dataclass(frozen=True)
class TransformProperties:
    """Optional dbt transform configuration."""

    dbt_dir: str
    dbt_select: str = ""


Watermark = namedtuple("Watermark", ["column", "value"])
"""Hold information on a Watermark for ingestion"""

WatermarkInfo = dict[str, Watermark]
"""Map a table name to a Watermark object"""
