import dataclasses as dc
from pathlib import Path
from typing import Any, Literal, Iterator, Optional, Sequence, Tuple

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
    tables: TableItems
    job_dir: Path = dc.field(default=Path("."))
    transform: "TransformProperties | None" = None

    @property
    def namespace(self) -> str:
        """The Iceberg namespace for this job: ``{domain}_{name}``."""
        return f"{self.domain}_{self.name}"


@dc.dataclass(frozen=False)
class TableCursor:
    """Encapsulate properties for a cursor within a table"""

    column: str
    max_value: Optional[Any]


@dc.dataclass(frozen=True)
class TableProperties:
    """Configuration for a single table within a job."""

    name: str
    write_mode: WriteMode = "append"
    cursor_column: TableCursor | None = None
    merge_on: Sequence[str] = ()
    partition: PartitionHint = dc.field(default_factory=dict)
    sort_order: SortOrderHint = dc.field(default_factory=dict)


@dc.dataclass(frozen=True)
class TransformProperties:
    """Optional dbt transform configuration."""

    dbt_dir: str
    dbt_select: str = ""
