import dataclasses as dc
from pathlib import Path
from typing import Literal, Sequence


@dc.dataclass(frozen=True)
class ELTJobManifest:
    """Parsed representation of an ELT job"""

    name: str
    domain: str
    ingest_job_dir: Path

    @property
    def full_name(self) -> str:
        return f"{self.domain}.{self.name}"

    @property
    def destination_namespace(self) -> str:
        """The destination namespace for this job: ``{domain}_{name}``."""
        return f"{self.domain}_{self.name}"


PartitionHint = dict[str, str]
"""Define the configuration of a Table partition where a key represents a column and the mapped
value defines an Iceberg transformation.
"""

SortOrderHint = dict[str, str]
"""Define the sort order on the columns in the Iceberg table.
"""


@dc.dataclass(frozen=True)
class TableProperties:
    """Configuration for a single table within a job."""

    name: str
    write_mode: "WriteMode" = "append"
    merge_on: Sequence[str] = ()
    partition: PartitionHint = dc.field(default_factory=dict)
    sort_order: SortOrderHint = dc.field(default_factory=dict)


WriteMode = Literal["append", "merge", "replace"]
"""Catalog write modes.

- Append: Append data to existing records
- Merge: Upsert data into existing records, updating values of any records that exist
- Replace: Before loading, drop the data in the destination then append the new records.
"""
