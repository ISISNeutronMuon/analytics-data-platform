from abc import abstractmethod
import dataclasses as dc
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal, get_args


if TYPE_CHECKING:
    import pyarrow as pa


class BaseIO:
    @abstractmethod
    def ensure_namespace(self, namespace: str) -> None:
        raise NotImplementedError(
            "Subclass should impement `ensure_namespace` to ensure the namespace exists."
        )

    @abstractmethod
    def write_table(
        self,
        table_id: "Identifier",
        props: "ResourceProperties",
        data: "pa.Table",
        *,
        force_write_mode: "WriteMode | None" = None,
    ) -> None:
        raise NotImplementedError(
            "Subclass should impement `write_table` to write a table to the destination."
        )


DataChunk = tuple[str, "pa.Table"]
"""Map a string name to a chunk of data to be loaded into the named table."""

DataChunks = Iterator[DataChunk]
"""An iterator to a collection of DataChunk objects."""


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


Identifier = tuple[str, ...]

PartitionConfig = dict[str, str]
"""Define the configuration of a Table partition where a key represents a column and the mapped
value defines an Iceberg transformation.
"""

SortOrderConfig = dict[str, str]
"""Define the sort order on the columns in the Iceberg table.
"""


@dc.dataclass(frozen=True)
class ResourceProperties:
    """Configuration for a single resource within a job."""

    write_mode: "WriteMode" = "append"
    merge_on: list[str] = dc.field(default_factory=list)
    partition: PartitionConfig = dc.field(default_factory=dict)
    sort_order: SortOrderConfig = dc.field(default_factory=dict)

    def __post_init__(self):
        if self.write_mode not in get_args(WriteMode):
            raise ValueError(
                f"Invalid write mode '{self.write_mode}'. Allowed values: {get_args(WriteMode)}"
            )
        if self.write_mode == "merge" and not self.merge_on:
            raise ValueError("'merge_on' must be provided when mode='merge'")


ResourcePropertiesMap = dict[str, ResourceProperties]
"""Map a resource name to a set of properties configuring that resource."""


WriteMode = Literal["append", "merge", "replace"]
"""Catalog write modes.

- Append: Append data to existing records
- Merge: Upsert data into existing records, updating values of any records that exist
- Replace: Before loading, drop the data in the destination then append the new records.
"""
