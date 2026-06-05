from abc import abstractmethod, ABC
import dataclasses as dc
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal


if TYPE_CHECKING:
    import pyarrow as pa


class BaseIO(ABC):
    @abstractmethod
    def ensure_namespace(self, namespace: str) -> None:
        raise NotImplementedError(
            "Subclass should impement `ensure_namespace` to ensure the namespace exists."
        )

    @abstractmethod
    def write_table(
        self,
        table_id: "Identifier",
        data: "pa.Table",
        write_mode: "WriteMode",
        *,
        merge_on: list[str] | None = None,
        partition: "PartitionConfig | None" = None,
        sort_order: "SortOrderConfig | None" = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        raise NotImplementedError(
            "Subclass should impement `write_table` to write a table to the destination."
        )


DataChunks = Iterator["pa.Table"]
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


WriteMode = Literal["append", "merge", "replace"]
"""Catalog write modes.

- Append: Append data to existing records
- Merge: Upsert data into existing records, updating values of any records that exist
- Replace: Before loading, drop the data in the destination then append the new records.
"""
