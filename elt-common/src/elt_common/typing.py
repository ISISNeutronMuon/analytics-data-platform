from abc import abstractmethod
import dataclasses as dc
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal, Sequence

from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    import pyarrow as pa

DataChunk = tuple[str, "pa.Table"]
"""Map a string name to a chunk of data to be loaded into the named table."""

DataChunks = Iterator[DataChunk]
"""An iterator to a collection of DataChunk objects."""


class BaseSourceConfig(BaseSettings):
    """Base for all classes providing runtime configuration"""

    pass


class BaseExtract:
    """Base class for all Extract classes"""

    def __init__(self, source_config: BaseSettings):
        self._source_config = source_config

    @property
    def source_config(self):
        return self._source_config

    @abstractmethod
    def tables(self) -> dict[str, "TableProperties"]:
        raise NotImplementedError(
            "Subclass should implement `tables()` to provide details of tables to be extracted."
        )

    @abstractmethod
    def extract(self) -> Iterator[DataChunk]:
        raise NotImplementedError("Subclass should implement `extract` to perform data extraction.")


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


PartitionConfig = dict[str, str]
"""Define the configuration of a Table partition where a key represents a column and the mapped
value defines an Iceberg transformation.
"""

SortOrderConfig = dict[str, str]
"""Define the sort order on the columns in the Iceberg table.
"""


@dc.dataclass(frozen=True)
class TableProperties:
    """Configuration for a single table within a job."""

    name: str
    write_mode: "WriteMode" = "append"
    merge_on: Sequence[str] = ()
    partition: PartitionConfig = dc.field(default_factory=dict)
    sort_order: SortOrderConfig = dc.field(default_factory=dict)


WriteMode = Literal["append", "merge", "replace"]
"""Catalog write modes.

- Append: Append data to existing records
- Merge: Upsert data into existing records, updating values of any records that exist
- Replace: Before loading, drop the data in the destination then append the new records.
"""
