from abc import abstractmethod
import dataclasses as dc
import json
from typing import Any, Callable, get_args

import pyarrow as pa
import pyarrow.compute as pc
from pydantic_settings import BaseSettings

from .typing import DataChunks, PartitionConfig, SortOrderConfig, WriteMode


_TABLE_WATERMARK_KEY = "ingest.watermark"


@dc.dataclass(frozen=True, kw_only=True)
class ResourceProperties:
    """Configuration for a single resource within a job."""

    # Required properties
    extractor: Callable[["Watermark | None"], DataChunks]

    # Destination table
    merge_on: list[str] = dc.field(default_factory=list)
    partition: PartitionConfig = dc.field(default_factory=dict)
    sort_order: SortOrderConfig = dc.field(default_factory=dict)
    write_mode: WriteMode = "append"

    # Ingestion properties
    watermark_column: str | None = None

    def __post_init__(self):
        if self.write_mode not in get_args(WriteMode):
            raise ValueError(
                f"Invalid write mode '{self.write_mode}'. Allowed values: {get_args(WriteMode)}"
            )
        if self.write_mode == "merge" and not self.merge_on:
            raise ValueError("'merge_on' must be provided when mode='merge'")


ResourcePropertiesMap = dict[str, ResourceProperties]
"""Map a resource name to a set of properties configuring that resource."""


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
    def resource_properties(self) -> ResourcePropertiesMap:
        raise NotImplementedError(
            "Subclass should implement `table_properties()` to provide details of tables to be extracted."
        )


@dc.dataclass(frozen=True)
class Watermark:
    """Store a watermark value as a string to indicate that a table already has data upto and including this value."""

    value: Any

    @classmethod
    def property_key(cls) -> str:
        return _TABLE_WATERMARK_KEY


class WatermarkSerializerJSONMaxColumnValue:
    """Convert watermarks to/from JSON"""

    def serialize(self, column: str, data: "pa.Table") -> str:
        return json.dumps({"column": column, "value": pc.max(data[column]).as_py()})

    def deserialize(self, str) -> Watermark:
        return Watermark(json.loads(str)["value"])
