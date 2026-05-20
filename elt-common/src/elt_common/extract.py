from abc import abstractmethod
import dataclasses as dc
import importlib.util
from pathlib import Path
import json
from typing import Any, Callable, get_args

import pyarrow as pa
import pyarrow.compute as pc
from pydantic_settings import BaseSettings

from .typing import DataChunks, ELTJobManifest, PartitionConfig, SortOrderConfig, WriteMode

EXTRACT_CLS_NAME = "Extract"


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
    """Store a deserialized watermark value"""

    value: Any


class WatermarkSerializer:
    """Convert watermarks to/from JSON"""

    @abstractmethod
    def serialize(self, column: str, data: "pa.Table") -> str:
        raise NotImplementedError("Override serialize in subclass.")

    @abstractmethod
    def deserialize(self, watermark_str: str) -> Watermark:
        raise NotImplementedError("Override deserialize in subclass.")


class WatermarkSerializerJSONMaxColumnValue(WatermarkSerializer):
    """Convert watermarks to/from JSON"""

    def serialize(self, column: str, data: "pa.Table") -> str:
        return json.dumps({"column": column, "value": pc.max(data[column]).as_py()})

    def deserialize(self, watermark_str: str) -> Watermark:
        return Watermark(json.loads(watermark_str)["value"])


def create_extract_obj(job: ELTJobManifest) -> BaseExtract:
    """Given the job directory and name create the object that will perform the data extraction"""
    extract_cls = _get_extract_cls(job)
    source_config_cls = extract_cls.source_config_cls

    return extract_cls(source_config_cls(_env_prefix=f"{job.name}__"))


##################
# private
##################
def _get_extract_cls(job: ELTJobManifest) -> Any:
    """Get the class that will handle the extraction."""
    custom_extract_script = job.ingest_job_dir / f"{job.name}.py"
    if custom_extract_script.exists():
        return _get_extract_cls_from_module_path(job.name, custom_extract_script)
    else:
        raise RuntimeError(
            f"No extraction class definition file found at '{custom_extract_script}'"
        )


def _get_extract_cls_from_module_path(module_name: str, file_path: Path) -> Any:
    """Get the class attribute that will handle extraction

    :raises: AttributeError if the attribute doesn't exist
    """
    module = _import_module_from_path(module_name, file_path)
    return getattr(module, EXTRACT_CLS_NAME)


def _import_module_from_path(module_name: str, file_path: Path):
    """Import a module given its name and file location"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        raise ImportError(f"Unable to find module spec for '{module_name}' at '{file_path}'")
    module = importlib.util.module_from_spec(spec)
    if spec.loader is not None:
        spec.loader.exec_module(module)
    else:
        raise ImportError(f"Module spec for {module_name} @ '{file_path}' has no loader attribute")

    return module
