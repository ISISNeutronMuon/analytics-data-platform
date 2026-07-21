import dataclasses as dc
import importlib.util
import json
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Callable, Iterator, Optional, get_args

from pydantic_settings import BaseSettings

from elt_common.pipeline_types import ELTJobManifest, PartitionConfig, SortOrderConfig, WriteMode

if TYPE_CHECKING:
    import pyarrow as pa

EXTRACT_CLS_NAME = "Extract"
"""Ingest pipelines must define a class with this name which extends BaseExtract."""


@dc.dataclass(frozen=True)
class Watermark:
    column: str
    value: str | int | float

    def serialize(self) -> str:
        return json.dumps({"column": self.column, "value": self.value})

    @staticmethod
    def deserialize(watermark_str: str) -> "Watermark":
        as_json = json.loads(watermark_str)
        if "column" not in as_json or as_json["column"] is None:
            raise ValueError(
                f"Couldn't deserialize {watermark_str} as a watermark, 'column' was missing"
            )
        elif "value" not in as_json or as_json["value"] is None:
            raise ValueError(
                f"Couldn't deserialize {watermark_str} as a watermark, 'value' was missing"
            )

        column = as_json["column"]
        if type(column) is not str:
            raise ValueError(f"Watermark 'column' must be a string, '{column}' is not valid")

        value = as_json["value"]
        if type(value) not in (str, int, float):
            raise ValueError(
                f"Watermark 'value' must be a string or number, '{value}' is not valid"
            )

        return Watermark(column=column, value=value)


@dc.dataclass(frozen=True, kw_only=True)
class ResourceWriteProperties:
    """Properties which control how data is written to iceberg.

    Defaults to appending with no partitioning or sorting.

    :ivar merge_on: Column(s) which are considered for matching rows with existing
                    data when write_mode is 'merge'. Required if write_mode is 'merge'.
    :ivar partition: Column names mapped to Iceberg transforms which will be used for partitioning.
    :ivar sort_order: Mapping of columns to their sort directions.
    :ivar write_mode: How the data should be written to iceberg.
    """

    merge_on: list[str] = dc.field(default_factory=list)
    partition: PartitionConfig = dc.field(default_factory=dict)
    sort_order: SortOrderConfig = dc.field(default_factory=dict)
    write_mode: WriteMode = "append"

    def __post_init__(self):
        if self.write_mode not in get_args(WriteMode):
            raise ValueError(
                f"Invalid write mode '{self.write_mode}'. Allowed values: {get_args(WriteMode)}"
            )
        if self.write_mode == "merge" and not self.merge_on:
            raise ValueError("'merge_on' must be provided when mode='merge'")


@dc.dataclass(frozen=True, kw_only=True)
class ResourceProperties:
    """Configuration for a single resource to be extracted.

    :ivar extractor: Function that extracts data from the source. Can return multiple chunks of data.
    :ivar write_properties: Properties that control how the resource will be written to iceberg.
    :ivar watermark_column: Column in the extracted data that should be used for watermarking.
    """

    extractor: Callable[[Optional[Watermark]], "Iterator[pa.Table]"]
    write_properties: ResourceWriteProperties
    watermark_column: Optional[str]


class BaseExtract(ABC):
    """Base class for ingest Extract classes"""

    config_cls: type[BaseSettings] = BaseSettings
    """Class used to provide configuration options.

    Override this in subclasses to provide custom configuration.

    Intended to be used with pydantic-settings.
    """

    def __init__(self, config: BaseSettings):
        self._config = config

    @property
    def config(self):
        return self._config

    @abstractmethod
    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        """Get ingestion properties for a data source.

        Each table/resource to be extracted from the data source should be returned
        as a tuple of (table/resource name, extraction properties). The 'extraction
        properties' include a method for reading the data, and properties to control
        how the data should be written to iceberg.

        During execution of this method, some implementations may maintain state
        (e.g. a database connection) which is required to extract the data. To ensure
        that state is available for the 'extractor'(s), they should be called whilst
        iterating over the results of this method.

        :returns: Iterator of tuples of (table/resource name, extraction properties).
        """
        pass


def create_extract_obj(job: ELTJobManifest) -> BaseExtract:
    """Given a job directory and name, create the object that will perform the data extraction."""
    extract_cls = _get_extract_cls(job)
    config_cls = extract_cls.config_cls

    return extract_cls(config_cls(_env_prefix=f"{job.name}__"))


def _get_extract_cls(job: ELTJobManifest) -> type[BaseExtract]:
    """Get the class that will handle the extraction."""
    extract_script = job.ingest_job_dir / f"{job.name}.py"
    if not extract_script.exists():
        raise RuntimeError(f"No extraction class definition file found at '{extract_script}'")

    module = _import_module_from_path(job.name, job.ingest_job_dir)
    return _get_extract_cls_from_module(module)


def _import_module_from_path(module_name: str, module_dir: Path):
    """Import a module given its name and directory"""
    sys.path.append(str(module_dir))
    return importlib.import_module(module_name)


def _get_extract_cls_from_module(module: ModuleType) -> type[BaseExtract]:
    """Get the class attribute that will handle extraction.

    :raises AttributeError: if the module doesn't include an 'Extract' attribute
    :raises TypeError: if 'Extract' in the module isn't a subclass of BaseExtract
    """
    try:
        extract_cls = getattr(module, EXTRACT_CLS_NAME)
    except AttributeError:
        raise AttributeError(
            f"Module '{module.__name__}' doesn't include an "
            f"'{EXTRACT_CLS_NAME}' class, which is required for defining an ingest job"
        )

    if not isinstance(extract_cls, type):
        raise TypeError(f"'{EXTRACT_CLS_NAME}' in module '{module.__name__}' is not a class")

    if not issubclass(extract_cls, BaseExtract):
        raise TypeError(
            f"'{EXTRACT_CLS_NAME}' in module '{module.__name__}' doesn't subclass elt_common.extract.BaseExtract"
        )

    return extract_cls
