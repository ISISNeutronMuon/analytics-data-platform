import dataclasses as dc
import importlib.util
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator, Optional, get_args

from pydantic_settings import BaseSettings

from elt_common.typing import ELTJobManifest, WriteMode

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
    # Destination table
    merge_on: list[str] = dc.field(default_factory=list)
    partition: dict[str, str] = dc.field(default_factory=dict)
    sort_order: dict[str, str] = dc.field(default_factory=dict)
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
    """Configuration for a single resource to be extracted."""

    # Required properties
    extractor: Callable[[Optional[Watermark]], "Iterator[pa.Table]"]
    write_properties: ResourceWriteProperties

    # Ingestion properties
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
        pass


def create_extract_obj(job: ELTJobManifest) -> BaseExtract:
    """Given a job directory and name, create the object that will perform the data extraction."""
    extract_cls = _get_extract_cls(job)
    config_cls = extract_cls.config_cls

    return extract_cls(config_cls(_env_prefix=f"{job.name}__"))


def _get_extract_cls(job: ELTJobManifest) -> type[BaseExtract]:
    """Get the class that will handle the extraction."""
    extract_script = job.ingest_job_dir / f"{job.name}.py"
    if extract_script.exists():
        return _get_extract_cls_from_module_path(job.name, extract_script)
    else:
        raise RuntimeError(f"No extraction class definition file found at '{extract_script}'")


def _get_extract_cls_from_module_path(module_name: str, file_path: Path) -> type[BaseExtract]:
    """Get the class attribute that will handle extraction.

    :raises AttributeError: if the module doesn't include an 'Extract' attribute
    :raises TypeError: if 'Extract' in the module isn't a subclass of BaseExtract
    """
    module = _import_module_from_path(module_name, file_path)
    try:
        extract_cls = getattr(module, EXTRACT_CLS_NAME)
    except AttributeError as e:
        raise AttributeError(
            f"Module '{module_name}' doesn't include an "
            f"'{EXTRACT_CLS_NAME}' class, which is required for defining an ingest job",
            e,
        )

    if not isinstance(extract_cls, type):
        raise TypeError(f"'{EXTRACT_CLS_NAME}' in module '{module_name}' is not a class")

    if not issubclass(extract_cls, BaseExtract):
        raise TypeError(
            f"'{EXTRACT_CLS_NAME}' in module '{module_name}' doesn't subclass elt_common.extract.BaseExtract"
        )

    return extract_cls


def _import_module_from_path(module_name: str, file_path: Path):
    """Import a module given its name and file location."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        raise ImportError(f"Unable to find module spec for '{module_name}' at '{file_path}'")
    module = importlib.util.module_from_spec(spec)
    if spec.loader is not None:
        spec.loader.exec_module(module)
    else:
        raise ImportError(f"Module spec for {module_name} @ '{file_path}' has no loader attribute")

    return module
