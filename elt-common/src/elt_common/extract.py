import dataclasses as dc
from typing import TYPE_CHECKING, Any, Callable, Iterator, Literal, Optional, get_args

WriteMode = Literal["append", "merge", "replace"]

if TYPE_CHECKING:
    import pyarrow as pa


@dc.dataclass(frozen=True)
class Watermark:
    column: str
    value: Any


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
