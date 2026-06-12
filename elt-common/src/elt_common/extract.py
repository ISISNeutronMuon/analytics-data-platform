import dataclasses as dc
import json
from typing import TYPE_CHECKING, Callable, Iterator, Optional, get_args

from elt_common.typing import WriteMode

if TYPE_CHECKING:
    import pyarrow as pa


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
