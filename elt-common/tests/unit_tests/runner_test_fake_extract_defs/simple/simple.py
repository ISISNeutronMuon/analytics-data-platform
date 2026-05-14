from elt_common.extract import (
    BaseExtract,
    BaseSourceConfig,
)
from elt_common.typing import (
    DataChunks,
    ResourceProperties,
    ResourcePropertiesMap,
)

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class JSONPropertyWatermark:
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> ResourcePropertiesMap:
        return {
            "table_default_write": ResourceProperties(),
            "table_replace_mode": ResourceProperties(write_mode="replace"),
            "table_merge_mode": ResourceProperties(write_mode="merge", merge_on=["name"]),
            "empty": ResourceProperties(),
        }

    def extract(self) -> DataChunks:
        yield (
            "table_default_write",
            pa.table({"name": pa.array(["table_default_write"], type=pa.string())}),
        )
        yield (
            "table_replace_mode",
            pa.table({"name": pa.array(["table_replace_mode"], type=pa.string())}),
        )
        yield (
            "table_merge_mode",
            pa.table({"name": pa.array(["table_merge_mode"], type=pa.string())}),
        )
        yield (
            "empty",
            pa.table({"name": pa.array([], type=pa.string())}),
        )
