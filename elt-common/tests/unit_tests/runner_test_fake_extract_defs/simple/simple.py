from elt_common.typing import BaseExtract, BaseSourceConfig, DataChunks, TableProperties

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> dict[str, TableProperties]:
        return {
            "table_default_write": TableProperties(),
            "table_replace_mode": TableProperties(write_mode="replace"),
            "table_merge_mode": TableProperties(write_mode="merge", merge_on=["name"]),
            "empty": TableProperties(),
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
