from elt_common.typing import BaseExtract, BaseSourceConfig, DataChunks, TableProperties

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> dict[str, TableProperties]:
        return {
            "table_default_write": TableProperties(name="table_default_write"),
            "table_replace_mode": TableProperties(name="table_replace_mode", write_mode="replace"),
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
