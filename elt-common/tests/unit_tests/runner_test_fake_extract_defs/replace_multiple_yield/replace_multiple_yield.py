from elt_common.typing import BaseExtract, BaseSourceConfig, DataChunks, TableIngestProperties

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> dict[str, TableIngestProperties]:
        return {
            "table_replace_mode": TableIngestProperties(write_mode="replace"),
        }

    def extract(self) -> DataChunks:
        yield (
            "table_replace_mode",
            pa.table({"name": pa.array(["table_replace_mode_call_1"], type=pa.string())}),
        )
        yield (
            "table_replace_mode",
            pa.table({"name": pa.array(["table_replace_mode_call_2"], type=pa.string())}),
        )
