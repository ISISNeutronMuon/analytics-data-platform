from elt_common.typing import BaseExtract, BaseSourceConfig, DataChunks, TableProperties

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> dict[str, TableProperties]:
        return {
            "table_1": TableProperties(),
        }

    def extract(self) -> DataChunks:
        yield (
            "table_not_listed",
            pa.table({"name": pa.array(["table_not_listed"], type=pa.string())}),
        )
