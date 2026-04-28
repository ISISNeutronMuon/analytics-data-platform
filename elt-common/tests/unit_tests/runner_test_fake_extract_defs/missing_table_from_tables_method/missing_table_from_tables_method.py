from elt_common.typing import BaseExtract, BaseSourceConfig, DataChunks, TableIngestProperties

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> dict[str, TableIngestProperties]:
        return {
            "table_1": TableIngestProperties(),
        }

    def extract(self) -> DataChunks:
        yield (
            "table_not_listed",
            pa.table({"name": pa.array(["table_not_listed"], type=pa.string())}),
        )
