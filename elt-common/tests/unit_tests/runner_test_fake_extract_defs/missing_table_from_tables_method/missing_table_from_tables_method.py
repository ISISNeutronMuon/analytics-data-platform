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


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def tables(self) -> ResourcePropertiesMap:
        return {
            "table_1": ResourceProperties(),
        }

    def extract(self) -> DataChunks:
        yield (
            "table_not_listed",
            pa.table({"name": pa.array(["table_not_listed"], type=pa.string())}),
        )
