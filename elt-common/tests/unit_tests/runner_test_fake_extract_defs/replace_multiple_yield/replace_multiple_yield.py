from elt_common.extract import (
    BaseExtract,
    BaseSourceConfig,
    DataChunks,
    ResourceProperties,
    ResourcePropertiesMap,
)

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def resource_properties(self) -> ResourcePropertiesMap:
        return {
            "table_replace_mode": ResourceProperties(extractor=extract, write_mode="replace"),
        }


def extract(_) -> DataChunks:
    yield pa.table({"name": pa.array(["table_replace_mode_call_1"], type=pa.string())})
    yield pa.table({"name": pa.array(["table_replace_mode_call_2"], type=pa.string())})
