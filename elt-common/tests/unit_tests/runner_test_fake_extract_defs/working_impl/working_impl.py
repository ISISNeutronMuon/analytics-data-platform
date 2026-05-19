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
        """Return the properties are each table to be loaded"""
        return {
            "table_default_write": ResourceProperties(extractor=extract_table_default_write),
            "table_replace_mode": ResourceProperties(
                extractor=extract_table_replace_mode, write_mode="replace"
            ),
            "table_merge_mode": ResourceProperties(
                extractor=extract_table_merge_mode, write_mode="merge", merge_on=["name"]
            ),
            "empty": ResourceProperties(extractor=extract_empty),
        }


def extract_table_default_write(_) -> DataChunks:
    yield pa.table(
        {
            "name": pa.array(["table_default_write"], type=pa.string()),
        }
    )


def extract_table_replace_mode(_) -> DataChunks:
    yield pa.table({"name": pa.array(["table_replace_mode"], type=pa.string())})


def extract_table_merge_mode(_) -> DataChunks:
    yield pa.table({"name": pa.array(["table_merge_mode"], type=pa.string())})


def extract_empty(_) -> DataChunks:
    yield pa.table({"name": pa.array([], type=pa.string())})
