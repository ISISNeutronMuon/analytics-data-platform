from elt_common.extract import (
    BaseExtract,
    BaseSourceConfig,
    DataChunks,
    Watermark,
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
            "table_with_watermark_column": ResourceProperties(
                extractor=table_with_watermark_column, watermark_column="id"
            ),
            "table_without_watermark_column": ResourceProperties(
                extractor=table_without_watermark_column
            ),
        }


def table_with_watermark_column(watermark: Watermark | None) -> DataChunks:
    if watermark is not None:
        first_id_to_load = watermark.value + 1
        ids = list(range(first_id_to_load, first_id_to_load + 5))
    else:
        ids = list(range(100, 120))

    yield pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "name": pa.array(["table_with_watermark_column"] * len(ids), type=pa.string()),
        }
    )


def table_without_watermark_column(_) -> DataChunks:
    yield pa.table(
        {
            "id": pa.array(list(range(200, 210)), type=pa.int32()),
            "name": pa.array(["table_without_watermark_column"] * 10, type=pa.string()),
        }
    )
