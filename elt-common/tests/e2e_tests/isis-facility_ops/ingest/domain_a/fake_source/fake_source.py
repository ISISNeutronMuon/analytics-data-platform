from elt_common.extract import (
    BaseExtract,
    BaseSourceConfig,
    DataChunks,
    ResourceProperties,
    ResourcePropertiesMap,
)

import pyarrow as pa


class SourceConfig(BaseSourceConfig):
    # Test access to config and chunked loading
    chunk_size: int = 1000


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def resource_properties(self) -> ResourcePropertiesMap:
        """Return the properties are each table to be loaded"""
        return {
            "comments": ResourceProperties(
                extractor=self.extract_comments,
                partition={"id": "bucket[2]"},
                sort_order={"id": "asc"},
                watermark_column="id",
            )
        }

    def extract_comments(self, _) -> DataChunks:
        # Yield multiple data chunks
        id_start = 100
        for _ in range(2):
            ids = list(range(id_start, id_start + self.source_config.chunk_size))
            yield pa.table(
                {
                    "id": pa.array(ids, type=pa.int64()),
                    "comment": pa.array([f"comment for {id}" for id in ids], type=pa.string()),
                }
            )
            id_start = ids[-1] + 1
