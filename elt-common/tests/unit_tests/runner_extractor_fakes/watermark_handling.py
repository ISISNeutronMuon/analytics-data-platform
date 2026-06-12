from typing import Optional

import pyarrow as pa

from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    ResourceWriteProperties,
    Watermark,
)


class Extract(BaseExtract):
    def extract_resource_properties(self):
        yield (
            "table_with_watermark",
            ResourceProperties(
                extractor=table_with_id_watermark,
                write_properties=ResourceWriteProperties(),
                watermark_column="id",
            ),
        )

        yield (
            "table_without_watermark",
            ResourceProperties(
                extractor=table_without_watermark,
                write_properties=ResourceWriteProperties(),
                watermark_column=None,
            ),
        )


def table_with_id_watermark(watermark: Optional[Watermark]):
    ids = list(filter(lambda n: not watermark or n > watermark.value, (i for i in range(1000))))

    yield pa.table(
        {
            "id": pa.array(ids, type=pa.int32()),
            "value": pa.array([f"watermarked_value_{n}" for n in ids], pa.string()),
        }
    )


def table_without_watermark(_):
    yield pa.table(
        {
            "id": pa.array([i for i in range(500)], type=pa.int32()),
            "value": pa.array([f"unwatermarked_value_{n}" for n in range(500)], pa.string()),
        }
    )
