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

        yield (
            "table_watermarked_out_of_order_data",
            ResourceProperties(
                extractor=out_of_order_data,
                write_properties=ResourceWriteProperties(),
                watermark_column="id",
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


def out_of_order_data(_):
    yield pa.table(
        {
            "id": pa.array([i for i in range(10, 20)], type=pa.int32()),
            "value": pa.array([f"first_half_{n}" for n in range(10)], pa.string()),
        }
    )
    yield pa.table(
        {
            "id": pa.array([i for i in range(10)], type=pa.int32()),
            "value": pa.array([f"second_half_{n}" for n in range(10)], pa.string()),
        }
    )
