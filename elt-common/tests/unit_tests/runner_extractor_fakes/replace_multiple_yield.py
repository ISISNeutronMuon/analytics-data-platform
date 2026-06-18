import pyarrow as pa

from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    ResourceWriteProperties,
)


class Extract(BaseExtract):
    def extract_resource_properties(self):
        yield (
            "table_replaced_with_multiple_chunks",
            ResourceProperties(
                extractor=extract_multiple_chunks,
                write_properties=ResourceWriteProperties(write_mode="replace"),
                watermark_column=None,
            ),
        )


def extract_multiple_chunks(_):
    yield pa.table(
        {
            "id": pa.array([i for i in range(500)], type=pa.int32()),
            "value": pa.array([f"first_half_{n}" for n in range(500)], pa.string()),
        }
    )

    yield pa.table(
        {
            "id": pa.array([i for i in range(500, 600)], type=pa.int32()),
            "value": pa.array([f"stragglers_{n}" for n in range(100)], pa.string()),
        }
    )
