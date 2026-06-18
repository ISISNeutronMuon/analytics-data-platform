import pyarrow as pa

from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    ResourceWriteProperties,
)


class Extract(BaseExtract):
    def extract_resource_properties(self):
        yield (
            "table_default_write",
            ResourceProperties(
                extractor=extract_with_name("table_default_write"),
                write_properties=ResourceWriteProperties(),
                watermark_column=None,
            ),
        )
        yield (
            "table_replace_mode",
            ResourceProperties(
                extractor=extract_with_name("table_replace_mode"),
                write_properties=ResourceWriteProperties(write_mode="replace"),
                watermark_column=None,
            ),
        )
        yield (
            "table_merge_mode",
            ResourceProperties(
                extractor=extract_with_name("table_merge_mode"),
                write_properties=ResourceWriteProperties(write_mode="merge", merge_on=["name"]),
                watermark_column=None,
            ),
        )
        yield (
            "empty",
            ResourceProperties(
                extractor=extract_empty,
                write_properties=ResourceWriteProperties(),
                watermark_column=None,
            ),
        )


def extract_with_name(name: str):
    def extractor(_):
        yield pa.table(
            {
                "name": pa.array([name], type=pa.string()),
            }
        )

    return extractor


def extract_empty(_):
    yield pa.table({"name": pa.array([], type=pa.string())})
