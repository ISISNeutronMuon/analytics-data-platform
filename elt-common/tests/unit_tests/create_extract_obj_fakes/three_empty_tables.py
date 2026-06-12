from typing import Iterator

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties


class Extract(BaseExtract):
    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        for i in range(3):
            yield (
                str(i),
                ResourceProperties(
                    extractor=extract_empty,
                    write_properties=ResourceWriteProperties(),
                    watermark_column=None,
                ),
            )


def extract_empty(watermark):
    yield []
