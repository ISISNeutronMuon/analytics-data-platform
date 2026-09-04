from typing import Iterator

from elt_common.extract import BaseExtract, ResourceProperties


class Extract(BaseExtract):
    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        for i in range(3):
            yield (
                str(i),
                ResourceProperties(
                    extractor=extract_empty,
                ),
            )


def extract_empty(_watermark):
    yield []
