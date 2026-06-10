from typing import Iterator, Optional

from pydantic_settings import BaseSettings

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties


class ACustomConfig(BaseSettings):
    required_str: str
    int_with_default: int = 123
    optional_str: Optional[str] = None


class Extract(BaseExtract):
    config_cls = ACustomConfig

    def resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        yield (
            "",
            ResourceProperties(
                extractor=extract_nothing,
                write_properties=ResourceWriteProperties(),
                watermark_column=None,
            ),
        )


def extract_nothing(watermark):
    yield []
