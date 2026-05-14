from abc import abstractmethod

from pydantic_settings import BaseSettings

from .typing import DataChunks, ResourcePropertiesMap


class BaseSourceConfig(BaseSettings):
    """Base for all classes providing runtime configuration"""

    pass


class BaseExtract:
    """Base class for all Extract classes"""

    def __init__(self, source_config: BaseSettings):
        self._source_config = source_config

    @property
    def source_config(self):
        return self._source_config

    @abstractmethod
    def tables(self) -> ResourcePropertiesMap:
        raise NotImplementedError(
            "Subclass should implement `tables()` to provide details of tables to be extracted."
        )

    @abstractmethod
    def extract(self) -> DataChunks:
        raise NotImplementedError("Subclass should implement `extract` to perform data extraction.")
