"""Provides an example of defining resources where the tables to be loaded are determined dynamically,e.g.
when loading all tables from a database schema.
"""

import functools

from elt_common.extract import (
    BaseSourceConfig,
    BaseExtract,
    DataChunks,
    ResourceProperties,
    ResourcePropertiesMap,
)
import pyarrow as pa

# Imagine this were some function that returns the list of tables within a database...
SOURCE_TABLES = [f"table_{num}" for num in range(1, 6)]


class SourceConfig(BaseSourceConfig):
    pass


class Extract(BaseExtract):
    source_config_cls = SourceConfig

    def resource_properties(self) -> ResourcePropertiesMap:
        props = {}
        for table_name in SOURCE_TABLES:
            props[table_name] = ResourceProperties(
                extractor=functools.partial(parametrized_extractor, table_name=table_name)
            )

        return props


def parametrized_extractor(_, table_name: str) -> DataChunks:
    yield pa.table({"name": [table_name]})
