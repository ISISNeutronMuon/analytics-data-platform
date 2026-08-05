import logging
from typing import Optional

from elt_common.extract import ResourceWriteProperties, Watermark
from elt_common.sources.sqldatabase import (
    ResourceProperties,
    SqlDatabaseExtract,
    TableInfo,
)
from fase.utils.oracle import OracleExtractor
from pydantic_settings import BaseSettings, SettingsConfigDict

LOGGER = logging.getLogger(__name__)


class PipelineOracleConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="isisuserdb__",
        env_nested_delimiter="__",
        extra="ignore",
        protected_namespaces=(),
    )

    drivername: str = "oracle+cx_oracle"
    host: str
    port: int
    username: str
    password: str
    schema: str
    table: str

    database: str

    @property
    def target_tables(self) -> list[str]:
        return [t.strip() for t in self.table.split(",") if t.strip()]

    @property
    def connection_uri(self) -> str:
        return f"{self.drivername}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class ExtendedResourceProperties:
    def __init__(self, base_props: ResourceProperties, name: str, columns: dict):
        self._base_props = base_props
        self.name = name
        self.columns = columns

    def __getattr__(self, name):
        return getattr(self._base_props, name)


class Extract(SqlDatabaseExtract):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.oracle_config = PipelineOracleConfig()
        self.extractor = OracleExtractor(self.oracle_config)

    def table_info(self) -> dict[str, Optional[TableInfo]]:
        return {
            t: TableInfo(write_properties=ResourceWriteProperties(write_mode="replace"))
            for t in self.oracle_config.target_tables
        }

    def extract_resource_properties(self):
        for name in self.oracle_config.target_tables:
            col_hints = self.extractor.get_table_schema(name)

            def make_extractor_func(t_name=name):
                def extractor(watermark: Optional[Watermark] = None):
                    yield from self.extractor.fetch_as_arrow(t_name)

                return extractor

            base_properties = ResourceProperties(
                extractor=make_extractor_func(name),
                write_properties=ResourceWriteProperties(write_mode="replace"),
                watermark_column=None,
            )

            properties = ExtendedResourceProperties(
                base_props=base_properties, name=name, columns=col_hints
            )

            yield name.lower(), properties
