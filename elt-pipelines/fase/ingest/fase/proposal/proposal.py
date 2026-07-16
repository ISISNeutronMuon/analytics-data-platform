import logging
from urllib.parse import quote_plus
from pydantic_settings import BaseSettings, SettingsConfigDict

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties
from fase.utils.postgres import PostgresExtractor

LOGGER = logging.getLogger(__name__)

class PipelinePostgresConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="proposal__", 
        env_nested_delimiter="__",
        extra="ignore",
        protected_namespaces=()
    )

    drivername: str = "postgresql+psycopg2"
    host: str
    port: int
    username: str
    password: str
    database: str
    table: str 

    @property
    def target_tables(self) -> list[str]:
        """Splits the raw table string by commas and strips accidental whitespaces."""
        return [t.strip() for t in self.table.split(",") if t.strip()]

    @property
    def connection_uri(self) -> str:
        driver = self.drivername.strip()
        user = quote_plus(self.username.strip())
        pwd = quote_plus(self.password.strip())
        h = self.host.strip()
        db = self.database.strip()
        return f"{driver}://{user}:{pwd}@{h}:{self.port}/{db}"


class Extract(BaseExtract):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Instantiate pipeline config and pass it directly to the generic utility extractor
        self.postgres_config = PipelinePostgresConfig()
        self.extractor = PostgresExtractor(self.postgres_config)

    def extract_resource_properties(self):
        for table_name in self.postgres_config.target_tables:
            # Encapsulate extraction stream generator mapping
            yield (
                table_name,
                ResourceProperties(
                    extractor=lambda _, t=table_name: self.extractor.fetch_as_arrow(t),
                    write_properties=ResourceWriteProperties(write_mode="replace"),
                    watermark_column=None
                )
            )