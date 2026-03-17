"""Iceberg catalog configuration.

Reads connection properties from environment variables and provides
a ``connect_catalog()`` helper that returns a connected pyiceberg ``Catalog``.
"""

from typing import Literal, Optional

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.utils.properties import HEADER_PREFIX as CATALOG_HEADER_PREFIX


class CatalogCredentials(BaseSettings):
    """Credentials for connecting to an Iceberg catalog.

    For REST catalogs the OAuth2 fields (``oauth2_server_uri``, ``client_id``,
    ``client_secret``) must either all be provided or all be omitted.
    """

    model_config = SettingsConfigDict(
        env_prefix="DESTINATION__PYICEBERG__CREDENTIALS__",
    )

    uri: str
    warehouse: Optional[str] = None
    project_id: Optional[str] = None
    access_delegation: str = "vended-credentials"
    oauth2_server_uri: Optional[str] = None
    client_id: Optional[SecretStr] = None
    client_secret: Optional[SecretStr] = None
    scope: Optional[str] = None

    @model_validator(mode="after")
    def _validate_auth_fields(self) -> "CatalogCredentials":
        auth_fields = {
            "oauth2_server_uri": self.oauth2_server_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        non_null = sum(1 for v in auth_fields.values() if v is not None)
        if non_null not in (0, 3):
            missing = [name for name, value in auth_fields.items() if value is None]
            raise ValueError(
                f"Missing required authentication field(s): {missing}. "
                "Provide all of oauth2_server_uri, client_id, and client_secret, or none."
            )
        return self


class CatalogConfig(BaseSettings):
    """Top-level catalog configuration.

    Reads ``DESTINATION__PYICEBERG__CATALOG_TYPE`` and delegates credential
    resolution to :class:`CatalogCredentials`.
    """

    model_config = SettingsConfigDict(
        env_prefix="DESTINATION__PYICEBERG__",
    )

    catalog_type: Literal["rest", "sql"] = "rest"
    credentials: CatalogCredentials | None = None

    def model_post_init(self, __context) -> None:
        if self.credentials is None:
            self.credentials = CatalogCredentials()  # type: ignore[call-arg]

    def connection_properties(self) -> dict[str, str]:
        """Build the properties dict expected by ``pyiceberg.catalog.load_catalog``."""
        creds = self.credentials
        if self.catalog_type == "sql":
            return {"type": "sql", "uri": creds.uri, "warehouse": creds.warehouse or ""}

        # REST catalog
        properties: dict[str, str] = {"type": "rest"}
        if creds.client_id is not None and creds.client_secret is not None:
            properties["credential"] = (
                f"{creds.client_id.get_secret_value()}:{creds.client_secret.get_secret_value()}"
            )

        field_map: dict[str, str] = {
            "access_delegation": f"{CATALOG_HEADER_PREFIX}x-iceberg-access-delegation",
            "oauth2_server_uri": "oauth2-server-uri",
            "project_id": f"{CATALOG_HEADER_PREFIX}x-project-id",
        }
        simple_fields = ("uri", "warehouse", "scope")
        for field_name in simple_fields:
            value = getattr(creds, field_name, None)
            if value is not None:
                properties[field_name] = value

        for field_name, property_name in field_map.items():
            value = getattr(creds, field_name, None)
            if value is not None:
                properties[property_name] = value

        return properties

    def connect_catalog(self) -> Catalog:
        """Create and return a connected pyiceberg Catalog."""
        return load_catalog(name="default", **self.connection_properties())
