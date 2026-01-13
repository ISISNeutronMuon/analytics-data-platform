import dataclasses
from typing import Dict, Literal, Final, Optional, TypeAlias, Type

from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128
from pyiceberg.utils.properties import HEADER_PREFIX as CATALOG_HEADER_PREFIX

TPyIcebergAccessDelegation: TypeAlias = Literal["vended-credentials", "remote-signing"]


@configspec(init=False)
class PyIcebergRestCatalogCredentials(CredentialsConfiguration):
    uri: str = None  # type: ignore
    project_id: Optional[str] = None
    warehouse: Optional[str] = None
    access_delegation: TPyIcebergAccessDelegation = "vended-credentials"
    oauth2_server_uri: Optional[str] = None  # This is the endpoint to use to retrieve a token
    client_id: Optional[TSecretStrValue] = None
    client_secret: Optional[TSecretStrValue] = None
    scope: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        """Return the credentials as a dictionary suitable for the Catalog constructor"""
        # Map variable names to property names
        # client_id & secret need to be combined
        properties = {"type": "rest"}
        if self.client_id:
            properties.update({"credential": self.client_credential()})

        field_aliases: Dict[str, str] = {
            "access_delegation": f"{CATALOG_HEADER_PREFIX}x-iceberg-access-delegation",
            "oauth2_server_uri": "oauth2-server-uri",
            "project_id": f"{CATALOG_HEADER_PREFIX}x-project-id",
        }
        skip_fields = ("client_id", "client_secret")
        properties.update(
            {
                field_aliases.get(cls_field, cls_field): value
                for cls_field, value in self.items()
                if value is not None and cls_field not in skip_fields
            }
        )

        return properties

    def client_credential(self) -> str:
        return f"{self.client_id}:{self.client_secret}"

    def on_resolved(self) -> None:
        # Check we have the minimum number of required authentication properties
        # if any are supplied
        auth_props = {
            prop: getattr(self, prop)
            for prop in ("oauth2_server_uri", "client_id", "client_secret")
        }

        non_null_count = sum(map(lambda x: 1 if x is not None else 0, auth_props.values()))
        if non_null_count != 0 and non_null_count != 3:
            raise DestinationTerminalException(
                f"Missing required configuration value(s) for authentication: {list(name for name, value in auth_props.items() if value is None)}"
            )


@configspec(init=False)
class PyIcebergSqlCatalogCredentials(CredentialsConfiguration):
    uri: str = None  # type: ignore
    warehouse: str = None  # type: ignore

    def as_dict(self) -> Dict[str, str]:
        """Return the credentials as a dictionary suitable for the Catalog constructor"""
        return {"type": "sql", "uri": self.uri, "warehouse": self.warehouse}

    def on_resolved(self) -> None:
        pass


PyIcebergCatalogCredentials: TypeAlias = (
    PyIcebergRestCatalogCredentials | PyIcebergSqlCatalogCredentials
)


@configspec(init=False)
class IcebergClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="pyiceberg", init=False, repr=False, compare=False
    )

    catalog_type: Literal["rest", "sql"] = "rest"
    credentials: PyIcebergCatalogCredentials = None  # type: ignore

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return (
            PyIcebergRestCatalogCredentials
            if self.catalog_type == "rest"
            else PyIcebergSqlCatalogCredentials
        )

    @property
    def connection_properties(self) -> Dict[str, str]:
        """Returns a mapping of connection properties to pass to the catalog constructor"""
        return self.credentials.as_dict() if self.credentials is not None else {}

    def on_resolved(self) -> None:
        return self.credentials.on_resolved()

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.credentials.uri:
            return digest128(self.credentials.uri)
        return ""
