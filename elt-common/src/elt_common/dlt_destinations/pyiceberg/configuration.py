import dataclasses
from typing import Dict, Literal, Final, Optional, TypeAlias

from dlt.common.configuration import configspec
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
        properties = {"credential": self.client_credential()} if self.client_id else {}

        field_aliases: Dict[str, str] = {
            "access_delegation": f"{CATALOG_HEADER_PREFIX}X-Iceberg-Access-Delegation",
            "oauth2_server_uri": "oauth2-server-uri",
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


PyIcebergCatalogCredentials: TypeAlias = PyIcebergRestCatalogCredentials


@configspec(init=False)
class IcebergClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(
        default="pyiceberg", init=False, repr=False, compare=False
    )  # type: ignore[misc]

    bucket_url: str = None  # type: ignore
    catalog_type: Literal["rest"] = "rest"
    credentials: PyIcebergCatalogCredentials = None  # type: ignore

    # possible placeholders: {dataset_name}, {table_name}, {location_tag}
    table_location_layout: Optional[str] = None  # type: ignore

    @property
    def connection_properties(self) -> Dict[str, str]:
        """Returns a mapping of connection properties to pass to the catalog constructor"""
        return self.credentials.as_dict() if self.credentials is not None else {}

    def on_resolved(self) -> None:
        self.normalize_bucket_url()

        # Check we have the minimum number of required authentication properties
        # if any are supplied
        auth_props = {
            prop: getattr(self.credentials, prop)
            for prop in ("oauth2_server_uri", "client_id", "client_secret")
        }

        non_null_count = sum(map(lambda x: 1 if x is not None else 0, auth_props.values()))
        if non_null_count != 0 and non_null_count != 3:
            raise DestinationTerminalException(
                f"Missing required configuration value(s) for authentication: {list(name for name, value in auth_props.items() if value is None)}"
            )

    def normalize_bucket_url(self) -> None:
        """Normalizes bucket_url, i.e. removes any trailing slashes"""
        self.bucket_url = self.bucket_url.rstrip("/")

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.credentials.uri:
            return digest128(self.credentials.uri)
        return ""
