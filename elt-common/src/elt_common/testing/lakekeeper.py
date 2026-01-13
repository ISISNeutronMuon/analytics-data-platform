from contextlib import contextmanager
import dataclasses
from typing import Callable, Generator

import uuid

import pyarrow as pa
from pyiceberg.catalog import Catalog as PyIcebergCatalog, load_catalog
import requests
import tenacity

from elt_common.dlt_destinations.pyiceberg.configuration import PyIcebergRestCatalogCredentials

from . import DEFAULT_RETRY_ARGS, Endpoint, Settings

DEFAULT_REQUESTS_TIMEOUT = 10.0


class Server:
    """Wraps a Lakekeeper instance. It is assumed that the instance is bootstrapped."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.access_token = access_token(settings)

    @property
    def token_endpoint(self) -> Endpoint:
        return self.settings.openid_provider_uri + "/protocol/openid-connect/token"

    def catalog_endpoint(self, *, version: int | None = None) -> Endpoint:
        endpoint = self.settings.lakekeeper_url + "/catalog"
        if version:
            endpoint += f"/v{version}"

        return endpoint

    def management_endpoint(self, *, version: int | None = None) -> Endpoint:
        endpoint = self.settings.lakekeeper_url + "/management"
        if version:
            endpoint += f"/v{version}"
        return endpoint

    def warehouse_endpoint(self, *, version: int = 1) -> Endpoint:
        return self.management_endpoint(version=version) + "/warehouse"

    def create_warehouse(
        self, name: str, project_id: str, storage_config: dict
    ) -> "RestCatalogWarehouse":
        """Create a warehouse in this server"""

        payload = {
            "project-id": project_id,
            **storage_config,
        }

        response = self._request_with_auth(
            requests.post,
            self.warehouse_endpoint(),
            json=payload,
        )
        try:
            response.raise_for_status()
        except Exception:
            raise ValueError(
                f"Failed to create warehouse ({response.status_code}): {response.text}."
            )

        warehouse_id = response.json()["warehouse-id"]
        print(f"Created warehouse {name} with ID {warehouse_id}")

        return RestCatalogWarehouse(
            self,
            name,
            uuid.UUID(warehouse_id),
            f"s3://{storage_config['storage-profile']['bucket']}",
        )

    @tenacity.retry(**DEFAULT_RETRY_ARGS)
    def purge_warehouse(self, warehouse: "RestCatalogWarehouse") -> None:
        """Purge all of the data in the given warehouse"""
        warehouse.purge()

    @tenacity.retry(**DEFAULT_RETRY_ARGS)
    def delete_warehouse(self, warehouse: "RestCatalogWarehouse") -> None:
        """Purge all of the data in the given warehouse and delete it"""
        response = self._request_with_auth(
            requests.delete, self.warehouse_endpoint() + f"/{str(warehouse.project_id)}"
        )
        response.raise_for_status()

    def _request_with_auth(self, requests_method: Callable, url: Endpoint, **kwargs):
        """Make a request, adding in the auth token"""
        headers = kwargs.setdefault("headers", {})
        headers.update({"Authorization": f"Bearer {self.access_token}"})
        kwargs.setdefault("timeout", DEFAULT_REQUESTS_TIMEOUT)
        return requests_method(url=str(url), **kwargs)


@dataclasses.dataclass
class RestCatalogWarehouse:
    server: Server
    name: str
    project_id: uuid.UUID
    bucket_url: str

    def connect(self) -> PyIcebergCatalog:
        """Connect to the warehouse in the catalog"""
        creds = PyIcebergRestCatalogCredentials()
        creds.uri = str(self.server.catalog_endpoint())
        creds.project_id = self.server.settings.project_id
        creds.warehouse = self.name
        creds.oauth2_server_uri = str(self.server.token_endpoint)
        creds.client_id = self.server.settings.openid_client_id
        creds.client_secret = self.server.settings.openid_client_secret
        creds.scope = self.server.settings.openid_scope
        return load_catalog(name="default", **creds.as_dict())

    @contextmanager
    def create_test_tables(
        self,
        namespace_count: int,
        table_count_per_ns: int,
        namespace_prefix: str = "test_ns_",
        table_prefix: str = "test_table_",
        snapshot_count: int = 1,
    ) -> Generator[PyIcebergCatalog]:
        """Create tables and additional snapshots of tables for maintenance testing"""

        def append_snapshot(table_id: str, id_start: int):
            """Append new data to the table to create another Iceberg snapshot.
            If the table does not exist it is created."""
            rows_per_frame = 2
            id_end = id_start + rows_per_frame - 1
            test_data = pa.Table.from_pydict(
                {
                    "id": pa.array([id for id in range(id_start, id_end + 1)]),
                    "value": pa.array([f"value-{id}" for id in range(id_start, id_end + 1)]),
                }
            )
            if catalog.table_exists(table_id):
                table = catalog.load_table(table_id)
            else:
                table = catalog.create_table(table_id, schema=test_data.schema)

            table.append(test_data)
            return id_start + rows_per_frame

        catalog = self.connect()

        for ns_index in range(namespace_count):
            ns_name = f"{namespace_prefix}{ns_index}"
            catalog.create_namespace(ns_name)
            for table_index in range(table_count_per_ns):
                id_start = 1
                for _ in range(snapshot_count):
                    id_start = append_snapshot(f"{ns_name}.{table_prefix}{table_index}", id_start)
        try:
            yield catalog
        finally:
            self.purge()

    def purge(self):
        """Purge all contents in warehouse"""
        catalog = self.connect()
        for ns in catalog.list_namespaces():
            for view_id in catalog.list_views(ns):
                catalog.drop_view(view_id)
            for table_id in catalog.list_tables(ns):
                catalog.purge_table(table_id)
            catalog.drop_namespace(ns)


@tenacity.retry(**DEFAULT_RETRY_ARGS)
def access_token(settings: Settings) -> str:
    response = requests.post(
        token_endpoint(settings),
        data={
            "grant_type": "client_credentials",
            "client_id": settings.openid_client_id,
            "client_secret": settings.openid_client_secret,
            "scope": settings.openid_scope,
        },
        timeout=DEFAULT_REQUESTS_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def token_endpoint(settings: Settings) -> str:
    response = requests.get(
        str(settings.openid_provider_uri + "/.well-known/openid-configuration"),
        timeout=DEFAULT_REQUESTS_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["token_endpoint"]
