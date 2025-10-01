from contextlib import contextmanager
import dataclasses
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List
import urllib.parse
import uuid
import warnings


# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.runtime.run_context import RunContext

from elt_common.dlt_destinations.pyiceberg.catalog import create_catalog
from elt_common.dlt_destinations.pyiceberg.configuration import PyIcebergCatalogCredentials
from minio import Minio
import pyarrow as pa
from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pydantic_settings import BaseSettings, SettingsConfigDict
import pytest
import requests
import tenacity

_RETRY_ARGS = {
    "wait": tenacity.wait_exponential(max=10),
    "stop": tenacity.stop_after_attempt(10),
    "reraise": True,
}


def initial_providers(self) -> List[ConfigProvider]:
    # do not read the global config
    # find the .dlt in the same directory as this file
    thisdir = Path(__file__).parent
    return [
        EnvironProvider(),
        SecretsTomlProvider(settings_dir=f"{thisdir}/.dlt"),
        ConfigTomlProvider(settings_dir=f"{thisdir}/.dlt"),
    ]


RunContext.initial_providers = initial_providers  # type: ignore[method-assign]


@dataclasses.dataclass
class Endpoint:
    raw_value: str
    internal_netloc: str

    def __add__(self, path: str) -> "Endpoint":
        return Endpoint(self.raw_value + path, self.internal_netloc)

    def __str__(self) -> str:
        return self.raw_value

    def value(self, *, use_internal_netloc: bool) -> str:
        if use_internal_netloc:
            fragments = list(urllib.parse.urlparse(self.raw_value))
            fragments[1] = self.internal_netloc
            return urllib.parse.urlunparse(fragments)
        else:
            return self.raw_value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="tests_",
    )

    # iceberg catalog
    # The default values assume the docker-compose.yml in the infra/local has been used.
    # These are provided for the convenience of easily running a debugger without having
    # to set up remote debugging
    host_netloc: str = "localhost:58080"
    docker_netloc: str = "traefik"
    s3_access_key: str = "adpuser"
    s3_secret_key: str = "adppassword"
    s3_bucket: str = "e2e-tests-warehouse"
    s3_endpoint: str = "http://minio:59000"
    s3_region: str = "local-01"
    s3_path_style_access: bool = True
    openid_client_id: str = "localinfra"
    openid_client_secret: str = "s3cr3t"
    openid_scope: str = "lakekeeper"
    warehouse_name: str = "e2e_tests"

    # trino
    trino_http_scheme: str = "http"
    trino_host: str = "localhost"
    trino_port: str = "58088"
    trino_user: str = "trino"
    trino_password: str = ""

    @property
    def lakekeeper_url(self) -> Endpoint:
        return Endpoint(f"http://{self.host_netloc}/iceberg", self.docker_netloc)

    @property
    def openid_provider_uri(self) -> Endpoint:
        return Endpoint(f"http://{self.host_netloc}/auth/realms/iceberg", self.docker_netloc)

    def storage_config(self) -> Dict[str, Any]:
        return {
            "warehouse-name": self.warehouse_name,
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": self.s3_access_key,
                "aws-secret-access-key": self.s3_secret_key,
            },
            "storage-profile": {
                "type": "s3",
                "bucket": self.s3_bucket,
                "key-prefix": "",
                "assume-role-arn": "",
                "endpoint": self.s3_endpoint,
                "region": self.s3_region,
                "path-style-access": self.s3_path_style_access,
                "flavor": "s3-compat",
                "sts-enabled": False,
            },
            "delete-profile": {"type": "hard"},
        }


class Server:
    def __init__(self, access_token: str, settings: Settings):
        self.access_token = access_token
        self.settings = settings

        # Bootstrap server once
        management_endpoint_v1 = self.management_endpoint(version=1)
        server_info = self._request_with_auth(
            requests.get,
            url=management_endpoint_v1 + "/info",
        )
        server_info.raise_for_status()
        server_info = server_info.json()
        if not server_info["bootstrapped"]:
            response = self._request_with_auth(
                requests.post,
                management_endpoint_v1 + "/bootstrap",
                json={"accept-terms-of-use": True},
            )
            response.raise_for_status()

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
        self, name: str, project_id: uuid.UUID, storage_config: dict
    ) -> "Warehouse":
        """Create a warehouse in this server"""

        payload = {
            "project-id": str(project_id),
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

        return Warehouse(
            self,
            name,
            uuid.UUID(warehouse_id),
            f"s3://{storage_config['storage-profile']['bucket']}",
        )

    @tenacity.retry(**_RETRY_ARGS)
    def purge_warehouse(self, warehouse: "Warehouse") -> None:
        """Purge all of the data in the given warehouse"""
        warehouse.purge()

    @tenacity.retry(**_RETRY_ARGS)
    def delete_warehouse(self, warehouse: "Warehouse") -> None:
        """Purge all of the data in the given warehouse and delete it"""
        response = self._request_with_auth(
            requests.delete, self.warehouse_endpoint() + f"/{str(warehouse.project_id)}"
        )
        response.raise_for_status()

    def _request_with_auth(self, requests_method: Callable, url: Endpoint, **kwargs):
        """Make a request, adding in the auth token"""
        headers = kwargs.setdefault("headers", {})
        headers.update({"Authorization": f"Bearer {self.access_token}"})
        kwargs.setdefault("timeout", 10.0)
        return requests_method(url=str(url), **kwargs)


@dataclasses.dataclass
class Warehouse:
    server: Server
    name: str
    project_id: uuid.UUID
    bucket_url: str

    def connect(self) -> PyIcebergCatalog:
        """Connect to the warehouse in the catalog"""
        creds = PyIcebergCatalogCredentials()
        creds.uri = str(self.server.catalog_endpoint())
        creds.warehouse = self.name
        creds.oauth2_server_uri = str(self.server.token_endpoint)
        creds.client_id = self.server.settings.openid_client_id
        creds.client_secret = self.server.settings.openid_client_secret
        creds.scope = self.server.settings.openid_scope
        return create_catalog(name="default", **creds.as_dict())

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


settings = Settings()


@pytest.fixture(scope="session")
def token_endpoint() -> str:
    response = requests.get(str(settings.openid_provider_uri + "/.well-known/openid-configuration"))
    response.raise_for_status()
    return response.json()["token_endpoint"]


@pytest.fixture(scope="session")
def access_token(token_endpoint: str) -> str:
    response = requests.post(
        token_endpoint,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.openid_client_id,
            "client_secret": settings.openid_client_secret,
            "scope": settings.openid_scope,
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


@pytest.fixture(scope="session")
def server(access_token: str) -> Server:
    return Server(access_token, settings)


@pytest.fixture(scope="session")
def project() -> uuid.UUID:
    return uuid.UUID("{00000000-0000-0000-0000-000000000000}")


@pytest.fixture(scope="session")
def warehouse(server: Server, project: uuid.UUID) -> Generator:
    if not settings.warehouse_name:
        raise ValueError("Empty 'warehouse_name' is not allowed.")

    storage_config = settings.storage_config()
    # Ensure bucket exists
    s3_hostname = urllib.parse.urlparse(storage_config["storage-profile"]["endpoint"]).netloc
    minio_client = Minio(
        s3_hostname,
        access_key=storage_config["storage-credential"]["aws-access-key-id"],
        secret_key=storage_config["storage-credential"]["aws-secret-access-key"],
        secure=False,
    )
    bucket_name = storage_config["storage-profile"]["bucket"]
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")

    warehouse = server.create_warehouse(settings.warehouse_name, project, storage_config)
    print(f"Warehouse {warehouse.project_id} created.")
    try:
        yield warehouse
    finally:

        @tenacity.retry(**_RETRY_ARGS)
        def _remove_bucket(bucket_name):
            minio_client.remove_bucket(bucket_name)

        try:
            server.purge_warehouse(warehouse)
            server.delete_warehouse(warehouse)
            _remove_bucket(bucket_name)

        except RuntimeError as exc:
            warnings.warn(
                f"Error deleting test warehouse '{str(warehouse.project_id)}'. It may need to be removed manually."
            )
            warnings.warn(f"Error:\n{str(exc)}")
