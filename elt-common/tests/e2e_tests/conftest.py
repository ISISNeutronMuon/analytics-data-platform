import dataclasses
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
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
from minio import Minio, S3Error

from pydantic_settings import BaseSettings, SettingsConfigDict
import pytest
import requests
from retry.api import retry_call


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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="tests_")

    # The default values assume the docker-compose.yml in the infra/local has been used.
    # These are provided for the convenience of easily running a debugger without having
    # to set up remote debugging
    lakekeeper_url: Optional[str] = "http://localhost:58080/iceberg"
    s3_access_key: Optional[str] = "adpuser"
    s3_secret_key: Optional[str] = "adppassword"
    s3_bucket: Optional[str] = "e2e-tests-warehouse"
    s3_endpoint: Optional[str] = "http://localhost:59000"
    s3_region: Optional[str] = "local-01"
    s3_path_style_access: Optional[bool] = True
    openid_provider_uri: Optional[str] = "http://localhost:58080/auth/realms/iceberg"
    openid_client_id: Optional[str] = "localinfra"
    openid_client_secret: Optional[str] = "s3cr3t"
    openid_scope: Optional[str] = "lakekeeper"
    warehouse_name: Optional[str] = "e2e_tests"

    @property
    def catalog_url(self) -> str:
        return f"{self.lakekeeper_url}/catalog"

    @property
    def management_url(self) -> str:
        return f"{self.lakekeeper_url}/management"

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
    access_token: str
    lakekeeper_url: str
    openid_provider_uri: str
    openid_client_id: str
    openid_client_secret: str
    openid_scope: str

    def __init__(self, **kwargs):
        self.access_token = kwargs["access_token"]
        self.server_url = kwargs["lakekeeper_url"]
        self.openid_provider_uri = kwargs["openid_provider_uri"]
        self.openid_client_id = kwargs["openid_client_id"]
        self.openid_client_secret = kwargs["openid_client_secret"]
        self.openid_scope = kwargs["openid_scope"]

        # Bootstrap server once
        access_token = kwargs["access_token"]
        server_info = requests.get(
            self.management_api_url + "/info", headers={"Authorization": f"Bearer {access_token}"}
        )
        server_info.raise_for_status()
        server_info = server_info.json()
        if not server_info["bootstrapped"]:
            response = requests.post(
                self.management_api_url + "/bootstrap",
                headers={"Authorization": f"Bearer {access_token}"},
                json={"accept-terms-of-use": True},
            )
            response.raise_for_status()

    @property
    def catalog_url(self):
        return self.server_url + "/catalog"

    @property
    def management_api_url(self):
        return self.server_url + "/management/v1"

    @property
    def token_endpoint(self):
        return self.openid_provider_uri + "/protocol/openid-connect/token"

    @property
    def warehouse_url(self):
        return self.management_api_url + "/warehouse"

    def create_warehouse(
        self, name: str, project_id: uuid.UUID, storage_config: dict
    ) -> "Warehouse":
        """Create a warehouse in this server"""

        payload = {
            "project-id": str(project_id),
            "warehouse-name": name,
            **storage_config,
        }

        response = requests.post(
            self.warehouse_url,
            json=payload,
            headers={"Authorization": f"Bearer {self.access_token}"},
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

    def delete_warehouse(self, warehouse_id: uuid.UUID) -> None:
        response = requests.delete(
            self.warehouse_url + f"/{str(warehouse_id)}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        response.raise_for_status()


@dataclasses.dataclass
class Warehouse:
    server: Server
    name: str
    project_id: uuid.UUID
    bucket_url: str


settings = Settings()


@pytest.fixture(scope="session")
def token_endpoint() -> str:
    if not settings.openid_provider_uri:
        raise ValueError("Empty 'openid_provider_uri' is not allowed.")

    return requests.get(settings.openid_provider_uri + "/.well-known/openid-configuration").json()[
        "token_endpoint"
    ]


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
    return Server(access_token=access_token, **settings.model_dump())


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
        try:
            retry_args = {
                "delay": 2,
                "tries": 100,
                "backoff": 1.7,
                "max_delay": 15,
            }
            retry_call(server.delete_warehouse, fargs=(warehouse.project_id,), **retry_args)
            retry_call(
                minio_client.remove_bucket, fargs=(bucket_name,), exceptions=S3Error, **retry_args
            )
        except Exception as exc:
            warnings.warn(
                f"Error deleting test warehouse '{str(warehouse.project_id)}'. It may need to be removed manually."
            )
            warnings.warn(f"Error:\n{str(exc)}")
