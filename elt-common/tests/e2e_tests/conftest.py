import dataclasses
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.runtime.run_context import RunContext

import pytest
import requests

from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # The default values assume the docker-compose.yml in the parent directory has been used.
    # These are provided for the convenience of easily running a debugger without having
    # to set up remote debugging
    lakekeeper_url: Optional[str] = "http://lakekeeper:8181"
    s3_access_key: Optional[str] = "minio-root-user"
    s3_secret_key: Optional[str] = "minio-root-password"
    s3_bucket: Optional[str] = "e2e-tests-warehouse"
    s3_endpoint: Optional[str] = "http://minio:9000"
    s3_region: Optional[str] = "local-01"
    s3_path_style_access: Optional[bool] = True
    openid_provider_uri: Optional[str] = "http://keycloak:8080/realms/iceberg"
    openid_client_id: Optional[str] = "e2e_tests"
    openid_client_secret: Optional[str] = "xspCqQ2YbUg8sIhf0MepQv5DhYKSUZxA"
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
                "bucket": settings.s3_bucket,
                "key-prefix": "",
                "assume-role-arn": "",
                "endpoint": settings.s3_endpoint,
                "region": settings.s3_region,
                "path-style-access": settings.s3_path_style_access,
                "flavor": "s3-compat",
                "sts-enabled": False,
            },
            "delete-profile": {"type": "hard"},
        }


@dataclasses.dataclass
class Server:
    settings: Settings
    token_endpoint: str
    access_token: str

    @property
    def warehouse_url(self):
        return self.settings.management_url + "/v1/warehouse"

    def create_warehouse(self, name: str, project_id: uuid.UUID, storage_config: dict) -> uuid.UUID:
        """Create a warehouse in this server"""

        create_payload = {
            "project-id": str(project_id),
            "warehouse-name": name,
            **storage_config,
            "delete-profile": {"type": "soft", "expiration-seconds": 2},
        }

        response = requests.post(
            self.warehouse_url,
            json=create_payload,
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
        return uuid.UUID(warehouse_id)

    def delete_warehouse(self, warehouse_id: uuid.UUID) -> None:
        response = requests.delete(
            self.settings.management_url + f"/v1/warehouse/{str(warehouse_id)}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        try:
            response.raise_for_status()
            print(f"Warehouse {str(warehouse_id)} deleted.")
        except Exception:
            raise ValueError(
                f"Failed to delete warehouse ({response.status_code}): {response.text}."
            )


@dataclasses.dataclass
class Warehouse:
    server: Server
    name: str
    bucket_url: str


settings = Settings()


@pytest.fixture(scope="session")
def token_endpoint() -> str:
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
def server(token_endpoint: str, access_token: str) -> Server:
    # Bootstrap server once
    management_url = settings.management_url.rstrip("/") + "/"
    server_info = requests.get(
        management_url + "v1/info", headers={"Authorization": f"Bearer {access_token}"}
    )
    server_info.raise_for_status()
    server_info = server_info.json()
    if not server_info["bootstrapped"]:
        response = requests.post(
            management_url + "v1/bootstrap",
            headers={"Authorization": f"Bearer {access_token}"},
            json={"accept-terms-of-use": True},
        )
        response.raise_for_status()

    return Server(settings, token_endpoint, access_token)


@pytest.fixture(scope="session")
def project() -> uuid.UUID:
    return uuid.UUID("{00000000-0000-0000-0000-000000000000}")


@pytest.fixture(scope="session")
def warehouse(server: Server, project: uuid.UUID) -> Warehouse:
    storage_config = settings.storage_config()
    warehouse_uuid = server.create_warehouse(settings.warehouse_name, project, storage_config)
    print(f"Warehouse {warehouse_uuid} created.")
    try:
        yield Warehouse(
            server,
            settings.warehouse_name,
            bucket_url=f"s3://{storage_config['storage-profile']['bucket']}",
        )
    finally:
        try:
            server.delete_warehouse(warehouse_uuid)
        except ValueError as exc:
            print(
                f"Warning: Error deleting test warehouse '{str(warehouse_uuid)}'. It may need to be removed manually."
            )
            print(f"Error:\n{str(exc)}")
