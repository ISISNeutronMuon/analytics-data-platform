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
    """
    Return a list of local configuration providers that read settings from a `.dlt` directory next to this file instead of the global configuration.
    
    The returned providers are, in order:
    - environment variables (EnvironProvider)
    - Secrets.toml from the local `.dlt` directory
    - Config.toml from the local `.dlt` directory
    
    This function is intended to override global config discovery so configuration is loaded only from the local `.dlt` folder.
    """
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
        """
        Build and return a storage configuration dict for creating a warehouse.
        
        Returns a dictionary describing the warehouse storage, credentials, and deletion profile.
        The structure follows the storage API expected by the management service and includes:
        - "warehouse-name": the configured warehouse name.
        - "storage-credential": S3 access-key credential with `aws-access-key-id` and `aws-secret-access-key`.
        - "storage-profile": S3-compatible storage settings (bucket, endpoint, region, path-style access, etc.).
        - "delete-profile": deletion behavior (hard delete).
        
        Returns:
            Dict[str, Any]: Storage configuration ready to be sent to the warehouse creation endpoint.
        """
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


class Server:
    access_token: str
    lakekeeper_url: str
    openid_provider_uri: str
    openid_client_id: str
    openid_client_secret: str
    openid_scope: str

    def __init__(self, **kwargs):
        """
        Initialize the Server with OpenID and management endpoint settings and ensure the remote server is bootstrapped.
        
        Stores the following attributes from keyword arguments: access_token, server_url (lakekeeper_url), openid_provider_uri, openid_client_id, openid_client_secret, and openid_scope. After setting attributes, queries the server management /info endpoint; if the server is not yet bootstrapped, sends a POST to /bootstrap to accept terms of use and bootstrap the server.
        
        Parameters:
            **kwargs: Keyword arguments providing initialization values. Required keys:
                - access_token: OAuth2 access token used for management API authorization.
                - lakekeeper_url: Base URL of the lakekeeper server (used as management/catalog base).
                - openid_provider_uri: OpenID provider discovery URI.
                - openid_client_id: OpenID client identifier.
                - openid_client_secret: OpenID client secret.
                - openid_scope: OpenID scopes to request.
        
        Raises:
            requests.HTTPError: If any management API call (/info or /bootstrap) returns an HTTP error status.
        """
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
        """
        Return the base catalog API URL for this server.
        
        Returns:
            str: Full URL to the server's catalog endpoint (server_url + "/catalog").
        """
        return self.server_url + "/catalog"

    @property
    def management_api_url(self):
        """
        Return the base URL for the server's management API.
        
        Constructs and returns the management API root by appending "/management/v1" to the
        instance's `server_url`.
        
        Returns:
            str: Full management API base URL.
        """
        return self.server_url + "/management/v1"

    @property
    def token_endpoint(self):
        """
        Return the OpenID Connect token endpoint URL.
        
        Constructs the token endpoint by appending the standard OIDC token path to the server's configured OpenID provider URI and returns it as a string.
        
        Returns:
            str: Full token endpoint URL (e.g., "<openid_provider_uri>/protocol/openid-connect/token").
        """
        return self.openid_provider_uri + "/protocol/openid-connect/token"

    @property
    def warehouse_url(self):
        """
        Return the base warehouse API URL for this server.
        
        Appends "/warehouse" to the server's management API URL.
        
        Returns:
            str: Full warehouse endpoint base URL.
        """
        return self.management_api_url + "/warehouse"

    def create_warehouse(
        self, name: str, project_id: uuid.UUID, storage_config: dict
    ) -> "Warehouse":
        """
        Create a warehouse on this server.
        
        Sends a POST request to the server's warehouse management endpoint to create a warehouse for the given project using the provided storage configuration.
        
        Parameters:
            name: The warehouse name.
            project_id: UUID of the project that will own the warehouse.
            storage_config: Storage configuration dictionary passed through to the server; must include a 'storage-profile' with a 'bucket' entry used to construct the returned warehouse's S3 URL.
        
        Returns:
            Warehouse: A dataclass representing the created warehouse (contains server reference, name, project_id, and bucket_url).
        
        Raises:
            ValueError: If the server responds with a non-success HTTP status code.
        """

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
        """
        Delete a warehouse by its UUID on the server's management API.
        
        Sends an authenticated DELETE request to the server's warehouse endpoint to remove
        the specified warehouse.
        
        Parameters:
            warehouse_id (uuid.UUID): Identifier of the warehouse to delete.
        
        Raises:
            requests.HTTPError: If the HTTP response has a non-success status.
        """
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
    """
    Return the OpenID Connect token endpoint URL discovered from the configured provider.
    
    Queries the provider's discovery document at "<openid_provider_uri>/.well-known/openid-configuration"
    and returns the value of the "token_endpoint" field.
    
    Returns:
        str: The token endpoint URL.
    
    Raises:
        ValueError: If `settings.openid_provider_uri` is empty.
    """
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
    """
    Create a Server configured from the module-level Settings and the provided access token.
    
    Parameters:
        access_token (str): OAuth2 access token used for authenticated requests.
    
    Returns:
        Server: An initialized Server instance populated from the module settings and the given access token.
    """
    return Server(access_token=access_token, **settings.model_dump())


@pytest.fixture(scope="session")
def project() -> uuid.UUID:
    return uuid.UUID("{00000000-0000-0000-0000-000000000000}")


@pytest.fixture(scope="session")
def warehouse(server: Server, project: uuid.UUID) -> Generator:
    """
    Create a test warehouse and ensure its backing S3 bucket exists, yielding the created Warehouse for use in tests.
    
    This fixture:
    - Validates that a non-empty warehouse name is configured; raises ValueError if not.
    - Ensures the S3 bucket from settings.storage_config() exists (creates it if missing).
    - Calls the server to create a warehouse and yields the resulting Warehouse object.
    - On teardown, attempts to delete the warehouse and remove the bucket using retry/backoff; if cleanup fails, emits warnings with details.
    
    Parameters:
        project (uuid.UUID): ID of the project under which the warehouse is created.
    
    Yields:
        Warehouse: The created warehouse object.
    
    Raises:
        ValueError: If settings.warehouse_name is empty.
    """
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
