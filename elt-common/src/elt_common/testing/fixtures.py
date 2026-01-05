"""A collection of utilities to support testing against this library"""

import tempfile
import time
from typing import Generator
import urllib.parse
import warnings

from minio import Minio
import pytest
import requests
import tenacity

from . import DEFAULT_RETRY_ARGS
from .dlt import PyIcebergDestinationTestConfiguration
from .lakekeeper import Settings, Server, Warehouse


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def token_endpoint(settings: Settings) -> str:
    response = requests.get(str(settings.openid_provider_uri + "/.well-known/openid-configuration"))
    response.raise_for_status()
    return response.json()["token_endpoint"]


@pytest.fixture(scope="session")
def access_token(settings: Settings, token_endpoint: str) -> str:
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
def server(settings: Settings, access_token: str) -> Server:
    return Server(access_token, settings)


@pytest.fixture(scope="session")
def warehouse(settings: Settings, server: Server) -> Generator:
    if not settings.warehouse_name:
        raise ValueError("Empty 'warehouse_name' is not allowed.")

    storage_config = settings.storage_config()
    # Ensure bucket exists
    s3_hostname = urllib.parse.urlparse(storage_config["storage-profile"]["endpoint"]).netloc
    minio_client = Minio(
        endpoint=s3_hostname,
        access_key=storage_config["storage-credential"]["aws-access-key-id"],
        secret_key=storage_config["storage-credential"]["aws-secret-access-key"],
        secure=False,
    )
    bucket_name = storage_config["storage-profile"]["bucket"]
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        minio_client.make_bucket(bucket_name=bucket_name)
        print(f"Bucket {bucket_name} created.")

    warehouse = server.create_warehouse(
        settings.warehouse_name, server.settings.project_id, storage_config
    )
    print(f"Warehouse {warehouse.project_id} created.")
    try:
        yield warehouse
    finally:

        @tenacity.retry(**DEFAULT_RETRY_ARGS)
        def _remove_bucket(bucket_name):
            minio_client.remove_bucket(bucket_name=bucket_name)

        try:
            # Allow a brief pause for the test operations to complete
            time.sleep(1)
            server.purge_warehouse(warehouse)
            server.delete_warehouse(warehouse)
            _remove_bucket(bucket_name)

        except RuntimeError as exc:
            warnings.warn(
                f"Error deleting test warehouse '{str(warehouse.project_id)}'. It may need to be removed manually."
            )
            warnings.warn(f"Error:\n{str(exc)}")


@pytest.fixture
def destination_config(warehouse: Warehouse):
    destination_config = PyIcebergDestinationTestConfiguration(warehouse)
    try:
        yield destination_config
    finally:
        destination_config.clean_catalog()


@pytest.fixture
def pipelines_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir
