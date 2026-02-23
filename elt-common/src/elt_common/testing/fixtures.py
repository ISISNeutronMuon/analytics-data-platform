"""A collection of utilities to support testing against this library"""

import tempfile
import time
from typing import Generator
import urllib.parse
import shutil
import warnings

from minio import Minio
import pytest
import tenacity

from . import DEFAULT_RETRY_ARGS
from .dlt import PyIcebergDestinationTestConfiguration
from .lakekeeper import Settings, Server
from .sqlcatalog import SqlCatalogWarehouse


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def warehouse(settings: Settings) -> Generator:
    if not settings.warehouse_name:
        raise ValueError("Empty 'warehouse_name' is not allowed.")

    if settings.catalog_type not in ("sql", "rest"):
        raise ValueError(
            f"Invalid catalog_type '{settings.catalog_type}'. Allowed values: sql, rest."
        )

    if settings.catalog_type == "sql":
        warehouse = SqlCatalogWarehouse(settings.warehouse_name)

        def cleanup_func():
            shutil.rmtree(warehouse.workdir.name)
    else:
        server = Server(settings)
        storage_config = settings.storage_config()
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

        warehouse = server.create_warehouse(settings.warehouse_name, storage_config)

        def cleanup_func():
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
                    f"Error deleting test warehouse '{str(warehouse.name)}'. It may need to be removed manually."
                )
                warnings.warn(f"Error:\n{str(exc)}")

    try:
        yield warehouse
    finally:
        cleanup_func()


@pytest.fixture
def destination_config(warehouse):
    destination_config = PyIcebergDestinationTestConfiguration(warehouse)
    try:
        yield destination_config
    finally:
        destination_config.clean_catalog()


@pytest.fixture
def pipelines_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir
