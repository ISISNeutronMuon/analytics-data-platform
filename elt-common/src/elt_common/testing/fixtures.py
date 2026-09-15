"""A collection of utilities to support testing against this library"""

import tempfile
import time
import warnings
from collections.abc import Generator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import boto3
import botocore.exceptions
import pytest
import tenacity

from . import DEFAULT_RETRY_ARGS
from .dlt import PyIcebergDestinationTestConfiguration
from .lakekeeper import Server, Settings
from .sqlcatalog import SqlCatalogWarehouse


def _ensure_s3_bucket_exists(storage_credential: dict[str, Any], storage_profile: dict[str, Any]):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=storage_credential["aws-access-key-id"],
        aws_secret_access_key=storage_credential["aws-secret-access-key"],
        endpoint_url=storage_profile["endpoint"],
    )
    bucket = storage_profile["bucket"]
    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise

    return s3, bucket


@pytest.fixture(scope="session")
def warehouse(settings: Settings) -> Generator:
    if not settings.warehouse_name:
        raise ValueError("Empty 'warehouse_name' is not allowed.")

    if settings.catalog_type not in ("sql", "rest"):
        raise ValueError(
            f"Invalid catalog_type '{settings.catalog_type}'. Allowed values: sql, rest."
        )

    if settings.catalog_type == "sql":
        d = TemporaryDirectory()
        warehouse = SqlCatalogWarehouse(settings.warehouse_name, Path(d.name))

        def cleanup_func():
            d.cleanup()
    else:
        server = Server(settings)
        storage_config = settings.storage_config()

        s3, bucket_name = _ensure_s3_bucket_exists(
            storage_config["storage-credential"], storage_config["storage-profile"]
        )
        warehouse = server.create_warehouse(settings.warehouse_name, storage_config)

        def cleanup_func():
            @tenacity.retry(**DEFAULT_RETRY_ARGS)
            def _remove_bucket(bucket_name):
                s3.delete_bucket(Bucket=bucket_name)

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
