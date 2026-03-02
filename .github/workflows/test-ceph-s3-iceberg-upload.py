#!/usr/bin/env -S uv run --script
# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "boto3",
#     "elt-common",
#     "pyiceberg[sql-sqlite]"
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../elt-common" }
# ///

import os
from pathlib import Path
import boto3
import botocore.exceptions

from pyiceberg.catalog import load_catalog
import pyarrow as pa

# create bucket
s3_access_key_id, s3_secret_access_key = (
    os.environ["CEPH_S3_ACCESS_KEY_ID"],
    os.environ["CEPH_S3_SECRET_ACCESS_KEY"],
)
s3_endpoint = os.environ["CEPH_S3_ENDPOINT"]
s3_region = os.environ["CEPH_S3_REGION"]
s3_bucket_name = Path(__file__).name
s3 = boto3.client(
    "s3",
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_secret_access_key,
    endpoint_url=s3_endpoint,
)
try:
    s3.create_bucket(Bucket=s3_bucket_name)
except botocore.exceptions.ClientError as exc:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        raise


# Example from https://py.iceberg.apache.org/api/#write-to-a-table
catalog = load_catalog(
    "default",
    **{
        "type": "sql",
        "uri": "sqlite:////tmp/pyiceberg_catalog.db",
        "warehouse": f"s3://{s3_bucket_name}",
        "s3.endpoint": s3_endpoint,
        "s3.region": s3_region,
        "s3.access-key-id": s3_access_key_id,
        "s3.secret-access-key": s3_secret_access_key,
        "s3.force-virtual-addressing": False,
    },
)
df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)

namespace, table_name = "testing", "cities"
catalog.create_namespace_if_not_exists(namespace)
table = catalog.create_table((namespace, table_name), df.schema)
table.append(df)
print(table.scan().to_arrow())
catalog.purge_table(
    (namespace, table_name),
)
catalog.drop_namespace(namespace)
