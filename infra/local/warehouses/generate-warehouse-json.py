# /// script
# requires-python = "==3.13.*"
# ///
"""Generate a Lakekeeper warehouse creation JSON document for a warehouse.

Writes the JSON for the named warehouse to stdout. Values are derived from the
warehouse name and environment variables so no static config files need to be
stored under ./warehouses/.

Bucket naming mirrors the previous static config: underscores in the warehouse
name become dashes (e.g. ``facility_ops`` -> ``facility-ops``). ``*_landing``
warehouses use an ``iceberg`` key-prefix; others use an empty prefix.

Usage:
    uv run generate-warehouse-json.py <warehouse_name>
"""

from __future__ import annotations

import json
import os
import sys


def build(name: str) -> dict:
    bucket = name.replace("_", "-")
    key_prefix = "iceberg" if name.endswith("_landing") else ""
    return {
        "warehouse-name": name,
        "storage-credential": {
            "type": "s3",
            "aws-access-key-id": os.environ["LOCAL_ADMIN_MACHINE"],
            "aws-secret-access-key": os.environ["LOCAL_PASSWORD"],
            "credential-type": "access-key",
        },
        "storage-profile": {
            "type": "s3",
            "bucket": bucket,
            "key-prefix": key_prefix,
            "endpoint": "http://adp-router:59000",
            "region": "local-01",
            "path-style-access": True,
            "sts-enabled": True,
            "flavor": "s3-compat",
        },
        "delete-profile": {"type": "hard"},
        "permissions": {"service-account-trino": ["select"]},
    }


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "Usage: generate-warehouse-json.py <warehouse_name>",
            file=sys.stderr,
        )
        return 1
    json.dump(build(argv[1]), sys.stdout, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
