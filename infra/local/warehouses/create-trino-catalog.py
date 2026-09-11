# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "trino>=0.330,<1",
# ]
# ///
"""Create an Iceberg REST catalog in Trino.

Catalogs are created dynamically at bootstrap time
(Trino must be configured with ``catalog.management=dynamic``).

Usage:
    uv run create-trino-catalog.py <catalog_name>
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

# Quieten the InsecureRequestWarning noise from verify=False.
import urllib3
from trino.auth import BasicAuthentication
from trino.dbapi import connect
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

LOGGER = logging.getLogger(__name__)


def _get_connection():
    host = os.environ["ROUTER_HOSTNAME_INTERNAL"]
    port = int(os.environ.get("TRINO_HTTPS_PORT", "8443"))
    user = os.environ["ADMIN_USER"]
    password = os.environ["ADMIN_PASSWORD"]

    # TLS entrypoint for Trino on Traefik. Password auth requires HTTPS, so we
    # go through the router that terminates TLS. This runs inside the compose
    # network, so we use the router's in-container TLS port.
    # verify=False is needed due to self-signed local dev certificate.
    return connect(
        host=host,
        port=port,
        user=user,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
        verify=False,
    )


def _run_statement(sql: str) -> list[list[Any]]:
    """Execute a single SQL statement and return all data rows."""
    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        # The trino client handles the async REST polling, error handling, and
        # nextUri retrieval internally via the cursor.
        return cur.fetchall()


def catalog_exists(name: str) -> bool:
    rows = _run_statement("SHOW CATALOGS")
    return any(row and row[0] == name for row in rows)


def create_catalog_sql(name: str) -> str:
    """Build the CREATE CATALOG statement.

    Property names are double-quoted (they contain dashes); values are
    single-quoted varchars as required by Trino. Environment-derived values use
    ``${ENV:...}`` so the Trino coordinator resolves them.
    """
    # The landing warehouses share the same connector configuration; only the
    # Lakekeeper warehouse name differs, which equals the Trino catalog name.
    properties = {
        "iceberg.catalog.type": "rest",
        "iceberg.rest-catalog.warehouse": name,
        "iceberg.rest-catalog.uri": (
            "http://${ENV:ROUTER_HOSTNAME_INTERNAL}:"
            "${ENV:ROUTER_PORT_HTTP}/iceberg/catalog"
        ),
        "iceberg.rest-catalog.vended-credentials-enabled": "false",
        "iceberg.rest-catalog.security": "OAUTH2",
        "iceberg.rest-catalog.oauth2.server-uri": (
            "${ENV:KEYCLOAK_REALM_INTERNAL}/protocol/openid-connect/token"
        ),
        "iceberg.rest-catalog.oauth2.credential": "machine-infra:s3cr3t",
        "iceberg.rest-catalog.oauth2.scope": "lakekeeper",
        # Our local S3 implementation has STS enabled but our production S3
        # doesn't, so we keep vended credentials off and provide the S3 access
        # credentials here to mimic prod.
        "fs.native-s3.enabled": "true",
        "s3.endpoint": "http://${ENV:ROUTER_HOSTNAME_INTERNAL}:59000",
        "s3.region": "local-01",
        "s3.path-style-access": "true",
        "s3.aws-access-key": "${ENV:ADMIN_USER}",
        "s3.aws-secret-key": "${ENV:ADMIN_PASSWORD}",
    }
    with_clause = ",\n  ".join(
        f"\"{key}\" = '{value}'" for key, value in properties.items()
    )
    return f"CREATE CATALOG {name} USING iceberg\nWITH (\n  {with_clause}\n)"


def main(argv: list[str]) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s|%(levelname)s|%(message)s"
    )
    if len(argv) != 2:
        LOGGER.error("Usage: create-trino-catalog.py <catalog_name>")
        return 1

    name = argv[1]
    if catalog_exists(name):
        LOGGER.info("Trino catalog '%s' already exists. Skipping.", name)
        return 0

    LOGGER.info("Creating Trino catalog '%s'", name)
    _run_statement(create_catalog_sql(name))
    LOGGER.info("Trino catalog '%s' created successfully.", name)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
