# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "requests>=2.32,<3",
# ]
# ///
"""Create an Iceberg REST catalog in Trino using the Trino client REST API.

Replaces the static per-warehouse ``.properties`` files that were previously
mounted into the Trino container. Catalogs are created dynamically at bootstrap
time (Trino must be configured with ``catalog.management=dynamic``).

The catalog properties mirror the previous ``warehouses/trino/*.properties``
files. Sensitive/environment-derived values are passed through as
``${ENV:...}`` so they are resolved by the Trino coordinator (which already has
these variables set via its env_file), keeping secrets out of this process.

Usage:
    uv run create-trino-catalog.py <catalog_name>
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)

# Retryable HTTP statuses per the Trino client protocol.
_RETRY_STATUSES = frozenset({429, 502, 503, 504})
_MAX_ATTEMPTS = 60


def _statement_url() -> str:
    host = os.environ["ROUTER_HOSTNAME_INTERNAL"]
    # TLS entrypoint for Trino on Traefik. Password auth requires HTTPS, so we
    # go through the router that terminates TLS. This runs inside the compose
    # network, so we use the router's in-container TLS port (TRINO_HTTPS_PORT,
    # 8443), not the host-published port (58443) used by trino-execute.sh.
    port = os.environ.get("TRINO_HTTPS_PORT", "8443")
    return f"https://{host}:{port}/v1/statement"


def _auth() -> tuple[str, str]:
    return os.environ["TRINO_USER"], os.environ["TRINO_PASSWORD"]


def _run_statement(sql: str) -> list[list[Any]]:
    """Execute a single SQL statement and return all data rows.

    Follows ``nextUri`` until the query completes and raises on any query error.
    """
    session = requests.Session()
    session.auth = _auth()
    session.verify = False  # self-signed local dev certificate

    headers = {"X-Trino-User": os.environ["TRINO_USER"]}
    rows: list[list[Any]] = []

    # Issue the initial POST, then follow nextUri with GETs until completion.
    def _post() -> requests.Response:
        return session.post(_statement_url(), data=sql.encode(), headers=headers)

    do_request = _post
    attempt = 0
    while True:
        response = do_request()

        if response.status_code in _RETRY_STATUSES:
            attempt += 1
            if attempt >= _MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Trino kept returning {response.status_code} after "
                    f"{_MAX_ATTEMPTS} attempts"
                )
            time.sleep(float(response.headers.get("Retry-After", "0.1")))
            continue  # retry the same request

        if response.status_code != 200:
            raise RuntimeError(
                f"Trino request failed ({response.status_code}): {response.text}"
            )

        attempt = 0
        payload = response.json()
        if "error" in payload:
            error = payload["error"]
            raise RuntimeError(
                f"Trino query error: {error.get('message')} "
                f"(errorCode={error.get('errorCode')})"
            )

        if payload.get("data"):
            rows.extend(payload["data"])

        next_uri = payload.get("nextUri")
        if not next_uri:
            return rows

        do_request = lambda uri=next_uri: session.get(uri)  # noqa: E731


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
        "s3.aws-access-key": "${ENV:ADP_SUPERUSER}",
        "s3.aws-secret-key": "${ENV:ADP_SUPERUSER_PASS}",
    }
    with_clause = ",\n  ".join(
        f'"{key}" = \'{value}\'' for key, value in properties.items()
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
    # Quieten the InsecureRequestWarning noise from verify=False.
    requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
    sys.exit(main(sys.argv))
