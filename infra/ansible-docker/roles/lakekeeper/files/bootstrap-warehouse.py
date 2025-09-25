# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "requests",
# ]
# ///
"""Combined script to bootstrap a Lakeeper instance and create a named warehouse:

The following environment variables are required:

- LAKEKEEPER_BOOTSTRAP__TOKEN_ENDPOINT_URL
- LAKEKEEPER_BOOTSTRAP__CLIENT_ID
- LAKEKEEPER_BOOTSTRAP__CLIENT_SECRET
- LAKEKEEPER_BOOTSTRAP__SCOPE
- LAKEKEEPER_BOOTSTRAP__LAKEKEEPER_URL
"""

import dataclasses
import json
from pathlib import Path
import logging
import os
import sys
from typing import Any, Dict, Callable

import requests

# Prefix for environment variables mapped directly to class variables
LAKEKEEPER_BOOTSTRAP_PREFIX = "LAKEKEEPER_BOOTSTRAP__"
LOGGER = logging.getLogger(__name__)
LOGGER_FILENAME = f"{Path(__file__).name}.log"
LOGGER_FORMAT = "%(asctime)s|%(message)s"
REQUESTS_TIMEOUT_DEFAULT = 60.0


@dataclasses.dataclass(init=False)
class EnvParser:
    # Logging
    log_level: str = "INFO"

    # ID provider
    token_endpoint_url: str
    client_id: str
    client_secret: str
    scope: str

    # Lakekeeper
    lakekeeper_url: str

    def __init__(self, env_prefix: str):
        """Parse environment variables prefixed with env_prefix into class variables"""
        for field in dataclasses.fields(self):
            setattr(self, field.name, os.environ[f"{env_prefix}{field.name.upper()}"])


@dataclasses.dataclass
class Server:
    url: str
    access_token: str

    @property
    def management_url(self) -> str:
        return self.url + "/management/v1"

    def is_bootstrapped(self) -> bool:
        response = self._request_with_auth(requests.get, self.management_url + "/info")
        return response.json()["bootstrapped"]

    def bootstrap(self):
        """Bootstrap the lakekeeper instance using the given access_token.

        Raises an excception if bootstrapping is unsuccessful."""
        if self.is_bootstrapped():
            LOGGER.info("Server already bootstrapped. Skipping bootstrap step.")
            return

        LOGGER.info("Bootstrapping server.")
        self._request_with_auth(
            requests.post,
            url=self.management_url + "/bootstrap",
            json={
                "accept-terms-of-use": True,
                "is-operator": True,
            },
        )
        LOGGER.info("Server bootstrapped successfully.")

    def warehouse_exists(self, warehouse_name: str) -> bool:
        response = self._request_with_auth(
            requests.get, self.management_url + "/warehouse"
        )
        LOGGER.debug(f"'/warehouse' response: {response.json()}")
        for warehouse in response.json()["warehouses"]:
            if warehouse["name"] == warehouse_name:
                return True

        return False

    def create_warehouse(self, warehouse_config: Dict[str, Any]):
        """Create a warehouse in the server with the given profile"""
        warehouse_name = warehouse_config["warehouse-name"]
        if self.warehouse_exists(warehouse_name):
            LOGGER.info(
                f"Warehouse '{warehouse_name}' already exists. Skipping warehouse creation."
            )
            return

        LOGGER.info(f"Creating warehouse '{warehouse_name}'")
        self._request_with_auth(
            requests.post, self.management_url + "/warehouse", json=warehouse_config
        )
        LOGGER.info(f"Warehouse '{warehouse_name}' created successfully.")

    def _request_with_auth(
        self,
        method: Callable,
        url: str,
        *,
        json=None,
        timeout: float = REQUESTS_TIMEOUT_DEFAULT,
    ) -> requests.Response:
        """Make an authenticated request to the given url.

        A non-success response raises a requests.RequestException"""
        response = method(
            url,
            headers={"Authorization": f"Bearer {self.access_token}"},
            json=json,
            timeout=timeout,
        )
        response.raise_for_status()
        return response


def request_access_token(token_endpoint, client_id, client_secret, scope) -> str:
    """Request and return an access token from the given ID provider"""
    response = requests.post(
        token_endpoint,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": scope,
            "scope": scope,
        },
        headers={"Content-type": "application/x-www-form-urlencoded"},
        timeout=REQUESTS_TIMEOUT_DEFAULT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def main():
    env_vars = EnvParser(LAKEKEEPER_BOOTSTRAP_PREFIX)

    this_file_dir = Path(__file__).parent
    logging.basicConfig(
        filename=this_file_dir / LOGGER_FILENAME,
        format=LOGGER_FORMAT,
        level=getattr(logging, env_vars.log_level),
        filemode="a",
        encoding="utf-8",
        force=True,
    )
    stream_handler = logging.StreamHandler()
    logging.getLogger().addHandler(stream_handler)
    stream_handler.setFormatter(logging.Formatter(LOGGER_FORMAT))

    server = Server(
        env_vars.lakekeeper_url,
        request_access_token(
            env_vars.token_endpoint_url,
            env_vars.client_id,
            env_vars.client_secret,
            env_vars.scope,
        ),
    )
    server.bootstrap()

    warehouses_json = sys.argv[1:]
    if warehouses_json:
        LOGGER.debug(f"Creating warehouses using files: {warehouses_json}")
        for warehouse_json_file in warehouses_json:
            with open(warehouse_json_file) as fp:
                server.create_warehouse(json.load(fp))


if __name__ == "__main__":
    main()
