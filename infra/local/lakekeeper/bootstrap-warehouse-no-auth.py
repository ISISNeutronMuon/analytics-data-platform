# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests",
# ]
# ///
"""Combined script to bootstrap a Lakeeper instance and create a named warehouse:

The following environment variables are required:

- LAKEKEEPER_BOOTSTRAP__LAKEKEEPER_URL
- LAKEKEEPER_BOOTSTRAP__WAREHOUSE_JSON_FILE
"""

import dataclasses
import json
from pathlib import Path
import logging
import os
from typing import Any, Dict, Callable

import requests

# Prefix for environment variables mapped directly to class variables
LAKEKEEPER_BOOTSTRAP_PREFIX = "LAKEKEEPER_BOOTSTRAP__"
LOGGER = logging.getLogger(__name__)
LOGGER_FILENAME = f"{Path(__file__).name}.log"


@dataclasses.dataclass(init=False)
class EnvParser:
    # Lakekeeper
    lakekeeper_url: str
    warehouse_json_file: str

    def __init__(self, env_prefix: str):
        """Parse environment variables prefixed with env_prefix into class variables"""
        for field in dataclasses.fields(self):
            setattr(self, field.name, os.environ[f"{env_prefix}{field.name.upper()}"])


@dataclasses.dataclass
class Server:
    url: str

    @property
    def management_url(self) -> str:
        return self.url + "/management/v1"

    def is_bootstrapped(self) -> bool:
        response = self._request(requests.get, self.management_url + "/info")
        return response.json()["bootstrapped"]

    def bootstrap(self):
        """Bootstrap the lakekeeper instance using the given access_token.

        Raises an excception if bootstrapping is unsuccessful."""
        if self.is_bootstrapped():
            LOGGER.info("Server already bootstrapped. Skipping bootstrap step.")
            return

        LOGGER.info("Bootstrapping server.")
        self._request(
            requests.post,
            url=self.management_url + "/bootstrap",
            json={
                "accept-terms-of-use": True,
                "is-operator": True,
            },
        )
        LOGGER.info("Server bootstrapped successfully.")

    def warehouse_exists(self, warehouse_name: str) -> bool:
        response = self._request(requests.get, self.management_url + "/warehouse")
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
        self._request(
            requests.post, self.management_url + "/warehouse", json=warehouse_config
        )
        LOGGER.info(f"Warehouse '{warehouse_name}' created successfully.")

    def _request(self, method: Callable, url: str, *, json=None) -> requests.Response:
        """Make an authenticated request to the given url.

        A non-success response raises a requests.RequestException"""
        response = method(url, json=json)
        response.raise_for_status()
        return response


def main():
    logging.basicConfig(level=logging.INFO)
    env_vars = EnvParser(LAKEKEEPER_BOOTSTRAP_PREFIX)
    server = Server(env_vars.lakekeeper_url)
    server.bootstrap()
    with open(env_vars.warehouse_json_file) as fp:
        server.create_warehouse(json.load(fp))


if __name__ == "__main__":
    main()
