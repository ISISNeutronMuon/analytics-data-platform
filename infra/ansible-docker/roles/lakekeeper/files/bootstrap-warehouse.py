# /// script
# requires-python = "==3.13.*"
# dependencies = [
#      "click~=8.3.0",
#     "requests~=2.32.5",
# ]
# ///
"""Combined script to bootstrap a Lakeeper instance and create a named warehouse:

The following environment variables are optional:

- LAKEKEEPER_BOOTSTRAP__REQUESTS_CA_BUNDLE: A CA bundle as an alternative to certifi
"""

import dataclasses
import json
from pathlib import Path
import logging
import os
from typing import Any, Dict, Callable

import click
import requests

# Prefix for environment variables mapped directly to class variables
LOGGER = logging.getLogger(__name__)
LOGGER_FILENAME = f"{Path(__file__).name}.log"
LOGGER_FORMAT = "%(asctime)s|%(message)s"
REQUESTS_TIMEOUT_DEFAULT = 60.0
REQUESTS_CA_BUNDLE = os.environ.get("LAKEKEEPER_BOOTSTRAP__REQUESTS_CA_BUNDLE")
REQUESTS_DEFAULT_KWARGS: Dict[str, Any] = {
    "timeout": REQUESTS_TIMEOUT_DEFAULT,
}
if REQUESTS_CA_BUNDLE is not None:
    REQUESTS_DEFAULT_KWARGS["verify"] = REQUESTS_CA_BUNDLE


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

    def assign_permissions(self, oidc_id: str, entities: Dict[str, Any]):
        """Assign a list of grants to each user"""
        LOGGER.debug(f"Assigning permissions for user {oidc_id}")
        for entity, grants in entities.items():
            response = self._request_with_auth(
                requests.get,
                url=self.management_url + f"/permissions/{entity}/assignments",
            )
            if any(map(lambda x: x["user"] == oidc_id, response.json()["assignments"])):
                LOGGER.debug(
                    f"Grants already assigned for entity '{entity}'. Skipping assignment."
                )
                continue
            else:
                self._request_with_auth(
                    requests.post,
                    url=self.management_url + f"/permissions/{entity}/assignments",
                    json={
                        "writes": [{"type": grant, "user": oidc_id} for grant in grants]
                    },
                )

    def warehouse_exists(self, warehouse_name: str) -> bool:
        response = self._request_with_auth(
            requests.get, self.management_url + "/warehouse"
        )
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
    ) -> requests.Response:
        """Make an authenticated request to the given url.

        A non-success response raises a requests.RequestException"""
        response = method(
            url,
            headers={"Authorization": f"Bearer {self.access_token}"},
            json=json,
            **REQUESTS_DEFAULT_KWARGS,
        )
        response.raise_for_status()
        return response


def request_access_token(token_endpoint, client_id, client_secret, scope) -> str:
    """Request and return an access token from the given ID provider"""
    LOGGER.debug(f"Requesting access token from '{token_endpoint}'")
    response = requests.post(
        token_endpoint,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        headers={"Content-type": "application/x-www-form-urlencoded"},
        **REQUESTS_DEFAULT_KWARGS,
    )
    response.raise_for_status()
    return response.json()["access_token"]


@click.command()
@click.argument("lakekeeper-url")
@click.option("--token-url", help="Endpoint to retrieve a token")
@click.option("--client-id", help="IDP client id")
@click.option("--client-secret", help="Secret for given client id")
@click.option("--token-scope", help="Additional scopes for token")
@click.option("--initial-admin-id", help="OIDC ID of user initial admin/project_admin")
@click.option("--warehouse-json", help="JSON file for creating a warehouse")
@click.option("-l", "--log-level", default="INFO", show_default=True)
def main(
    lakekeeper_url: str,
    token_url: str,
    client_id: str,
    client_secret: str,
    token_scope: str,
    initial_admin_id: str,
    warehouse_json: str | None,
    log_level: str,
):
    """Bootstrap Lakekeeper and create a list of warehouses

    lakekeeper_url: Lakekeeper api base endpoint
    """

    this_file_dir = Path(__file__).parent
    logging.basicConfig(
        filename=this_file_dir / LOGGER_FILENAME,
        format=LOGGER_FORMAT,
        level=getattr(logging, log_level.upper()),
        filemode="a",
        encoding="utf-8",
        force=True,
    )
    stream_handler = logging.StreamHandler()
    logging.getLogger().addHandler(stream_handler)
    stream_handler.setFormatter(logging.Formatter(LOGGER_FORMAT))

    server = Server(
        lakekeeper_url,
        request_access_token(
            token_url,
            client_id,
            client_secret,
            token_scope,
        ),
    )
    server.bootstrap()
    server.assign_permissions(
        oidc_id=initial_admin_id,
        entities={"server": ["admin"], "project": ["project_admin"]},
    )

    if warehouse_json:
        LOGGER.debug(f"Creating warehouse using file: {warehouse_json}")
        with open(warehouse_json) as fp:
            server.create_warehouse(json.load(fp))


if __name__ == "__main__":
    main()
