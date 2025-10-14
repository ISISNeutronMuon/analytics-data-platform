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
from typing import Any, Dict, Callable, Iterable

import click
import requests

# Prefix for environment variables mapped directly to class variables
LOGGER = logging.getLogger(__name__)
LOGGER_FILENAME = f"{Path(__file__).name}.log"
LOGGER_FORMAT = "%(asctime)s|%(message)s"

LAKEKEEPER_PROJECT_ID_DEFAULT = "00000000-0000-0000-0000-000000000000"
OIDC_PREFIX = "oidc~"
REQUESTS_TIMEOUT_DEFAULT = 60.0
REQUESTS_CA_BUNDLE = os.environ.get("LAKEKEEPER_BOOTSTRAP__REQUESTS_CA_BUNDLE")
REQUESTS_DEFAULT_KWARGS: Dict[str, Any] = {
    "timeout": REQUESTS_TIMEOUT_DEFAULT,
}
if REQUESTS_CA_BUNDLE is not None:
    REQUESTS_DEFAULT_KWARGS["verify"] = REQUESTS_CA_BUNDLE


def _request_with_auth(
    method: Callable, url: str, access_token: str | None, **kwargs
) -> requests.Response:
    """Make an authenticated request to the given url.

    A non-success response raises a requests.RequestException"""
    headers = kwargs.pop("headers", {})
    headers.update({"Authorization": f"Bearer {access_token}"} if access_token else {})
    response = method(url, headers=headers, **REQUESTS_DEFAULT_KWARGS, **kwargs)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        LOGGER.debug(f"request failed: {response.content.decode('utf-8')}")
        raise exc
    return response


@dataclasses.dataclass
class Keycloak:
    """Encapsulate Keycloak as an OpenID provider

    Keycloak should be accessible at "{url}" and the admin api at "{url}/admin"
    """

    url: str
    realm: str

    @property
    def realm_url(self) -> str:
        return self.url + f"/realms/{self.realm}"

    @property
    def realm_admin_url(self) -> str:
        return self.url + f"/admin/realms/{self.realm}"

    @property
    def openid_config(self) -> Dict[str, Any]:
        response = requests.get(self.realm_url + "/.well-known/openid-configuration")
        response.raise_for_status()
        return response.json()

    @property
    def token_endpoint(self) -> str:
        return self.openid_config["token_endpoint"]

    def oidc_ids(self, users: Iterable[str], access_token: str | None) -> Iterable[str]:
        oidc_users = []
        for user in users:
            response = _request_with_auth(
                requests.get,
                self.realm_admin_url + "/users",
                access_token,
                params={"username": user, "exact": True},
            )
            response.raise_for_status()
            users_json = response.json()
            if len(users_json) == 1:
                oidc_users.append(OIDC_PREFIX + users_json[0]["id"])
            elif len(users_json) == 0:
                raise RuntimeError(f"No user found for username={user}.")
            else:
                raise RuntimeError(
                    f"Multiple users ({len(users_json)}) found for username={user}."
                )

        return oidc_users

    def request_access_token(
        self, client_id: str, client_secret: str, scope: str
    ) -> str:
        """Request and return an access token from the given ID provider"""
        LOGGER.debug(f"Requesting access token from '{self.token_endpoint}'")
        response = requests.post(
            self.token_endpoint,
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


@dataclasses.dataclass
class Lakekeeper:
    url: str
    access_token: str | None

    @property
    def management_url(self) -> str:
        return self.url + "/management/v1"

    def assign_permissions(self, oidc_ids: Iterable[str], entities: Dict[str, Any]):
        """Assign a list of grants to a user"""
        for oidc_id in oidc_ids:
            LOGGER.debug(f"Assigning permissions for user {oidc_id}")
            for entity, grants in entities.items():
                self.assign_grants(oidc_id, entity, grants)

    def assign_grants(self, oidc_id: str, entity: str, grants: Iterable[str]):
        response = _request_with_auth(
            requests.get,
            self.management_url + f"/permissions/{entity}/assignments",
            self.access_token,
        )
        if any(map(lambda x: x["user"] == oidc_id, response.json()["assignments"])):
            LOGGER.debug(
                f"Grants already assigned for entity '{entity}'. Skipping assignment."
            )
        else:
            _request_with_auth(
                requests.post,
                url=self.management_url + f"/permissions/{entity}/assignments",
                access_token=self.access_token,
                json={"writes": [{"type": grant, "user": oidc_id} for grant in grants]},
            )

    def bootstrap(self):
        """Bootstrap the lakekeeper instance using the given access_token.

        Raises an excception if bootstrapping is unsuccessful."""
        if self.is_bootstrapped():
            LOGGER.info("Server already bootstrapped. Skipping bootstrap step.")
            return

        LOGGER.info("Bootstrapping server.")
        _request_with_auth(
            requests.post,
            url=self.management_url + "/bootstrap",
            access_token=self.access_token,
            json={
                "accept-terms-of-use": True,
                "is-operator": True,
            },
        )
        LOGGER.info("Server bootstrapped successfully.")

    def is_bootstrapped(self) -> bool:
        response = _request_with_auth(
            requests.get, self.management_url + "/info", self.access_token
        )
        return response.json()["bootstrapped"]

    def get_or_create_project(self, name: str) -> str:
        """Create a new project of the given name if one does not exist. Return the id"""
        project_id = self.get_project(name)
        if project_id is None:
            response = _request_with_auth(
                requests.post,
                url=self.management_url + "/project",
                access_token=self.access_token,
                json={"project-name": name},
            )
            response.raise_for_status()
            project_id = response.json()["project-id"]
            LOGGER.debug(f"Project '{name}' created with id '{project_id}'.")

        return project_id

    def get_project(self, name: str) -> str | None:
        """Get a project by name"""
        response = _request_with_auth(
            requests.get,
            self.management_url + "/project-list",
            self.access_token,
        )
        for warehouse in response.json()["projects"]:
            if warehouse["project-name"] == name:
                LOGGER.debug(f"Project with name '{name}' exists with'")
                return warehouse["project-id"]

        return None

    def warehouse_exists(self, project_id: str, warehouse_name: str) -> bool:
        response = _request_with_auth(
            requests.get,
            self.management_url + "/warehouse",
            self.access_token,
            headers={"x-project-id": project_id},
        )
        for warehouse in response.json()["warehouses"]:
            if warehouse["name"] == warehouse_name:
                return True

        return False

    def create_warehouse(self, project_id: str, warehouse_config: Dict[str, Any]):
        """Create a warehouse in the server with the given profile"""
        warehouse_name = warehouse_config["warehouse-name"]
        if self.warehouse_exists(project_id, warehouse_name):
            LOGGER.info(
                f"Warehouse '{warehouse_name}' already exists. Skipping warehouse creation."
            )
            return

        LOGGER.info(f"Creating warehouse '{warehouse_name}' in '{project_id}'")
        warehouse_config["project-id"] = project_id
        _request_with_auth(
            requests.post,
            self.management_url + "/warehouse",
            self.access_token,
            json=warehouse_config,
        )
        LOGGER.info(f"Warehouse '{warehouse_name}' created successfully.")


@click.command()
@click.argument("lakekeeper-url")
@click.option("--keycloak-url", help="Base endpoint of Keycloak")
@click.option("--keycloak-realm", help="Realm name to retrieve access token")
@click.option("--client-id", help="IDP client id")
@click.option("--client-secret", help="Secret for given client id")
@click.option("--token-scope", help="Additional scopes for token")
@click.option("--project-name", help="Project name other than the default")
@click.option(
    "--initial-admin",
    help="Usernames(s) assigned as warehouse/project admin as a comma-separated list.",
)
@click.option("--warehouse-json-file", help="JSON file for creating a warehouse")
@click.option("-l", "--log-level", default="INFO", show_default=True)
def main(
    lakekeeper_url: str,
    keycloak_url: str | None,
    keycloak_realm: str | None,
    client_id: str | None,
    client_secret: str | None,
    token_scope: str | None,
    project_name: str | None,
    initial_admin: str | None,
    warehouse_json_file: str | None,
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

    identity_provider, access_token = None, None
    idp_required_args = (
        "keycloak_url",
        "keycloak_realm",
        "client_id",
        "client_secret",
        "token_scope",
    )
    main_args = locals()
    if all(map(lambda x: main_args[x] is not None, idp_required_args)):
        LOGGER.debug("Creating identity provider.")
        identity_provider = Keycloak(keycloak_url, keycloak_realm)  # type: ignore
        access_token = identity_provider.request_access_token(
            client_id,  # type: ignore
            client_secret,  # type: ignore
            token_scope,  # type: ignore
        )
    else:
        LOGGER.debug(
            f"Skipping identity provider creation. Missing one of {idp_required_args}"
        )

    server = Lakekeeper(lakekeeper_url, access_token)
    server.bootstrap()
    if project_name is not None:
        project_id = server.get_or_create_project(project_name)
    else:
        project_id = LAKEKEEPER_PROJECT_ID_DEFAULT

    if initial_admin is not None and identity_provider is not None:
        users = map(lambda x: x.strip(), initial_admin.split(","))
        server.assign_permissions(
            identity_provider.oidc_ids(users, access_token),
            entities={"server": ["admin"], f"project/{project_id}": ["project_admin"]},
        )

    if warehouse_json_file is not None:
        LOGGER.debug(f"Creating warehouse using file: {warehouse_json_file}")
        with open(warehouse_json_file) as fp:
            warehouse_json = json.load(fp)
            server.create_warehouse(project_id, warehouse_json)


if __name__ == "__main__":
    main()
