# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "authlib>=1.6,<=2",
#     "boto3>=1.42,<=2",
#     "click>=8.3,<9",
#     "python-keycloak>=7,<8",
#     "requests>=2.32,<3",
# ]
# ///
"""Combined script to bootstrap a Lakeeper instance and create a named warehouse.

Note: This script is used by both local, dev deployments and the ansible scripts.
"""

from collections import namedtuple
import json
from pathlib import Path
import logging
from typing import Any, Dict, Iterable, Sequence

from authlib.integrations.requests_client import OAuth2Session
import boto3
import botocore.exceptions
import click
from keycloak import KeycloakAdmin
import requests

# Prefix for environment variables mapped directly to class variables
LOGGER = logging.getLogger(__name__)
LOGGER_FILENAME = f"{Path(__file__).name}.log"
LOGGER_FORMAT = "%(asctime)s|%(message)s"

OIDC_PREFIX = "oidc~"


class LakekeeperRestV1:
    def __init__(self, url: str, auth_session: OAuth2Session):
        self._auth_session = auth_session
        self._url = url

    @property
    def catalog_url(self) -> str:
        return self._url + "/catalog/v1"

    @property
    def management_url(self) -> str:
        return self._url + "/management/v1"

    def assign_permissions(self, oidc_ids: Iterable[str], entities: Dict[str, Any]):
        """Assign a list of grants to a user"""
        for oidc_id in oidc_ids:
            LOGGER.debug(f"Assigning permissions for user {oidc_id}")
            for entity, grants in entities.items():
                self.assign_grants(oidc_id, entity, grants)

    def assign_grants(self, oidc_id: str, entity: str, grants: Iterable[str]):
        response = self._auth_session.get(
            self.management_url + f"/permissions/{entity}/assignments",
        )
        response.raise_for_status()
        if any(map(lambda x: x["user"] == oidc_id, response.json()["assignments"])):
            LOGGER.debug(
                f"Grants already assigned for entity '{entity}'. Skipping assignment."
            )
        else:
            response = self._auth_session.post(
                url=self.management_url + f"/permissions/{entity}/assignments",
                json={"writes": [{"type": grant, "user": oidc_id} for grant in grants]},
            )
            response.raise_for_status()

    def bootstrap(self):
        """Bootstrap the lakekeeper instance using the given access_token.

        Raises an excception if bootstrapping is unsuccessful."""
        if self.is_bootstrapped():
            LOGGER.info("Server already bootstrapped. Skipping bootstrap step.")
            return

        LOGGER.info("Bootstrapping server.")
        self._auth_session.request(
            "POST",
            self.management_url + "/bootstrap",
            json={
                "accept-terms-of-use": True,
                "is-operator": True,
            },
        )

        LOGGER.info("Server bootstrapped successfully.")

    def is_bootstrapped(self) -> bool:
        response = self._auth_session.get(self.management_url + "/info")
        response.raise_for_status()
        return response.json()["bootstrapped"]

    def rename_default_project(self, project_name: str):
        response = self._auth_session.get(
            self.management_url + "/project",
        )
        response.raise_for_status()
        LOGGER.debug(f"Current project info: {response.json()}")

        if response.json()["project-name"] != project_name:
            LOGGER.info(f"Renaming project to '{project_name}'")
            response = self._auth_session.post(
                self.management_url + "/project/rename",
                json={"new-name": project_name},
            )
            response.raise_for_status()

    def provision_user(self):
        """Provision a user through the /catalog/v1/config endpoint

        User details are retrieved from the access token
        """
        try:
            response = self._auth_session.get(self.catalog_url + "/config")
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            # Do not check the response status. If the user does not exist then a 400 is returned
            # but the user is provisioned internally.
            pass

    def get_warehouse_id(self, warehouse_name: str) -> str | None:
        response = self._auth_session.get(
            self.management_url + "/warehouse",
        )
        for warehouse in response.json()["warehouses"]:
            if warehouse["name"] == warehouse_name:
                return warehouse["id"]

        return None

    def create_warehouse(self, warehouse_config: Dict[str, Any]) -> str:
        """Create a warehouse in the server with the config.

        If the bucket does not exist it is created.
        """
        name = warehouse_config["warehouse-name"]
        id = self.get_warehouse_id(name)
        if id is not None:
            LOGGER.info(
                f"Warehouse '{name}' already exists. Skipping warehouse creation."
            )
            return id

        LOGGER.info(f"Creating warehouse '{name}'")
        _ensure_s3_bucket_exists(
            warehouse_config["storage-credential"], warehouse_config["storage-profile"]
        )
        response = self._auth_session.request(
            "POST",
            self.management_url + "/warehouse",
            json=warehouse_config,
        )
        response.raise_for_status()
        LOGGER.info(f"Warehouse '{name}' created successfully.")
        return response.json()["id"]


TwoTuple = namedtuple("TwoTuple", ["left", "right"])


class TwoTupleParamType(click.ParamType):
    """Accepts a string in the form 'abcd:efgh' and splits it a 2-namedtuple (left, right)"""

    name = "two_tuple"

    def convert(self, value, param, ctx):
        if isinstance(value, tuple):
            return value

        try:
            left, right = value.split(":")
            return TwoTuple(left, right)
        except ValueError:
            self.fail(
                f"{value!r} is not a valid credential string in the form 'abcd:efgh'",
                param,
                ctx,
            )


def oidc_user_id(kcadm: KeycloakAdmin, username: str) -> str:
    """Return the ID of the user prefixed with the string Lakekeeper expects"""
    return f"{OIDC_PREFIX}{kcadm.get_user_id(username)}"


def _ensure_s3_bucket_exists(
    storage_credential: Dict[str, Any], storage_profile: Dict[str, Any]
):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=storage_credential["aws-access-key-id"],
        aws_secret_access_key=storage_credential["aws-secret-access-key"],
        endpoint_url=storage_profile["endpoint"],
    )
    try:
        s3.create_bucket(Bucket=storage_profile["bucket"])
    except botocore.exceptions.ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def configure_logging(log_level: str):
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


def create_oauth2_session(
    keycloak_url: str,
    keycloak_user_realm: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> OAuth2Session:
    client = OAuth2Session(
        client_id=client_id,
        client_secret=client_secret,
        scope=f"openid {scope}",
        token_endpoint=f"{keycloak_url}/realms/{keycloak_user_realm}/protocol/openid-connect/token",
    )
    # Cache an access token
    client.fetch_access_token()
    return client


@click.command()
@click.argument("lakekeeper-url")
@click.option(
    "--lakekeeper-project-name",
    help="Project name",
)
@click.option("--keycloak-url", help="Base endpoint of Keycloak")
@click.option("--keycloak-user-realm", help="User realm for Lakekeeper users.")
@click.option(
    "--keycloak-admin-credentials",
    type=TwoTupleParamType(),
    help="Credentials for a user who has admin access to the KC master realm",
)
@click.option(
    "--bootstrap-credentials",
    type=TwoTupleParamType(),
    help="Credentials for user who will bootstrap lakekeeper",
)
@click.option("--token-scope", help="Additional scopes for required for token")
@click.option(
    "--server-admin",
    multiple=True,
    help="Human users assigned as server/project admin for UI access. Value should be username. Options can be provided multiple times.",
)
@click.option("--warehouse-json-file", help="JSON file for creating a warehouse")
@click.option("-l", "--log-level", default="INFO", show_default=True)
def main(
    lakekeeper_url: str,
    lakekeeper_project_name: str,
    keycloak_url: str,
    keycloak_user_realm: str,
    keycloak_admin_credentials: TwoTuple,
    bootstrap_credentials: TwoTuple,
    token_scope: str,
    warehouse_json_file: str,
    server_admin: Sequence[str],
    log_level: str,
):
    """Bootstrap Lakekeeper and create a list of warehouses.

    lakekeeper_url: Lakekeeper api base endpoint
    """
    configure_logging(log_level)
    auth_client = create_oauth2_session(
        keycloak_url,
        keycloak_user_realm,
        client_id=bootstrap_credentials.left,
        client_secret=bootstrap_credentials.right,
        scope=token_scope,
    )
    server = LakekeeperRestV1(lakekeeper_url, auth_client)
    server.bootstrap()
    server.rename_default_project(lakekeeper_project_name)

    # Server/project permissions
    kcadm = KeycloakAdmin(
        server_url=keycloak_url.rstrip("/") + "/",
        username=keycloak_admin_credentials.left,
        password=keycloak_admin_credentials.right,
    )
    kcadm.connection.refresh_token()  # force token request to master realm before switching target
    kcadm.change_current_realm(keycloak_user_realm)
    # Server admins
    server_admin_ids = [oidc_user_id(kcadm, username) for username in server_admin]
    server.assign_permissions(
        server_admin_ids,
        entities={
            "server": ["admin"],
            "project": ["project_admin"],
        },
    )

    LOGGER.debug(f"Creating warehouse using file: {warehouse_json_file}")
    with open(warehouse_json_file) as fp:
        warehouse_json = json.load(fp)
        # Permissions are not part of the spec to create the warehouse.
        # Remove and deal with them separately
        permissions = warehouse_json.pop("permissions", None)
        warehouse_id = server.create_warehouse(warehouse_json)

    # Warehouse access
    if permissions is not None:
        for username, user_permissions in permissions.items():
            server.assign_grants(
                oidc_user_id(kcadm, username),
                f"warehouse/{warehouse_id}",
                user_permissions,
            )


if __name__ == "__main__":
    main()
