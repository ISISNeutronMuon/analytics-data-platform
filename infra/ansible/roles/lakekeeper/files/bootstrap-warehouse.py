# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "boto3~=1.42.24",
#     "click~=8.3.0",
#     "requests~=2.32.5",
# ]
# ///
"""Combined script to bootstrap a Lakeeper instance and create a named warehouse.

Note: This script is used by both local, dev deployments and the ansible scripts.
"""

from collections import namedtuple
import dataclasses
import json
from pathlib import Path
import logging
from typing import Any, Dict, Callable, Iterable, Sequence

import boto3
import botocore.exceptions
import click
import requests

# Prefix for environment variables mapped directly to class variables
LOGGER = logging.getLogger(__name__)
LOGGER_FILENAME = f"{Path(__file__).name}.log"
LOGGER_FORMAT = "%(asctime)s|%(message)s"

KEYCLOAK_MACHINE_USER_PREFIX = "service-account-"
OIDC_PREFIX = "oidc~"
REQUESTS_TIMEOUT_DEFAULT = 60.0
REQUESTS_DEFAULT_KWARGS: Dict[str, Any] = {
    "timeout": REQUESTS_TIMEOUT_DEFAULT,
}


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
        response = requests.get(
            self.realm_url + "/.well-known/openid-configuration",
            **REQUESTS_DEFAULT_KWARGS,
        )
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
        self, client_id: str, client_secret: str, scope: str | None
    ) -> str:
        """Request and return an access token from the given ID provider"""
        LOGGER.debug(f"Requesting access token from '{self.token_endpoint}'")
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if scope is not None:
            payload["scope"] = scope

        response = requests.post(
            self.token_endpoint,
            headers={"Content-type": "application/x-www-form-urlencoded"},
            data=payload,
            **REQUESTS_DEFAULT_KWARGS,
        )
        response.raise_for_status()
        return response.json()["access_token"]


@dataclasses.dataclass
class LakekeeperRestV1:
    url: str
    access_token: str | None

    @property
    def catalog_url(self) -> str:
        return self.url + "/catalog/v1"

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

    def rename_default_project(self, project_name: str):
        response = _request_with_auth(
            requests.post,
            url=self.management_url + "/project/rename",
            access_token=self.access_token,
            json={"new-name": project_name},
        )
        response.raise_for_status()

    def provision_user(self, access_token: str):
        """Provision a user through the /catalog/v1/config endpoint

        User details are retrieved from the access token
        """
        try:
            _request_with_auth(requests.get, self.catalog_url + "/config", access_token)
        except requests.exceptions.HTTPError:
            # Do not check the response status. If the user does not exist then a 400 is returned
            # but the user is provisioned internally.
            pass

    def warehouse_exists(self, warehouse_name: str) -> bool:
        response = _request_with_auth(
            requests.get,
            self.management_url + "/warehouse",
            self.access_token,
        )
        for warehouse in response.json()["warehouses"]:
            if warehouse["name"] == warehouse_name:
                return True

        return False

    def create_warehouse(self, warehouse_config: Dict[str, Any]):
        """Create a warehouse in the server with the given profile.

        If the bucket does not exist it is created.
        """
        warehouse_name = warehouse_config["warehouse-name"]
        if self.warehouse_exists(warehouse_name):
            LOGGER.info(
                f"Warehouse '{warehouse_name}' already exists. Skipping warehouse creation."
            )
            return

        LOGGER.info(f"Creating warehouse '{warehouse_name}'")
        _ensure_s3_bucket_exists(
            warehouse_config["storage-credential"], warehouse_config["storage-profile"]
        )
        _request_with_auth(
            requests.post,
            self.management_url + "/warehouse",
            self.access_token,
            json=warehouse_config,
        )
        LOGGER.info(f"Warehouse '{warehouse_name}' created successfully.")


Credential = namedtuple("Credential", ["client_id", "client_secret"])


class CredentialsParamType(click.ParamType):
    """Accepts a string in the form 'username:password' and splits it a 2-namedtuple (username, password)"""

    name = "credential"

    def convert(self, value, param, ctx):
        if isinstance(value, tuple):
            return value

        try:
            left, right = value.split(":")
            return Credential(left, right)
        except ValueError:
            self.fail(
                f"{value!r} is not a valid credential string in the form 'id:secret'",
                param,
                ctx,
            )


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
    except requests.exceptions.HTTPError:
        LOGGER.debug(f"request failed: {response.content.decode('utf-8')}")
        raise
    return response


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


@click.command()
@click.argument("lakekeeper-url")
@click.option(
    "--project-name",
    help="Project name other than the default",
)
@click.option("--keycloak-url", help="Base endpoint of Keycloak")
@click.option("--keycloak-realm", help="Realm name to retrieve access token")
@click.option(
    "--bootstrap-credentials",
    type=CredentialsParamType(),
    help="IDP client id & client secret in the format client_id:client_secret",
)
@click.option("--token-scope", help="Additional scopes for token")
@click.option(
    "--server-admin",
    multiple=True,
    help="Human users assigned as server/project admin for UI access. Value should be username. Options can be provided multiple times.",
)
@click.option(
    "--additional-user",
    type=CredentialsParamType(),
    multiple=True,
    help="Credentials for an additional user. Registers them but does not set permissions.",
)
@click.option("--warehouse-json-file", help="JSON file for creating a warehouse")
@click.option("-l", "--log-level", default="INFO", show_default=True)
def main(
    lakekeeper_url: str,
    project_name: str,
    keycloak_url: str | None,
    keycloak_realm: str | None,
    bootstrap_credentials: Credential | None,
    token_scope: str | None,
    server_admin: Sequence[str] | None,
    additional_user: Sequence[Credential] | None,
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
        "bootstrap_credentials",
        "token_scope",
    )
    main_args = locals()
    if all(map(lambda x: main_args[x] is not None, idp_required_args)):
        LOGGER.debug("Creating identity provider.")
        identity_provider = Keycloak(keycloak_url, keycloak_realm)  # type: ignore
        access_token = identity_provider.request_access_token(
            bootstrap_credentials.client_id,  # type: ignore
            bootstrap_credentials.client_secret,  # type: ignore
            token_scope,  # type: ignore
        )
    else:
        LOGGER.debug(
            f"Skipping identity provider creation. Missing one of {idp_required_args}"
        )

    server = LakekeeperRestV1(lakekeeper_url, access_token)
    server.bootstrap()

    if project_name is not None:
        server.rename_default_project(project_name)

    if server_admin is not None and identity_provider is not None:
        # Server admins are human users and will be provisioned the first time they log in
        server.assign_permissions(
            identity_provider.oidc_ids(server_admin, server.access_token),
            entities={"server": ["admin"]},
        )
        server.assign_permissions(
            identity_provider.oidc_ids(server_admin, server.access_token),
            entities={
                "server": ["admin"],
                "project": ["project_admin"],
            },
        )

    if additional_user is not None and identity_provider is not None:
        for user in additional_user:
            server.provision_user(
                identity_provider.request_access_token(
                    user.client_id, user.client_secret, token_scope
                )
            )

    if warehouse_json_file is not None:
        LOGGER.debug(f"Creating warehouse using file: {warehouse_json_file}")
        with open(warehouse_json_file) as fp:
            warehouse_json = json.load(fp)
            server.create_warehouse(warehouse_json)


if __name__ == "__main__":
    main()
