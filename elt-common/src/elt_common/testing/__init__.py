import dataclasses
import urllib.parse
from typing import Any, Dict

from pydantic_settings import BaseSettings, SettingsConfigDict
import tenacity

DEFAULT_RETRY_ARGS = {
    "wait": tenacity.wait_exponential(max=10),
    "stop": tenacity.stop_after_attempt(5),
    "reraise": True,
}


@dataclasses.dataclass
class Endpoint:
    raw_value: str
    internal_netloc: str

    def __add__(self, path: str) -> "Endpoint":
        return Endpoint(self.raw_value + path, self.internal_netloc)

    def __str__(self) -> str:
        return self.raw_value

    def value(self, *, use_internal_netloc: bool) -> str:
        if use_internal_netloc:
            fragments = list(urllib.parse.urlparse(self.raw_value))
            fragments[1] = self.internal_netloc
            return urllib.parse.urlunparse(fragments)
        else:
            return self.raw_value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="tests_",
    )

    # iceberg catalog
    # The default values assume the docker-compose.yml in the infra/local has been used.
    # These are provided for the convenience of easily running a debugger without having
    # to set up remote debugging
    host_netloc: str = "localhost:50080"
    docker_netloc: str = "adp-router:50080"
    s3_access_key: str = "adpsuperuser"
    s3_secret_key: str = "adppassword"
    s3_bucket: str = "e2e-tests"
    s3_endpoint: str = "http://adp-router:59000"
    s3_region: str = "local-01"
    s3_path_style_access: bool = True
    openid_client_id: str = "machine-infra"
    openid_client_secret: str = "s3cr3t"
    openid_scope: str = "lakekeeper"
    project_id: str = "c4fcd44f-7ce7-4446-9f7c-dcc7ba76dd22"
    warehouse_name: str = "e2e_tests"

    # trino
    trino_http_scheme: str = "https"
    trino_host: str = "localhost"
    trino_port: str = "58443"
    trino_user: str = "machine-infra"
    trino_password: str = "s3cr3t"

    @property
    def lakekeeper_url(self) -> Endpoint:
        return Endpoint(f"http://{self.host_netloc}/iceberg", self.docker_netloc)

    @property
    def openid_provider_uri(self) -> Endpoint:
        return Endpoint(
            f"http://{self.host_netloc}/auth/realms/analytics-data-platform", self.docker_netloc
        )

    def storage_config(self) -> Dict[str, Any]:
        return {
            "warehouse-name": self.warehouse_name,
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": self.s3_access_key,
                "aws-secret-access-key": self.s3_secret_key,
            },
            "storage-profile": {
                "type": "s3",
                "bucket": self.s3_bucket,
                "key-prefix": "",
                "assume-role-arn": "",
                "endpoint": self.s3_endpoint,
                "region": self.s3_region,
                "path-style-access": self.s3_path_style_access,
                "flavor": "s3-compat",
                "sts-enabled": False,
            },
            "delete-profile": {"type": "hard"},
        }
