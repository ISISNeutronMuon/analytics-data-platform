"""M365 credentials using pydantic-settings."""

from typing import ClassVar

from authlib.integrations.httpx_client import OAuth2Client
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class M365Credentials(BaseSettings):
    """Microsoft 365 OAuth2 client credentials.

    Reads from environment variables prefixed with
    ``SOURCES__M365__CREDENTIALS__``.
    """

    model_config = SettingsConfigDict(
        env_prefix="SOURCES__M365__CREDENTIALS__",
    )

    base_url: ClassVar[str] = "https://graph.microsoft.com"
    api_url: ClassVar[str] = f"{base_url}/v1.0"
    default_scope: ClassVar[str] = f"{base_url}/.default"
    login_url: ClassVar[str] = "https://login.microsoftonline.com"

    tenant_id: str
    client_id: str
    client_secret: SecretStr

    @property
    def authority_url(self) -> str:
        return f"{self.login_url}/{self.tenant_id}"

    @property
    def oauth2_token_endpoint(self) -> str:
        return f"{self.authority_url}/oauth2/v2.0/token"

    def create_oauth_client(self) -> OAuth2Client:
        return OAuth2Client(
            self.client_id,
            self.client_secret.get_secret_value(),
            token_endpoint=self.oauth2_token_endpoint,
            scope=self.default_scope,
            grant_type="client_credentials",
            follow_redirects=True,
        )
