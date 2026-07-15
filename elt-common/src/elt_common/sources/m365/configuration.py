from authlib.integrations.httpx_client import OAuth2Client
from pydantic import SecretStr, BaseModel
from pydantic_settings import BaseSettings

base_url = "https://graph.microsoft.com"
_default_scope = f"{base_url}/.default"
_login_url = "https://login.microsoftonline.com"


class M365Credentials(BaseModel):
    """Configuration class for M365 credentials"""

    tenant_id: str
    client_id: str
    client_secret: SecretStr

    @property
    def _authority_url(self) -> str:
        return f"{_login_url}/{self.tenant_id}"

    @property
    def _oauth2_token_endpoint(self) -> str:
        return f"{self._authority_url}/oauth2/v2.0/token"

    def create_oauth_client(self) -> OAuth2Client:
        return OAuth2Client(
            self.client_id,
            self.client_secret.get_secret_value(),
            token_endpoint=self._oauth2_token_endpoint,
            scope=_default_scope,
            grant_type="client_credentials",
            follow_redirects=True,
        )


class M365Config(BaseSettings):
    site_url: str
    credentials: M365Credentials
