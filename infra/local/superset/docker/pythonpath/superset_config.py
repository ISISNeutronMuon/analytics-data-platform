import logging
import os

from celery.schedules import crontab
from flask import g
from flask_appbuilder.security.manager import AUTH_OAUTH
from flask_caching.backends.rediscache import RedisCache
from superset.security import SupersetSecurityManager


logger = logging.getLogger()

#####
# Logging configuration used in Superset initialization
# The above logger object cannot be used to log at module level as logging as not been
# configured when this file is read. It can be used inside class methods of classes defined here
log_level_text = os.getenv("SUPERSET_LOG_LEVEL", "INFO")
LOG_LEVEL = getattr(logging, log_level_text.upper(), logging.INFO)
if LOG_LEVEL == logging.DEBUG:
    SILENCE_FAB = False

#####
# Supersets own database details
SUPERSET_DB_DIALECT = os.getenv("SUPERSET_DB_DIALECT")
SUPERSET_DB_USER = os.getenv("SUPERSET_DB_USER")
SUPERSET_DB_PASSWORD = os.getenv("SUPERSET_DB_PASSWORD")
SUPERSET_DB_HOST = os.getenv("SUPERSET_DB_HOST")
SUPERSET_DB_PORT = os.getenv("SUPERSET_DB_PORT")
SUPERSET_DB_NAME = os.getenv("SUPERSET_DB_NAME")
SUPERSET_DB_SCHEMA_NAME = os.getenv("SUPERSET_DB_SCHEMA_NAME")
# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = (
    f"{SUPERSET_DB_DIALECT}://"
    f"{SUPERSET_DB_USER}:{SUPERSET_DB_PASSWORD}@"
    f"{SUPERSET_DB_HOST}:{SUPERSET_DB_PORT}/{SUPERSET_DB_NAME}"
)
if SUPERSET_DB_SCHEMA_NAME:
    SQLALCHEMY_DATABASE_URI += f"?options=-c%20search_path={SUPERSET_DB_SCHEMA_NAME}"
#####

#####
# Auth


class CustomSsoSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):  # noqa: ARG002
        me = self.appbuilder.sm.oauth_remotes[provider].get("openid-connect/userinfo")
        me.raise_for_status()
        data = me.json()
        logging.debug("User info from Keycloak: %s", data)
        return {
            "username": data.get("preferred_username", ""),
            "first_name": data.get("given_name", ""),
            "last_name": data.get("family_name", ""),
            "email": data.get("email", ""),
            "role_keys": data.get("realm_access", {}).get("roles", []),
        }

    def load_user_jwt(self, _jwt_header, jwt_data):
        username = jwt_data["preferred_username"]
        user = self.find_user(username=username)
        if user is not None and user.is_active:
            # Set flask g.user to JWT user, we can't do it on before request
            g.user = user
            return user

        return None


CUSTOM_SECURITY_MANAGER = CustomSsoSecurityManager

# User registration is required to fill the Superset users table and happens automatically
# via Keycloak
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS = False
FAB_ADD_SECURITY_API = True

KEYCLOAK_REALM = os.environ["KEYCLOAK_REALM_INTERNAL"]
OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "superset",
            "client_kwargs": {
                "scope": "openid email profile roles",
            },
            "server_metadata_url": KEYCLOAK_REALM + "/.well-known/openid-configuration",
            "api_base_url": KEYCLOAK_REALM + "/protocol/",
        },
    }
]
AUTH_ROLES_MAPPING = {
    "adp_platform_admins": ["Admin"],
}
AUTH_USER_REGISTRATION_ROLE = "Gamma"
AUTH_ROLES_SYNC_AT_LOGIN = True


#####
# Caching layer
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
COMMON_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_DB,
}
CACHE_CONFIG = dict(**COMMON_CACHE_CONFIG, CACHE_KEY_PREFIX="superset_metadata_cache")
DATA_CACHE_CONFIG = dict(
    **COMMON_CACHE_CONFIG, CACHE_KEY_PREFIX="superset_charting_data_cache"
)
# SQL lab
RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, key_prefix="superset_results_backend"
)
SQLLAB_CTAS_NO_LIMIT = True


#####
# Celery
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    imports = ("superset.sql_lab",)
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=10, hour=0),
        },
    }


CELERY_CONFIG = CeleryConfig
WEBDRIVER_BASEURL = f"http://localhost:8088{os.environ.get('SUPERSET_APP_ROOT', '')}/"
# The base URL for the email report hyperlinks.
WEBDRIVER_BASEURL_USER_FRIENDLY = (
    f"https://localhost:50080{os.environ.get('SUPERSET_APP_ROOT', '')}/"
)


#####

#####
# Misc features
# fmt: off
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "TAGGING_SYSTEM": True,
    "CACHE_IMPERSONATION": True
}
# fmt: on
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True
SQLLAB_CTAS_NO_LIMIT = True
#####
