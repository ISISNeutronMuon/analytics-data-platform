import logging
import os

from celery.schedules import crontab
from flask_appbuilder.security.manager import AUTH_OAUTH
from flask_caching.backends.rediscache import RedisCache


logger = logging.getLogger()

#####
# Logging configuration used in Superset initialization
# The above logger object cannot be used to log at module level as logging as not been
# configured when this file is read. It can be used inside class methods of classes defined here
log_level_text = os.getenv("SUPERSET_LOG_LEVEL", "INFO")
LOG_LEVEL = getattr(logging, log_level_text.upper(), logging.INFO)

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
# disable recaptcha as we don't allow self registration
AUTH_USER_REGISTRATION = True
AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS = False
RECAPTCHA_PUBLIC_KEY = ""
RECAPTCHA_PRIVATE_KEY = ""

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION_ROLE = "Gamma"
AUTH_ROLES_SYNC_AT_LOGIN = True

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "superset",
            "api_base_url": "http://analytics.local:58080/auth/realms/iceberg/protocol/openid-connect",
            "client_kwargs": {"scope": "openid email profile"},
            "access_token_url": "http://analytics.local:58080/auth/realms/iceberg/protocol/openid-connect/token",
            "authorize_url": "http://analytics.local:58080/auth/realms/iceberg/protocol/openid-connect/auth",
            "server_metadata_url": "http://analytics.local:58080/auth/realms/iceberg/.well-known/openid-configuration",
            "request_token_url": None,
        },
    }
]


#####
# Caching layer
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_metadata_cache",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_charting_data_cache",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}
#####

#####
# SQL Lab
RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST, port=REDIS_PORT, key_prefix="superset_results_backend"
)
SQLLAB_CTAS_NO_LIMIT = True


#####
# Celery
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    imports = ("superset.sql_lab",)
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
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
    "https://{{ top_level_domain }}{os.environ.get('SUPERSET_APP_ROOT', '')}/"
)

#####

#####
# Theming.
# Workaround missing brand logo: https://github.com/apache/superset/pull/34935. Once merged this can
# be removed.
THEME_DEFAULT = {
    "token": {
        "brandLogoUrl": f"{os.environ.get('SUPERSET_APP_ROOT', '')}/static/assets/images/superset-logo-horiz.png",
    }
}


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
