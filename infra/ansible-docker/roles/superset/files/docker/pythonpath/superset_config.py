import logging
import os
from pathlib import Path
from typing import Dict, List

from celery.schedules import crontab
from flask import g, redirect, flash
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.security.forms import LoginForm_db
from flask_appbuilder.security.manager import AUTH_LDAP
from flask_appbuilder.security.sqla.models import Role
from flask_appbuilder.security.views import AuthLDAPView, expose
from flask_caching.backends.rediscache import RedisCache
from flask_login import login_user
from superset.security import SupersetSecurityManager
import yaml

logger = logging.getLogger("superset_config")

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
# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = (
    f"{SUPERSET_DB_DIALECT}://"
    f"{SUPERSET_DB_USER}:{SUPERSET_DB_PASSWORD}@"
    f"{SUPERSET_DB_HOST}:{SUPERSET_DB_PORT}/{SUPERSET_DB_NAME}"
)
#####

# Our own config options
SUPERSET_CONFIG_YAML_FILE = Path(__file__).parent / "superset_config.yml"
with open(SUPERSET_CONFIG_YAML_FILE) as fp:
    SUPERSET_CONFIG = yaml.safe_load(fp)
#####


class InternallyManagedAdminRoleSecurityManager(SupersetSecurityManager):
    # Override base method to consult our own list of mappings.

    @property
    def _admin_users(self) -> List[str]:
        return SUPERSET_CONFIG.get("admin_user_emails", [])

    def _ldap_calculate_user_roles(
        self, user_attributes: Dict[str, List[bytes]]
    ) -> List[Role]:
        logger.debug(
            f"Calculating role(s) for user with attributes '{user_attributes}'"
        )
        try:
            mail = user_attributes["mail"][0].decode("utf-8")
        except (IndexError, KeyError) as exc:
            logger.debug(f"Error retrieving mail attribute: {str(exc)}")
            mail = ""

        return (
            [self.find_role("Admin")]
            if mail in self._admin_users
            else [
                self.find_role(role_name)
                for role_name in AUTH_USER_REGISTRATION_ROLE_NAMES
            ]
        )


# LDAP authentication
SILENCE_FAB = False
AUTH_TYPE = AUTH_LDAP
AUTH_LDAP_SERVER = os.getenv("LDAP_SERVER")
AUTH_LDAP_USE_TLS = False
AUTH_LDAP_TLS_DEMAND = True
AUTH_LDAP_TLS_CACERTFILE = os.getenv("LDAP_CACERTFILE")
AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS = True

# registration configs
AUTH_USER_REGISTRATION = True
RECAPTCHA_PUBLIC_KEY = ""
RECAPTCHA_PRIVATE_KEY = ""
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION_ROLE_NAMES = SUPERSET_CONFIG.get(
    "auth_user_registration_role_names", ["Gamma"]
)
AUTH_LDAP_FIRSTNAME_FIELD = "givenName"
AUTH_LDAP_LASTNAME_FIELD = "sn"
AUTH_LDAP_EMAIL_FIELD = "mail"

# search configs using email (userPrincipalName in AD) as username
AUTH_LDAP_SEARCH = SUPERSET_CONFIG["auth_ldap_search"]
AUTH_LDAP_SEARCH_FILTER = SUPERSET_CONFIG["auth_ldap_search_filter"]
AUTH_LDAP_UID_FIELD = "userPrincipalName"
AUTH_LDAP_BIND_USER = "anonymous"
AUTH_LDAP_BIND_PASSWORD = os.getenv("LDAP_BIND_PASSWORD", "")

CUSTOM_SECURITY_MANAGER = InternallyManagedAdminRoleSecurityManager

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
    "TAGGING_SYSTEM": True
}
# fmt: on
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True
SQLLAB_CTAS_NO_LIMIT = True
#####
