import logging
import os
from typing import Dict, List

from celery.schedules import crontab
from flask import g, redirect, flash
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.security.forms import LoginForm_db
from flask_appbuilder.security.manager import AUTH_LDAP
from flask_appbuilder.security.sqla.models import Role
from flask_appbuilder.security.views import AuthLDAPView, expose
from flask_caching.backends.filesystemcache import FileSystemCache
from flask_login import login_user
from superset.security import SupersetSecurityManager


logger = logging.getLogger()

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

#####


class AuthLocalAndLDAPView(AuthLDAPView):
    """Implements an auth view that first uses LDAP to authorise
    a user and falls back to the internal database if necessary.

    This ensures we can have a local admin user should LDAP be
    unavailable.
    """

    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        if g.user is not None and g.user.is_authenticated:
            return redirect(self.appbuilder.get_url_for_index)
        form = LoginForm_db()
        if form.validate_on_submit():
            user = self.appbuilder.sm.auth_user_ldap(
                form.username.data, form.password.data
            )
            if not user:
                user = self.appbuilder.sm.auth_user_db(
                    form.username.data, form.password.data
                )
            if user:
                login_user(user, remember=False)
                return redirect(self.appbuilder.get_url_for_index)
            else:
                flash(as_unicode(self.invalid_login_message), "warning")
                return redirect(self.appbuilder.get_url_for_login)
        return self.render_template(
            self.login_template, title=self.title, form=form, appbuilder=self.appbuilder
        )


class InternallyManagedAdminRoleSecurityManager(SupersetSecurityManager):
    authldapview = AuthLocalAndLDAPView

    # Override base method to consult our own list of mappings.
    # Currently we simply maintain a list of Admin users and all others
    # are assigned the standard role
    ADMIN_USERS: List[bytes] = [b"martyn.gigg@stfc.ac.uk"]

    def _ldap_calculate_user_roles(
        self, user_attributes: Dict[str, bytes]
    ) -> List[Role]:
        try:
            mail = user_attributes["mail"][0]
        except (IndexError, KeyError):
            mail = ""

        logger.debug(f"Calculating role(s) for user with mail='{mail}'")
        return (
            [self.find_role("Admin")]
            if mail in self.ADMIN_USERS
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
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION_ROLE_NAMES = [
    "Gamma",
    "can_read_schema__isis.analytics_facility",
    "can_read_schema__isis.analytics_accelerator",
    "sql_lab",
]
AUTH_LDAP_FIRSTNAME_FIELD = "givenName"
AUTH_LDAP_LASTNAME_FIELD = "sn"
AUTH_LDAP_EMAIL_FIELD = "mail"

# search configs using email (userPrincipalName in AD) as username
AUTH_LDAP_SEARCH = "OU=FBU,DC=fed,DC=cclrc,DC=ac,DC=uk"
AUTH_LDAP_SEARCH_FILTER = (
    "(memberOf=CN=Isis,OU=Automatic Groups,DC=fed,DC=cclrc,DC=ac,DC=uk)"
)
AUTH_LDAP_UID_FIELD = "userPrincipalName"
AUTH_LDAP_BIND_USER = "anonymous"
AUTH_LDAP_BIND_PASSWORD = ""

CUSTOM_SECURITY_MANAGER = InternallyManagedAdminRoleSecurityManager

#####
# Caching layer
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_CELERY_DB = os.getenv("REDIS_CELERY_DB", "0")
REDIS_RESULTS_DB = os.getenv("REDIS_RESULTS_DB", "1")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_RESULTS_DB,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
#####

#####
# SQL Lab
RESULTS_BACKEND = FileSystemCache("/app/superset_home/sqllab")
SQLLAB_CTAS_NO_LIMIT = True
#####


#####
# Celery
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    imports = ("superset.sql_lab",)
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
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
