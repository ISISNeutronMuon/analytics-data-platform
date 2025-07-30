import logging
import os

from celery.schedules import crontab
from flask_caching.backends.filesystemcache import FileSystemCache


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

# disable recaptcha as we don't allow self registration
AUTH_USER_REGISTRATION = False
AUTH_ROLES_SYNC_AT_LOGIN = False
AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS = False
RECAPTCHA_PUBLIC_KEY = ""
RECAPTCHA_PRIVATE_KEY = ""

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
