import os

from superset_config_auth import *  # noqa: E402, F403
from superset_config_caching import *  # noqa: E402, F403
from superset_config_celery import *  # noqa: E402, F403
from superset_config_db import postgres_uri  # noqa: E402, F403
from superset_config_features import *  # noqa: E402, F403
from superset_config_logging import *  # noqa: E402, F403

SQLALCHEMY_DATABASE_URI = postgres_uri + f"/{os.environ['SUPERSET_FASE_DB_NAME']}"
