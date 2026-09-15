import os

from flask_caching.backends.rediscache import RedisCache

# -----------------------------------------------------------------------------
# Caching
# -----------------------------------------------------------------------------
redis_host, redis_port, redis_db = (
    os.environ[key] for key in ("REDIS_HOST", "REDIS_PORT", "SUPERSET_REDIS_DB")
)

COMMON_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_REDIS_HOST": redis_host,
    "CACHE_REDIS_PORT": redis_port,
    "CACHE_REDIS_DB": redis_db,
}
CACHE_CONFIG = dict(**COMMON_CACHE_CONFIG, CACHE_KEY_PREFIX="superset_metadata_cache")
DATA_CACHE_CONFIG = dict(
    **COMMON_CACHE_CONFIG, CACHE_KEY_PREFIX="superset_charting_data_cache"
)
# SQL lab
RESULTS_BACKEND = RedisCache(
    host=COMMON_CACHE_CONFIG["CACHE_REDIS_HOST"],
    port=COMMON_CACHE_CONFIG["CACHE_REDIS_PORT"],
    db=COMMON_CACHE_CONFIG["CACHE_REDIS_DB"],
    key_prefix="superset_results_backend",
)
SQLLAB_CTAS_NO_LIMIT = True
