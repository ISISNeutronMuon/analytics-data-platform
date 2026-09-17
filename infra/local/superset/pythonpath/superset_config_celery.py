import os

from celery.schedules import crontab

# -----------------------------------------------------------------------------
# Celery (worker orchestration)
# -----------------------------------------------------------------------------
redis_host, redis_port, redis_db = (
    os.environ[key] for key in ("REDIS_HOST", "REDIS_PORT", "SUPERSET_REDIS_DB")
)


class CeleryConfig:
    broker_url = f"redis://{redis_host}:{redis_port}/{redis_db}"
    imports = ("superset.sql_lab",)
    result_backend = broker_url
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
