import os

# -----------------------------------------------------------------------------
# Superset's own DB
# -----------------------------------------------------------------------------
pg_user, pg_passwd, pg_host, pg_port = (
    os.environ[key]
    for key in (
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    )
)
postgres_uri = f"postgresql://{pg_user}:{pg_passwd}@{pg_host}:{pg_port}"
