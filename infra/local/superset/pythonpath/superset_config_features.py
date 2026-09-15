import os

# -----------------------------------------------------------------------------
# Feature configuration
# -----------------------------------------------------------------------------
WEBDRIVER_BASEURL = f"http://{os.environ['ROUTER_HOSTNAME_EXTERNAL']}:{os.environ['ROUTER_PORT_HTTP']}{os.environ['SUPERSET_APP_ROOT']}/"
WEBDRIVER_BASEURL_USER_FRIENDLY = WEBDRIVER_BASEURL

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
