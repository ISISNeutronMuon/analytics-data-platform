import os

# -----------------------------------------------------------------------------
# Feature configuration
# -----------------------------------------------------------------------------
WEBDRIVER_BASEURL = f"http://localhost:8088{os.environ['SUPERSET_APP_ROOT']}/"
# The base URL for the email report hyperlinks.
WEBDRIVER_BASEURL_USER_FRIENDLY = (
    f"https://localhost:50080{os.environ['SUPERSET_APP_ROOT']}/"
)
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
