import logging
import os

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
# The above logger object cannot be used to log at module level as logging as not been
# configured when this file is read. It can be used inside class methods of classes defined here
LOG_LEVEL = getattr(
    logging, os.getenv("SUPERSET_LOG_LEVEL", "INFO").upper(), logging.INFO
)
if LOG_LEVEL == logging.DEBUG:
    SILENCE_FAB = False
