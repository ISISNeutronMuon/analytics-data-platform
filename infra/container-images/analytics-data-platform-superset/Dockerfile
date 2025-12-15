ARG BASE_IMAGE=ghcr.io/isisneutronmuon/superset-fork:sha256-1f2019bd5a6ed81902d7c4035f0a363bceb10f60f5d9d98b4784857e2578506e
FROM ${BASE_IMAGE}

LABEL org.opencontainers.image.source=https://github.com/ISISNeutronMuon/analytics-data-platform
LABEL org.opencontainers.image.description="Custom build of Apache Superset"
LABEL org.opencontainers.image.licenses=MIT

# Install Python requirements
USER root
RUN /app/docker/pip-install.sh --requires-build-essential \
  openpyxl==3.1.5 \
  psycopg2-binary==2.9.10 \
  prophet==1.1.5 \
  playwright==1.57.0 \
  python-ldap==3.4.4 \
  trino[sqlalchemy]==0.332.0

RUN playwright install-deps && \
    playwright install chromium

# Run as superset
USER superset
