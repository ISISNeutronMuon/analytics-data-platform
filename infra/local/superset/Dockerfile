# syntax=docker/dockerfile
FROM superset-allow-app-root:latest

# Install Python requirements
USER root
RUN /app/docker/pip-install.sh --requires-build-essential \
  openpyxl==3.1.5 \
  psycopg2-binary==2.9.10 \
  prophet==1.1.5 \
  python-ldap==3.4.4 \
  trino[sqlalchemy]==0.332.0

# Run as superset
USER superset
