# syntax=docker/dockerfile:1.4

# Choose a python version that you know works with your application
FROM python:3.12-slim

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app

# Install the requirements using uv
RUN --mount=type=bind,source=./requirements.txt,target=./requirements.txt \
  --mount=type=cache,target=/root/.cache/uv \
  uv pip install -r requirements.txt

# Public port
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:8080/health || exit 1

# Create a non-root user and switch to it
RUN useradd -m nbuser
USER nbuser

WORKDIR /home/nbuser
VOLUME /home/nbuser

# Copy notebooks
RUN mkdir notebooks
COPY --chown=nbuser:nbuser  notebooks/* notebooks/

# DO NOT USE THIS IN PRODUCTION!
CMD [ "marimo", "edit", "--headless", "--host", "0.0.0.0", "--port", "8080", "--no-token"]
