FROM ghcr.io/marimo-team/marimo:0.16.4-sql

# Add basic utilities and Iceberg tools
RUN uv pip install \
  dotenv \
  pyiceberg[s3fs]~=0.9.1

HEALTHCHECK --interval=30s --timeout=3s CMD curl -f http://localhost:8080/health || exit 1
