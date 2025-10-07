#!/usr/bin/env bash
set -e

#
# Install the certificate CA bundle
#
if [ -n "$SUPERSET_CA_BUNDLE" ]; then
  certifi_cacert_file=$(python -m certifi)
  # Check if 5th line is in the file already
  if ! cat "$certifi_cacert_file" | grep -q -- "$(sed -n '5p' "$SUPERSET_CA_BUNDLE")"; then
    echo "Appending $SUPERSET_CA_BUNDLE to $certifi_cacert_file"
    cat "$SUPERSET_CA_BUNDLE" >> "$certifi_cacert_file"
  fi
fi

#
# Bootstrap
#
/app/docker/docker-bootstrap.sh "${@}"
