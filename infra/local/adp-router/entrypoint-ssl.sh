#!/bin/sh
set -e

# Docker should have created this
CERTS_DIR=/certs
KEY_FILE=$CERTS_DIR/localdev-key.pem
CERT_FILE=$CERTS_DIR/localdev.pem

if [ ! -f "$CERT_FILE" ]; then
    # Generate self-signed certificate if one doesn't exist
    echo "No SSL certificate detected at $CERT_FILE. Generating self-signed certificate..."
    apk add openssl
    openssl req -x509 -newkey rsa:4096 \
    -keyout $KEY_FILE \
    -out $CERT_FILE \
    -days 365 \
    -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1"
    echo "Certificate file: $CERT_FILE"
    echo "Certificate key: $KEY_FILE"
else
    echo "SSL certificate '$CERT_FILE' exists, skipping generation."
fi

# Run traefik
exec traefik
