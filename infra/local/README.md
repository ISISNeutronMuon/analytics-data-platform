# Development infrastructure

The services defined here are intended for local development and should not be used for production.
We do not use https for local development, with the exception of Trino that requires it, due to
complications of ensuring self-signed certificates are trusted correctly across the host and
all service containers.
*Repeat: This configuration should not be used in production.*.

## Local set-up

The service can be set up locally using the instructions provided on [`getting-started.md`](/docs-devel/getting-started.md#Setup-/etc/hosts).
