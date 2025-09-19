"""A standalone module to perform Iceberg table maintenance tasks.
See https://iceberg.apache.org/docs/latest/maintenance/ for more details.

Use the Trino query engine over pyiceberg as the latter currently only supports
the `expire_snapshots` operation.
"""

from . import cli


if __name__ == "__main__":
    cli()
