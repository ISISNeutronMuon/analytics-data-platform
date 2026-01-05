from contextlib import contextmanager
from typing import cast

import dlt

from .pyiceberg import PyIcebergClient


@contextmanager
def iceberg_catalog(pipeline: dlt.Pipeline):
    """Given a pipeline return the Iceberg catalog client for this pipeline"""
    with pipeline.destination_client() as client:
        assert isinstance(client, PyIcebergClient)
        yield cast(PyIcebergClient, client).iceberg_catalog
