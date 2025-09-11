import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from elt_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
    PyIcebergCatalogCredentials,
)
from elt_common.dlt_destinations.pyiceberg.pyiceberg import (
    PyIcebergClient,
)
from elt_common.dlt_destinations.pyiceberg.schema import PyIcebergTypeMapper


class pyiceberg(Destination[IcebergClientConfiguration, "PyIcebergClient"]):
    spec = IcebergClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.preferred_table_format = "iceberg"
        caps.supported_table_formats = ["iceberg"]
        caps.type_mapper = PyIcebergTypeMapper
        caps.has_case_sensitive_identifiers = True
        # v1 & v2 of Iceberg on support timestamps at  microsecond resolution
        caps.timestamp_precision = 6

        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255

        caps.supported_merge_strategies = ["delete-insert"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
        ]

        return caps

    @property
    def client_class(self) -> PyIcebergClient:
        return PyIcebergClient

    def __init__(
        self,
        credentials: PyIcebergCatalogCredentials = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Iceberg destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: An instance of IcebergCatalogCredentials to connect to the iceberg catalog.
        """
        super().__init__(
            credentials=credentials,
            **kwargs,
        )


pyiceberg.register()
