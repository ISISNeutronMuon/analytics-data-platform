from collections.abc import MutableMapping
from dataclasses import dataclass
import os
import time
from typing import List

import dlt
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
)
from dlt.common.destination import (
    TDestinationReferenceArg,
)
from dlt.common.runtime.run_context import RunContext

from .lakekeeper import Warehouse


def configure_dlt_for_testing():
    """Set up dlt for a testing environment"""

    def initial_providers(self) -> List[ConfigProvider]:
        # Use only environmental variables to configure
        return [
            EnvironProvider(),
        ]

    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
    RunContext.initial_providers = initial_providers  # type: ignore[method-assign]


@dataclass
class PyIcebergDestinationTestConfiguration:
    """Class for defining test setup for pyiceberg destination."""

    warehouse: Warehouse
    destination: TDestinationReferenceArg = "elt_common.dlt_destinations.pyiceberg"

    def setup(self, environ: MutableMapping = os.environ) -> None:
        """Sets up environment variables for this destination configuration

        Defaults to to os.environ
        """
        server, server_settings = self.warehouse.server, self.warehouse.server.settings
        environ["DESTINATION__PYICEBERG__CREDENTIALS__URI"] = str(
            self.warehouse.server.catalog_endpoint()
        )
        environ["DESTINATION__PYICEBERG__CREDENTIALS__PROJECT_ID"] = str(
            self.warehouse.server.settings.project_id
        )
        environ.setdefault("DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE", self.warehouse.name)
        environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__OAUTH2_SERVER_URI",
            str(server.token_endpoint),
        )
        environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_ID",
            server_settings.openid_client_id,
        )
        environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_SECRET",
            server_settings.openid_client_secret,
        )
        environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__SCOPE",
            server_settings.openid_scope,
        )

    def setup_pipeline(
        self,
        pipeline_name: str,
        dataset_name: str = None,
        dev_mode: bool = False,
        **kwargs,
    ) -> dlt.Pipeline:
        """Convenience method to setup pipeline with this configuration"""
        self.setup()
        return dlt.pipeline(
            pipeline_name=pipeline_name,
            pipelines_dir=kwargs.pop("pipelines_dir", None),
            destination=self.destination,
            dataset_name=(dataset_name if dataset_name is not None else pipeline_name + "_data"),
            dev_mode=dev_mode,
            **kwargs,
        )

    def clean_catalog(self):
        """Clean the destination catalog of all namespaces and tables"""
        catalog = self.warehouse.connect()
        for ns_name in catalog.list_namespaces():
            tables = catalog.list_tables(ns_name)
            for qualified_table_name in tables:
                catalog.purge_table(qualified_table_name)
            catalog.drop_namespace(ns_name)
        # allow a brief moment for cleanup tasks to complete
        time.sleep(0.1)
