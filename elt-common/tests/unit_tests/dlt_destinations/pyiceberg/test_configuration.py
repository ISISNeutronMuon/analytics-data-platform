from itertools import combinations
from typing import Any, Dict

from dlt.common.configuration import resolve
from dlt.common.destination.exceptions import DestinationTerminalException
import pytest

from elt_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
)


def test_initial_iceberg_config_state_defaults_to_rest():
    default_config = IcebergClientConfiguration()

    assert default_config.catalog_type == "rest"
    assert default_config.credentials is None
    assert default_config.connection_properties == {}


def _auth_arg_combinations():
    auth_arg_combinations = [
        ("oauth2_server_uri", "http://auth.provider/token/endpoint"),
        ("client_id", "myclient"),
        ("client_secret", "s3cr3t"),
    ]
    auth_arg_combinations.extend(combinations(auth_arg_combinations, 2))  # type: ignore
    return auth_arg_combinations


@pytest.mark.parametrize("auth_arg_combination", _auth_arg_combinations())
def test_missing_required_auth_params_raises_exception(auth_arg_combination):
    if not isinstance(auth_arg_combination[0], tuple):
        auth_arg_combination = [auth_arg_combination]
    with pytest.raises(DestinationTerminalException):
        config_values: Dict[str, Any] = {
            "bucket_url": "s3://mybucket/",
            "dataset_name": "mydataset",
        }
        credentials = {"uri": "http://catalog_host/catalog"}
        for key_value_pair in auth_arg_combination:
            credentials[key_value_pair[0]] = key_value_pair[1]
        config_values["credentials"] = credentials

        resolve.resolve_configuration(
            IcebergClientConfiguration(),
            explicit_value=config_values,
        )


def test_minimum_required_auth_params_accepted():
    config = resolve.resolve_configuration(
        IcebergClientConfiguration(),
        explicit_value={
            "bucket_url": "s3://mybucket/",
            "dataset_name": "mydataset",
            "credentials": {
                "uri": "http://catalog_host/catalog",
                "oauth2_server_uri": "http://auth.provider/token/endpoint",
                "client_id": "myclient",
                "client_secret": "s3cr3t",
            },
        },
    )

    assert config is not None
