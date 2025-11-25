from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.data_types.typing import DATA_TYPES as DLT_DATA_TYPES
from dlt.common.schema.typing import TColumnSchema
from elt_common.dlt_destinations.pyiceberg.helpers import (
    PARTITION_HINT,
    PartitionTrBuilder,
    RestCatalog,
    create_catalog,
    create_iceberg_schema,
    create_partition_spec,
    dlt_type_to_iceberg,
    iceberg_to_dlt_type,
    namespace_exists,
    transforms,
)
from pyiceberg.exceptions import NoSuchNamespaceError as PyIcebergNoSuchNamespaceError
import pyiceberg.types
import pytest
from pytest_mock import MockerFixture


@pytest.fixture()
def dlt_schema() -> PreparedTableSchema:
    columns = dict(
        row_id=TColumnSchema(data_type="bigint", nullable=False, primary_key=True),
        entry_name=TColumnSchema(data_type="text", nullable=False),
        entry_timestamp=TColumnSchema(data_type="timestamp", nullable=False),
        entry_weight=TColumnSchema(data_type="double", nullable=False),
    )
    return PreparedTableSchema(name="table_name", columns=columns)


def test_create_catalog_passes_properties_to_catalog_impl(mocker: MockerFixture):
    patched_catalog = mocker.patch("elt_common.dlt_destinations.pyiceberg.helpers.RestCatalog")
    name, properties = "unit_test_catalog", {"a": 1, "c": 3}
    create_catalog(name, **properties)

    patched_catalog.assert_called_once_with(name, **properties)


def test_namespace_exists_returns_true_if_load_namespace_properties_succeeds(mocker: MockerFixture):
    mock_catalog = mocker.MagicMock(spec=RestCatalog)
    mock_catalog.load_namespace_properties.return_value = {"namespace": "analytics"}

    assert namespace_exists(mock_catalog, "analytics")


def test_namespace_exists_returns_false_if_load_namespace_properties_fails(mocker: MockerFixture):
    mock_catalog = mocker.MagicMock(spec=RestCatalog)
    mock_catalog.load_namespace_properties.side_effect = PyIcebergNoSuchNamespaceError

    assert not namespace_exists(mock_catalog, "analytics")


def test_dlt_type_to_iceberg_returns_type_for_dlt_types():
    for data_type in DLT_DATA_TYPES:
        if data_type == "wei":  # this isn't supported
            continue
        column = TColumnSchema(data_type=data_type)
        if data_type == "decimal":
            column["precision"] = 32
            column["scale"] = 3
        assert isinstance(dlt_type_to_iceberg(column), pyiceberg.types.PrimitiveType)


def test_dlt_type_to_iceberg_raises_type_error_if_type_not_supported():
    column = TColumnSchema(data_type="wei")

    with pytest.raises(TypeError):
        dlt_type_to_iceberg(column)


def test_dlt_type_to_iceberg_with_decimal_type_missing_extra_properties_fails_to_convert():
    column = TColumnSchema(data_type="decimal")

    with pytest.raises(TypeError):
        dlt_type_to_iceberg(column)


def test_iceberg_field_to_dlt_type_gives_known_types():
    for type_name in (
        "StringType",
        "DoubleType",
        "TimestamptzType",
        "DateType",
        "TimeType",
        "BinaryType",
    ):
        field_required = pyiceberg.types.NestedField(
            1, "name", getattr(pyiceberg.types, type_name)(), required=True
        )
        dlt_column = iceberg_to_dlt_type(field_required)
        assert dlt_column.get("data_type") in DLT_DATA_TYPES
        assert not dlt_column.get("nullable")

        field_not_required = pyiceberg.types.NestedField(
            1, "name", getattr(pyiceberg.types, type_name)(), required=False
        )
        dlt_column = iceberg_to_dlt_type(field_not_required)
        assert dlt_column.get("nullable")

    # decimal needs extra arugments
    dlt_column = iceberg_to_dlt_type(
        pyiceberg.types.NestedField(1, "name", getattr(pyiceberg.types, "DecimalType")(28, 4))
    )
    assert dlt_column.get("data_type") == "decimal"
    assert dlt_column.get("precision") == 28
    assert dlt_column.get("scale") == 4


def test_iceberg_to_dlt_type_raises_type_error_if_type_not_supported():
    class NewType(pyiceberg.types.PrimitiveType):
        pass

    with pytest.raises(TypeError):
        iceberg_to_dlt_type(pyiceberg.types.NestedField(1, "name", NewType()))


def test_create_iceberg_schema(dlt_schema: PreparedTableSchema):
    iceberg_schema = create_iceberg_schema(dlt_schema)

    assert len(iceberg_schema.fields) == len(dlt_schema.get("columns"))
    assert iceberg_schema.identifier_field_ids == [1]
    expected_types = dict(
        row_id=pyiceberg.types.LongType,
        entry_name=pyiceberg.types.StringType,
        entry_timestamp=pyiceberg.types.TimestamptzType,
        entry_weight=pyiceberg.types.DoubleType,
    )
    for field in iceberg_schema.fields:
        assert isinstance(field.field_type, expected_types[field.name])


def test_create_partition_spec(dlt_schema: PreparedTableSchema):
    dlt_schema[PARTITION_HINT] = {
        "entry_timestamp": PartitionTrBuilder.year("entry_timestamp").transform
    }
    iceberg_schema = create_iceberg_schema(dlt_schema)

    partition_spec = create_partition_spec(dlt_schema, iceberg_schema)

    assert len(partition_spec.fields) == 1
    assert partition_spec.fields[0].source_id == 3  # 3rd column in dlt_schema
    assert isinstance(partition_spec.fields[0].transform, transforms.YearTransform)
