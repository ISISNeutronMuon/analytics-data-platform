import pyarrow as pa
import pytest
import sqlalchemy as sa

from elt_common.sources.sqldatabase.schema import to_pyarrow_schema


def test_builds_schema_from_multiple_columns():
    table = sa.Table(
        "example",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("name", sa.String),
        sa.Column("active", sa.Boolean),
    )

    schema = to_pyarrow_schema(table)

    expected = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("active", pa.bool_()),
        ]
    )

    assert schema == expected


@pytest.mark.parametrize(
    "sql_type, expected_type",
    [
        (sa.Integer(), pa.int32()),
        (sa.SmallInteger(), pa.int16()),
        (sa.BigInteger(), pa.int64()),
        (sa.String(), pa.string()),
        (sa.Text(), pa.string()),  # extended mapping
        (sa.Boolean(), pa.bool_()),
        (sa.Date(), pa.date32()),
        (sa.DateTime(), pa.timestamp("us")),
        (sa.Time(), pa.time64("us")),
        (sa.Float(), pa.float64()),
        (sa.Double(), pa.float64()),
        (sa.REAL(), pa.float32()),
        (sa.LargeBinary(), pa.binary()),
        (sa.Uuid(), pa.uuid()),
        (sa.Numeric(12, 3), pa.decimal128(12, 3)),
        (sa.Numeric(), pa.float64()),
        (sa.DECIMAL(), pa.float64()),
        (sa.DECIMAL(10, 2), pa.decimal128(10, 2)),
    ],
)
def test_supported_sqlalchemy_types(sql_type, expected_type):
    table = sa.Table(
        "example",
        sa.MetaData(),
        sa.Column("value", sql_type),
    )

    schema = to_pyarrow_schema(table)

    assert schema == pa.schema([pa.field("value", expected_type)])


# Emulates an sqlalchemy type which is not part of the core sqlalchemy types,
# but maps to a python string
class CustomStringType(sa.types.TypeDecorator):
    impl = sa.String

    @property
    def python_type(self):
        return str


def test_falls_back_to_python_type_mapping():
    table = sa.Table(
        "example",
        sa.MetaData(),
        sa.Column("value", CustomStringType()),
    )

    schema = to_pyarrow_schema(table)

    assert schema == pa.schema([pa.field("value", pa.string())])


# Emulates an sqlalchemy type which is not part of the core sqlalchemy types,
# and doesn't map to one of the supported python types
class UnsupportedType(sa.types.TypeDecorator):
    impl = sa.Enum

    @property
    def python_type(self):
        return list


def test_raises_for_unsupported_type():
    table = sa.Table(
        "example",
        sa.MetaData(),
        sa.Column("value", UnsupportedType()),
    )

    with pytest.raises(
        TypeError,
        match="Unsupported SQLAlchemy type: UnsupportedType",
    ):
        to_pyarrow_schema(table)
