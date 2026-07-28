"""Functionality for converting an SQL table's schema into a pyarrow schema"""

import datetime as dt
import uuid

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def to_pyarrow_schema(table: sa.Table) -> pa.Schema:
    return pa.schema([_to_pyarrow_field(c) for c in table.columns])


def _to_pyarrow_field(column: sa.Column) -> pa.Field:
    return pa.field(column.name, _to_pyarrow_type(column.type))


# Mapping of generic sqlalchemy types to pyarrow types
_SQL_ROOT_TYPES = {
    sa.BigInteger: pa.int64,
    sa.Boolean: pa.bool_,
    sa.Date: pa.date32,
    sa.DateTime: lambda: pa.timestamp("us"),
    sa.Double: pa.float64,
    sa.Float: pa.float64,
    sa.Integer: pa.int32,
    sa.Interval: lambda: pa.duration("us"),
    sa.JSON: pa.string,
    sa.LargeBinary: pa.binary,
    sa.SmallInteger: pa.int16,
    sa.String: pa.string,
    sa.Time: lambda: pa.time64("us"),
    sa.Uuid: pa.uuid,
    sa.REAL: pa.float32,
}

# sqlalchemy types which should have the same value as a root type
_EXTENDED_SQL_TYPES = {
    sa.Text: _SQL_ROOT_TYPES[sa.String],
    sa.Unicode: _SQL_ROOT_TYPES[sa.String],
    sa.UnicodeText: _SQL_ROOT_TYPES[sa.String],
    sa.BIGINT: _SQL_ROOT_TYPES[sa.BigInteger],
    sa.BINARY: _SQL_ROOT_TYPES[sa.LargeBinary],
    sa.BLOB: _SQL_ROOT_TYPES[sa.LargeBinary],
    sa.BOOLEAN: _SQL_ROOT_TYPES[sa.Boolean],
    sa.CHAR: _SQL_ROOT_TYPES[sa.String],
    sa.CLOB: _SQL_ROOT_TYPES[sa.String],
    sa.DATE: _SQL_ROOT_TYPES[sa.Date],
    sa.DATETIME: _SQL_ROOT_TYPES[sa.DateTime],
    sa.DOUBLE: _SQL_ROOT_TYPES[sa.Double],
    sa.DOUBLE_PRECISION: _SQL_ROOT_TYPES[sa.Double],
    sa.INTEGER: _SQL_ROOT_TYPES[sa.Integer],
    sa.FLOAT: _SQL_ROOT_TYPES[sa.Float],
    sa.INT: _SQL_ROOT_TYPES[sa.Integer],
    sa.NCHAR: _SQL_ROOT_TYPES[sa.String],
    sa.NVARCHAR: _SQL_ROOT_TYPES[sa.String],
    sa.SMALLINT: _SQL_ROOT_TYPES[sa.SmallInteger],
    sa.TEXT: _SQL_ROOT_TYPES[sa.String],
    sa.TIME: _SQL_ROOT_TYPES[sa.Time],
    sa.TIMESTAMP: _SQL_ROOT_TYPES[sa.DateTime],
    sa.UUID: _SQL_ROOT_TYPES[sa.Uuid],
    sa.VARCHAR: _SQL_ROOT_TYPES[sa.String],
    postgresql.JSON: _SQL_ROOT_TYPES[sa.String],
    postgresql.JSONB: _SQL_ROOT_TYPES[sa.String],
}

_SQL_TYPE_MAP = _SQL_ROOT_TYPES | _EXTENDED_SQL_TYPES

# Backup mapping for DB specific types which we don't cover in _SQL_TYPE_MAP
_PYTHON_TYPE_MAP = {
    str: pa.string,
    int: pa.int64,
    float: pa.float64,
    bool: pa.bool_,
    dt.datetime: lambda: pa.timestamp("us"),
    uuid.UUID: pa.uuid,
}


def _to_pyarrow_type(sql_type):
    factory = _SQL_TYPE_MAP.get(type(sql_type))
    if factory is not None:
        return factory()

    # If type is not a generic SQL type, see if it's represented by a python type
    factory = _PYTHON_TYPE_MAP.get(sql_type.python_type)
    if factory is not None:
        return factory()

    if isinstance(sql_type, sa.Numeric) or isinstance(sql_type, sa.NUMERIC):
        precision = getattr(sql_type, "precision", None)
        scale = getattr(sql_type, "scale", None)
        if precision is None or scale is None:
            return pa.float64()

        if not isinstance(precision, int):
            raise TypeError(f"Numeric precision was non-integer '{precision}'")
        elif not isinstance(scale, int):
            raise TypeError(f"Numeric scale was non-integer '{precision}'")

        # 38 and 76 are the maximum precision for decimal128 and decimal256
        # https://arrow.apache.org/docs/python/generated/pyarrow.decimal128.html
        # https://arrow.apache.org/docs/python/generated/pyarrow.decimal256.html
        if precision < 39:
            return pa.decimal128(precision, scale)
        elif precision < 77:
            return pa.decimal256(precision, scale)
        else:
            raise TypeError(f"Numeric precision cannot be larger than 76, was {precision}")

    raise TypeError(f"Unsupported SQLAlchemy type: {type(sql_type).__name__}")
