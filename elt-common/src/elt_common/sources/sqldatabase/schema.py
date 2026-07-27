"""Functionality for converting an SQL table's schema into a pyarrow schema"""

import datetime as dt
import uuid

import pyarrow as pa
import sqlalchemy as sa


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
    sa.JSON: _SQL_ROOT_TYPES[sa.String],
    sa.NCHAR: _SQL_ROOT_TYPES[sa.String],
    sa.NVARCHAR: _SQL_ROOT_TYPES[sa.String],
    sa.SMALLINT: _SQL_ROOT_TYPES[sa.SmallInteger],
    sa.TEXT: _SQL_ROOT_TYPES[sa.String],
    sa.TIME: _SQL_ROOT_TYPES[sa.Time],
    sa.TIMESTAMP: _SQL_ROOT_TYPES[sa.DateTime],
    sa.UUID: _SQL_ROOT_TYPES[sa.Uuid],
    sa.VARCHAR: _SQL_ROOT_TYPES[sa.String],
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
    p_factory = _PYTHON_TYPE_MAP.get(sql_type.python_type)
    factory = _SQL_TYPE_MAP.get(type(sql_type), p_factory)
    if factory is not None:
        return factory()

    if isinstance(sql_type, sa.Numeric) or isinstance(sql_type, sa.NUMERIC):
        precision = getattr(sql_type, "precision", None)
        scale = getattr(sql_type, "scale", None)
        if precision is not None and scale is not None:
            return pa.decimal128(precision, scale)
        return pa.float64()

    raise TypeError(f"Unsupported SQLAlchemy type: {type(sql_type).__name__}")
