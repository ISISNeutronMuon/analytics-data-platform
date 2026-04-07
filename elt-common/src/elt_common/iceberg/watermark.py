# Functionaility to track watermark values for incremental ingestion
import datetime as dt

from elt_common.typing import Watermark
import pyarrow as pa
import pyarrow.types
import pyarrow.compute as pc
from pyiceberg.table import Table as IcebergTable

WATERMARK_INGEST_COLUMN_KEY = "ingest.watermark.column"
WATERMARK_INGEST_VALUE_KEY = "ingest.watermark.value"

# shorten module name
pa.types = pyarrow.types


def get_watermark_property_value(table: IcebergTable) -> Watermark | None:
    """Get the watermark value if one exists on the table, else return None"""
    column_name = table.properties.get(WATERMARK_INGEST_COLUMN_KEY)
    value_str = table.properties.get(WATERMARK_INGEST_VALUE_KEY)
    if column_name is None or value_str is None:
        return None

    # Convert from string to the expected type
    schema = table.schema().as_arrow()
    try:
        column_type = schema.field(column_name).type
    except KeyError:
        return None

    if pa.types.is_timestamp(column_type):
        value = dt.datetime.fromisoformat(value_str)
    elif pa.types.is_integer(column_type):
        value = int(value_str)
    else:
        raise ValueError(
            f"Table '{table.name}': Unsupported column type, '{column_type}', while converting watermark value from string."
        )

    return Watermark(column=column_name, value=value)


def create_watermark_property(data: pa.Table, column: str) -> dict[str, str]:
    """Create a table property to mark the latest ingest value.

    The implmentation simply takes the maximum value of a column and converts it to a string.
    """
    column_type = data.field(column).type
    max_value = pc.max(data[column])
    if pa.types.is_timestamp(column_type):
        watermark_value = max_value.as_py().isoformat()
    elif pa.types.is_integer(column_type):
        watermark_value = str(max_value.as_py())
    else:
        raise ValueError(
            f"Unsupported column type, {data.schema.field(column).type}, while converting watermark value to string."
        )

    return {WATERMARK_INGEST_COLUMN_KEY: column, WATERMARK_INGEST_VALUE_KEY: watermark_value}
