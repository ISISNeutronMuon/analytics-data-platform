from typing import Dict

from elt_common.dlt_destinations.pyiceberg.helpers import (
    dlt_type_to_iceberg,
    iceberg_to_dlt_type,
    PrimitiveType,
)

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.common.utils import without_none
from dlt.destinations.type_mapping import TypeMapperImpl


TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}


class PyIcebergTypeMapper(TypeMapperImpl):
    def to_destination_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> PrimitiveType:
        return dlt_type_to_iceberg(column)

    def from_destination_type(self, iceberg_field) -> TColumnType:
        return without_none(iceberg_to_dlt_type(iceberg_field))
