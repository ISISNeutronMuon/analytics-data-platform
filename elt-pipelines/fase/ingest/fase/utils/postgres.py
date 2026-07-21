import json
import logging
from typing import Iterator

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from elt_common.extract import Watermark
from elt_common.sources.sqldatabase import SqlDatabaseExtract

LOGGER = logging.getLogger(__name__)


class PostgresExtract(SqlDatabaseExtract):
    def map_pg_to_pq_type(self, pg_type) -> str:
        t = str(pg_type).lower()
        if "int" in t:
            return "bigint"
        if "bool" in t:
            return "bool"
        if "json" in t or "uuid" in t:
            return "text"
        if "float" in t or "numeric" in t or "double" in t:
            return "double"
        if "timestamp" in t:
            return "timestamp"
        if "date" in t:
            return "date"
        return "text"

    def get_table_schema(self, table_name: str) -> pa.Schema:
        inspector = sa.inspect(self._engine)
        columns = inspector.get_columns(table_name)

        arrow_fields = []
        for col in columns:
            pq_type_str = self.map_pg_to_pq_type(col["type"])
            if pq_type_str == "bigint":
                pa_type = pa.int64()
            elif pq_type_str == "bool":
                pa_type = pa.bool_()
            elif pq_type_str == "double":
                pa_type = pa.float64()
            elif pq_type_str == "timestamp":
                pa_type = pa.timestamp("us")
            elif pq_type_str == "date":
                pa_type = pa.date32()
            else:
                pa_type = pa.string()
            arrow_fields.append(pa.field(col["name"], pa_type, nullable=True))

        return pa.schema(arrow_fields)

    def _extract_table(
        self,
        name: str,
        *,
        conn: sa.Connection,
        watermark: Watermark | None = None,
    ) -> Iterator[pa.Table]:
        LOGGER.debug(
            f"Extracting Postgres table {name} in chunks of {self._chunk_size} rows."
        )

        target_schema = self.get_table_schema(name)
        table = sa.Table(name, self._metadata, autoload_with=self._engine)

        query = sa.select(table)
        if watermark is not None:
            column, max_value = watermark.column, watermark.value
            LOGGER.debug(
                f"Cursor value detected. Limiting query to {column} > {max_value}"
            )
            query = query.where(sa.column(column) > max_value)

        result = conn.execution_options(yield_per=self._chunk_size).execute(query)

        has_data = False
        while True:
            chunk = result.fetchmany(self._chunk_size)
            if not chunk:
                break

            has_data = True
            df = pd.DataFrame(chunk, columns=result.keys())

            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].apply(
                        lambda x: (
                            json.dumps(x)
                            if isinstance(x, (dict, list))
                            else str(x)
                            if pd.notnull(x) and not isinstance(x, str)
                            else x
                        )
                    )

            arrow_table = pa.Table.from_pandas(df)

            aligned_columns = []
            for field in target_schema:
                if field.name in arrow_table.column_names:
                    aligned_columns.append(
                        arrow_table.column(field.name).cast(field.type)
                    )
                else:
                    aligned_columns.append(
                        pa.array([None] * len(arrow_table), type=field.type)
                    )

            yield pa.Table.from_arrays(aligned_columns, schema=target_schema)

        if not has_data:
            dummy_row = {col_key: [None] for col_key in result.keys()}
            empty_df = pd.DataFrame(dummy_row)
            yield pa.Table.from_pandas(empty_df, schema=target_schema)
