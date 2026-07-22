import logging
import os

import cx_Oracle
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine, event, inspect

LOGGER = logging.getLogger(__name__)

os.environ["ORA_SDTZ"] = "UTC"


def oracle_output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type in (
        cx_Oracle.DATETIME,
        cx_Oracle.TIMESTAMP,
        getattr(cx_Oracle, "TIMESTAMPTZ", -1),
    ):
        return cursor.var(str, 128, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)


class OracleExtractor:
    def __init__(self, config):
        self.config = config
        self.engine = create_engine(config.connection_uri)

        def custom_connection_creator():
            return cx_Oracle.connect(
                config.username,
                config.password,
                f"{config.host}:{config.port}/{config.database}",
            )

        self.engine.pool._creator = custom_connection_creator

        @event.listens_for(self.engine, "connect")
        def on_connect(dbapi_conn, _):
            dbapi_conn.outputtypehandler = oracle_output_type_handler
            cursor = dbapi_conn.cursor()
            cursor.execute("""
                ALTER SESSION SET
                  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
                  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
                  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'
            """)
            cursor.close()

    def map_oracle_to_pq_type(self, oracle_type) -> str:
        """Maps incoming Oracle internal types to canonical parquet string representations."""
        t = str(oracle_type).lower()
        if "number" in t or "int" in t:
            if "," in t:
                return "double"
            return "bigint"
        if "float" in t or "double" in t or "decimal" in t:
            return "double"
        if "timestamp" in t or "date" in t:
            return "timestamp"
        if "clob" in t or "blob" in t:
            return "text"
        return "text"

    def get_table_schema(self, table_name: str) -> dict:
        """Inspects structural column parameters directly out of the Oracle data dictionary."""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=self.config.schema)

        col_hints = {}
        for c in columns:
            col_name = c["name"]
            col_hints[col_name] = {
                "name": col_name,
                "data_type": self.map_oracle_to_pq_type(c["type"]),
                "nullable": not c.get("primary_key", False),
            }
        return col_hints

    def _get_lob_columns(self, table_name: str) -> list[str]:
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=self.config.schema)
        return [
            c["name"]
            for c in columns
            if any(kw in str(c["type"]).lower() for kw in ["clob", "blob"])
        ]

    def fetch_as_arrow(self, table_name: str):
        """Streams Oracle segments into structured PyArrow tables using unified schema alignment."""
        col_hints = self.get_table_schema(table_name)
        lob_cols = self._get_lob_columns(table_name)

        arrow_fields = []
        for col_name, hint in col_hints.items():
            pq_type_str = hint["data_type"]
            if pq_type_str == "bigint":
                pa_type = pa.int64()
            elif pq_type_str == "double":
                pa_type = pa.float64()
            elif pq_type_str == "timestamp":
                pa_type = pa.timestamp("us")
            else:
                pa_type = pa.string()
            arrow_fields.append(pa.field(col_name.lower(), pa_type, nullable=True))

        target_schema = pa.schema(arrow_fields)

        with self.engine.connect() as conn:
            query = (
                f'SELECT * FROM "{self.config.schema.upper()}"."{table_name.upper()}"'
            )
            df = pd.read_sql(query, conn)

            if not df.empty:
                df.columns = [c.lower() for c in df.columns]

                for col_name, hint in col_hints.items():
                    col_lower = col_name.lower()
                    if col_lower in df.columns:
                        if hint["data_type"] == "timestamp":
                            df[col_lower] = pd.to_datetime(
                                df[col_lower], utc=True, errors="coerce"
                            ).dt.tz_localize(None)

                        elif col_lower in lob_cols:
                            df[col_lower] = df[col_lower].apply(
                                lambda x: str(x) if x is not None else None
                            )

                non_ts_cols = [c for c in df.columns if df[c].dtype.kind != "M"]
                df[non_ts_cols] = (
                    df[non_ts_cols].astype(object).where(df[non_ts_cols].notna(), None)
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
            else:
                dummy_row = {col_name.lower(): [None] for col_name in col_hints.keys()}
                empty_df = pd.DataFrame(dummy_row)
                yield pa.Table.from_pandas(empty_df, schema=target_schema)
