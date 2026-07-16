import json
import logging
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine, inspect, select, Table, MetaData

LOGGER = logging.getLogger(__name__)

class PostgresExtractor:
    def __init__(self, config):
        """Accepts any valid configuration object exposing connection_uri."""
        self.config = config
        self.engine = create_engine(config.connection_uri)
        self.metadata = MetaData()    
       
    def map_pg_to_pq_type(self, pg_type) -> str:
        t = str(pg_type).lower()
        if 'int' in t: return 'bigint'
        if 'bool' in t: return 'bool'
        if 'json' in t or 'uuid' in t: return 'text' 
        if 'float' in t or 'numeric' in t or 'double' in t: return 'double'
        if 'timestamp' in t: return 'timestamp'
        if 'date' in t: return 'date'
        return 'text'

    def get_table_schema(self, table_name: str) -> dict:
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        return {col['name']: self.map_pg_to_pq_type(col['type']) for col in columns}

    def fetch_as_arrow(self, table_name: str, chunk_size: int = 50000):
        """Streams database data into structured PyArrow tables safely, supporting empty tables."""
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        col_hints = self.get_table_schema(table_name)
        
        arrow_fields = []
        for col_name in col_hints.keys():
            pq_type_str = col_hints[col_name]
            if pq_type_str == 'bigint': pa_type = pa.int64()
            elif pq_type_str == 'bool': pa_type = pa.bool_()
            elif pq_type_str == 'double': pa_type = pa.float64()
            elif pq_type_str == 'timestamp': pa_type = pa.timestamp('us')
            elif pq_type_str == 'date': pa_type = pa.date32()
            else: pa_type = pa.string()
            arrow_fields.append(pa.field(col_name, pa_type, nullable=True))
            
        target_schema = pa.schema(arrow_fields)
        
        with self.engine.connect() as conn:
            result_proxy = conn.execution_options(stream_results=True).execute(select(table))
            
            has_data = False
            while True:
                chunk = result_proxy.fetchmany(chunk_size)
                if not chunk:
                    break
                
                has_data = True
                df = pd.DataFrame(chunk, columns=result_proxy.keys())
                
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].apply(
                            lambda x: json.dumps(x) if isinstance(x, (dict, list)) 
                            else str(x) if pd.notnull(x) and not isinstance(x, str) \
                            else x
                        )

                arrow_table = pa.Table.from_pandas(df)
                
                aligned_columns = []
                for field in target_schema:
                    if field.name in arrow_table.column_names:
                        aligned_columns.append(arrow_table.column(field.name).cast(field.type))
                    else:
                        # Fallback for missing columns
                        aligned_columns.append(pa.array([None] * len(arrow_table), type=field.type))
                        
                yield pa.Table.from_arrays(aligned_columns, schema=target_schema)

            if not has_data:
                yield pa.table({}, schema=target_schema)