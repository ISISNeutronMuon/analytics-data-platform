import dlt
import pandas as pd
import json
import argparse
import sys
from sqlalchemy import create_engine, inspect
from elt_common.dlt_destinations import pyiceberg

BASE_URI = "postgresql://duouser:duopassword@localhost:5432/duo"

def map_pg_to_dlt_type(pg_type):
    """Maps SQLAlchemy types to dlt core types."""
    t = str(pg_type).lower()
    if 'int' in t: return 'bigint'
    if 'bool' in t: return 'bool'
    if 'json' in t: return 'text' 
    if 'float' in t or 'numeric' in t or 'double' in t: return 'double'
    if 'timestamp' in t: return 'timestamp'
    if 'date' in t: return 'date'
    return 'text'

def get_table_metadata(engine, schema, table_name):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)
    json_cols = [c['name'] for c in columns if 'json' in str(c['type']).lower()]
    return json_cols, columns

@dlt.source
def postgres_dynamic_source(engine, schema, target_table):
    inspector = inspect(engine)
    tables = [target_table] if target_table.lower() != 'all' else inspector.get_table_names(schema=schema)
    
    for table_name in tables:
        json_cols, columns = get_table_metadata(engine, schema, table_name)
        
        # Build hints and a dummy row
        col_hints = {}
        dummy_row = {}
        
        for c in columns:
            col_name = c['name']
            col_hints[col_name] = {
                "name": col_name,
                "data_type": map_pg_to_dlt_type(c['type']),
                "nullable": True # FORCE nullable for the dummy load
            }
            dummy_row[col_name] = None

        def make_resource(t_name, j_cols, hints, d_row):
            @dlt.resource(
                name=t_name,
                write_disposition="replace",
                columns=hints,
                table_format="iceberg"
            )
            def load_resource():
                query = f'SELECT * FROM "{schema}"."{t_name}"'
                df = pd.read_sql(query, engine)

                if not df.empty:
                    for col in j_cols:
                        if col in df.columns:
                            df[col] = df[col].apply(
                                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else (str(x) if x is not None else None)
                            )
                    yield df.to_dict(orient="records")
                else:
                    print(f"--- Table {t_name} is empty. Yielding dummy row to force Iceberg creation. ---")
                    # Yielding a list with one dictionary containing Nones forces a load package
                    yield [d_row] 
                    
            return load_resource

        yield make_resource(table_name, json_cols, col_hints, dummy_row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("schema")
    parser.add_argument("table")
    args = parser.parse_args()
    
    engine = create_engine(BASE_URI)
    pipeline = dlt.pipeline(
        pipeline_name=f"pg_to_iceberg_{args.schema}",
        destination=pyiceberg,
        dataset_name=args.schema,
    )

    try:
        info = pipeline.run(postgres_dynamic_source(engine, args.schema, args.table))
        print(info)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)