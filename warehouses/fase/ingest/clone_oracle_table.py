#!/usr/bin/env python3
import os
# Set session default timezone to UTC to help bypass potential ORA-01805 version mismatches
os.environ["ORA_SDTZ"] = "UTC"

import argparse
import dlt
import cx_Oracle
import dateutil.parser  

from dlt.sources.sql_database import sql_database
from elt_common.dlt_destinations import pyiceberg
from sqlalchemy import create_engine, event
from sqlalchemy.pool import NullPool

# ----------------------------
# Arguments
# ----------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Run DLT pipeline for a specific Oracle table or ALL tables"
    )
    parser.add_argument("target_schema", help="Oracle schema name")
    parser.add_argument("target_table", help="Oracle table name to clone or 'all' for entire schema")
    parser.add_argument(
        "--drop", 
        action="store_true", 
        help="Force drop the destination table and clear pipeline state before running"
    )
    # NEW ARGUMENT ADDED HERE
    parser.add_argument(
        "--include-views",
        action="store_true",
        default=False,
        help="If set, includes views in the cloning process (default: False)"
    )
    return parser.parse_args()

# ----------------------------
# Debug Mapper: Inspecting Nulls and Types
# ----------------------------
def robust_data_mapper(item):
    date_keywords = ['DATE', 'TIME', 'TIMESTAMP']

    if 'PAYLOAD' in item or 'payload' in item:
        key = 'PAYLOAD' if 'PAYLOAD' in item else 'payload'
        val = item[key]
        if val is None:
            item[key] = ""

    for key, value in item.items():
        if isinstance(value, str) and any(kw in key.upper() for kw in date_keywords):
            try:
                item[key] = dateutil.parser.parse(value)
            except (ValueError, TypeError):
                pass
    return item

# ----------------------------
# No PK handling
# ----------------------------
def disable_dlt_identifiers_if_no_pk(resource):
    has_pk = any(
        col.get("primary_key", False)
        for col in resource.columns.values()
    )
    if not has_pk:
        resource.apply_hints(primary_key=[])

# ----------------------------
# Oracle type fix (Timestamp & LOBs)
# ----------------------------   
def oracle_output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type in (cx_Oracle.DATETIME, cx_Oracle.TIMESTAMP, getattr(cx_Oracle, 'TIMESTAMPTZ', -1)):
        return cursor.var(str, 128, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    if default_type == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)

# ----------------------------
# Robust Schema Hints
# ----------------------------
def apply_robust_hints(resource):
    new_cols = {}
    for col_name, col_meta in resource.columns.items():
        is_pk = col_meta.get("primary_key", False)
        data_type = col_meta.get("data_type")        
        hint = {}
        
        if not is_pk:
            hint["nullable"] = True
        else:
            hint["primary_key"] = True
            hint["nullable"] = False
            
        if any(kw in col_name.upper() for kw in ['DATE', 'TIME', 'TIMESTAMP']):
            hint["data_type"] = "timestamp"

        if data_type in ("decimal", "double", "float"):
            hint["data_type"] = "double"

        if hint:
            new_cols[col_name] = hint
            
    if new_cols:
        resource.apply_hints(columns=new_cols)

# ----------------------------
# DLT Oracle source
# ----------------------------
@dlt.source()
def oracle_source(oracle_uri: str, schema_name: str, table_name: str, include_views: bool = False):
    engine = create_engine(
        oracle_uri,
        poolclass=NullPool,
        connect_args={"encoding": "UTF-8", "nencoding": "UTF-8"},
    )

    @event.listens_for(engine, "connect")
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

    tables = None if table_name.lower() == 'all' else [table_name]

    # PASSING THE ARGUMENT HERE
    source = sql_database(
        credentials=engine,
        schema=schema_name,
        include_views=include_views,
        table_names=tables
    )

    for resource in source.resources.values():
        apply_robust_hints(resource)
        disable_dlt_identifiers_if_no_pk(resource)
        resource.add_map(robust_data_mapper)
        
    return source

if __name__ == "__main__":
    args = parse_args()
    
    oracle_uri = (
        f"oracle+cx_oracle://{args.target_schema}:pa55w0rdTolocalDB"
        "@localhost:1521/?service_name=xepdb"
    )

    pipeline_id = f"debug_oracle_{args.target_schema.lower()}_{args.target_table.lower()}"
    
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination=pyiceberg,
        dataset_name=args.target_schema.lower()  
    )

    if args.drop:
        print(f"\n--- [RESET] Dropping state for: {pipeline_id} ---")
        pipeline.drop()

    try:
        # PASSING THE ARGUMENT FROM ARGS
        source_data = oracle_source(
            oracle_uri, 
            args.target_schema, 
            args.target_table, 
            include_views=args.include_views
        )
        info = pipeline.run(source_data, write_disposition="replace")
        print(info)
    except Exception:
        raise