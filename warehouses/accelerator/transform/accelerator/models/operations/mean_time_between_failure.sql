{{
  config(
    properties={
      "partitioning": "ARRAY['cycle_name']",
    },
    on_table_exists = 'drop'
)
}}
