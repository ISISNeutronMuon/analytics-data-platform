{{
  config(
    on_table_exists = 'drop'
)
}}

select * from {{ ref('stg_statusdisplay__cycle') }}
