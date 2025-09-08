with

source as (
  select * from {{ source('src_opralogweb', 'more_entry_columns') }}
)

select
  entry_id,
  TRIM(col_data) AS string_data,
  number_value AS number_data,
  additional_column_id
from
  source
