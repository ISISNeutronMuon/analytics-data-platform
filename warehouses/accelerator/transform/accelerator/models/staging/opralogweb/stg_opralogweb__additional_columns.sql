with

source as (
  select * from {{ source('src_opralogweb', 'additional_columns') }}
)

select
    additional_column_id,
    TRIM(col_title) AS column_title
from
  source
