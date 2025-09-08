with

source as (
  select * from {{ source('src_opralogweb', 'logbooks') }}
)

select
    logbook_id,
    logbook_name
from
  source
