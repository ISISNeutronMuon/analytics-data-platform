with

source as (
  select * from {{ source('src_opralogweb', 'logbook_chapter') }}
)

select
  logbook_chapter_no
from
  source
