with

source as (

  select * from {{ source('src_opralogweb', 'chapter_entry') }}

),

renamed as (

select

    entry_id,
    principal_logbook,
    logbook_chapter_no,
    logbook_id

from

  source

)

select * from renamed
