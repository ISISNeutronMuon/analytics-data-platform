with

source as (

  select * from {{ source('src_opralogweb', 'logbook_chapter') }}

),

renamed as (

  select

    logbook_chapter_no

  from

    source

)

select * from renamed
