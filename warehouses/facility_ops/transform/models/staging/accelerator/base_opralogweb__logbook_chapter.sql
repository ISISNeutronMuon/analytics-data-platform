with

source as (

  select * from {{ source('accelerator_opralogweb', 'logbook_chapter') }}

),

renamed as (

  select

    logbook_chapter_no

  from

    source

)

select * from renamed
