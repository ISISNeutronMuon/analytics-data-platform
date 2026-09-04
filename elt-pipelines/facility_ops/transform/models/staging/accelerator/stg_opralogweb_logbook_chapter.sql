with

source as (

  select * from {{ source('accelerator_opralogweb', 'LogBookChapter') }}

),

renamed as (

  select

    LogBookChapterNo as logbook_chapter_no

  from

    source

)

select * from renamed
