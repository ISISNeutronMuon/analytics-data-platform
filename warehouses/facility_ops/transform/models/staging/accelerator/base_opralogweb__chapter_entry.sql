with

source as (

  select * from {{ source('accelerator_opralogweb', 'ChapterEntry') }}

),

renamed as (

select

    EntryId as entry_id,
    PrincipalLogbook as principal_logbook,
    LogbookChapterNo as logbook_chapter_no,
    LogbookId as logbook_id

from

  source

)

select * from renamed
