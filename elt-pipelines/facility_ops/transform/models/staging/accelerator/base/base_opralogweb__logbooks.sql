with

source as (

  select * from {{ source('accelerator_opralogweb', 'Logbooks') }}

),

renamed as (

  select

      LogbookId as logbook_id,
      LogbookName as logbook_name

  from

    source
)

select * from renamed
