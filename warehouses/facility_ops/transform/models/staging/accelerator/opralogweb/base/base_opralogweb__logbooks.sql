with

source as (

  select * from {{ source('accelerator_opralogweb', 'logbooks') }}

),

renamed as (

  select

      logbook_id,
      logbook_name

  from

    source
)

select * from renamed
