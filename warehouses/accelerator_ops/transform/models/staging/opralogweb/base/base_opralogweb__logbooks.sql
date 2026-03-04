with

source as (

  select * from {{ source('src_opralogweb', 'logbooks') }}

),

renamed as (

  select

      logbook_id,
      logbook_name

  from

    source
)

select * from renamed
