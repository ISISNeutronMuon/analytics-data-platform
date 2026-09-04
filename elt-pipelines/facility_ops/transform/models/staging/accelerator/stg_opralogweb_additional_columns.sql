with

source as (

  select * from {{ source('accelerator_opralogweb', 'AdditionalColumns') }}

),

renamed as (

  select

    AdditionalColumnId as additional_column_id,
    trim(ColTitle) as column_title

from

  source

)

select * from renamed
