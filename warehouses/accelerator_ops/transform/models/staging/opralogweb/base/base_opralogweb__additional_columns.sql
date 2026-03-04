with

source as (

  select * from {{ source('src_opralogweb', 'additional_columns') }}

),

renamed as (

  select

    additional_column_id,
    trim(col_title) as column_title

from

  source

)

select * from renamed
