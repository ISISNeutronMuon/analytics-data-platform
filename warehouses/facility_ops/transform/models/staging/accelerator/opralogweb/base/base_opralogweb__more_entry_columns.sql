with

source as (
  select * from {{ source('src_opralogweb', 'more_entry_columns') }}
),

renamed as (

  select

    entry_id,
    {{ normalize_whitespace('col_data') }} as string_data,
    number_value as number_data,
    additional_column_id

  from

    source

)

select * from renamed
