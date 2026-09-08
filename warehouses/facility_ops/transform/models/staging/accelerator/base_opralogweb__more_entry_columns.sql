with

source as (
  select * from {{ source('accelerator_opralogweb', 'MoreEntryColumns') }}
),

renamed as (

  select

    EntryId as entry_id,
    {{ normalize_whitespace('ColData') }} as string_data,
    NumberValue as number_data,
    AdditionalColumnId as additional_column_id

  from

    source

)

select * from renamed
