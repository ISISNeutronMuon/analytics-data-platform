with

source as (

  select * from {{ source('src_opralogweb', 'user_entries') }}

),

renamed as (

  select

      entry_id,
      entry_timestamp as fault_occurred_at,
      cast({{ identifier("entry_timestamp") }} as date) as fault_date,
      trim(additional_comment) as fault_description

  from source
  where entry_timestamp >= from_iso8601_timestamp('{{ var("opralog_epoch_date") }}')

)

select * from renamed
