with

source as (

  select * from {{ source('src_opralogweb', 'entries') }}

),

renamed as (

  select

      entry_id,
      entry_timestamp as fault_occurred_at,
      cast({{ adapter.quote('entry_timestamp') }} as date) as fault_date,
      trim(additional_comment) as fault_description

  from source

)

select * from renamed
