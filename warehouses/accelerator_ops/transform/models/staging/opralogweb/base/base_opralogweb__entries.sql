with

source as (

  select * from {{ source('src_opralogweb', 'entries') }}

),

renamed as (

  select

      entry_id,
      with_timezone(entry_timestamp, 'UTC') as fault_occurred_at,
      cast({{ adapter.quote('entry_timestamp') }} as date) as fault_date,
      trim(additional_comment) as fault_description,
      case
          when logically_deleted = 'Y' then true
          else false
      end as logically_deleted

  from source

)

select * from renamed
