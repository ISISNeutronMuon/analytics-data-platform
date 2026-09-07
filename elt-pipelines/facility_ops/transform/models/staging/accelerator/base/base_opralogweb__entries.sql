with

source as (

  select * from {{ source('accelerator_opralogweb', 'Entries') }}

),

renamed as (

  select

      EntryId as entry_id,
      with_timezone(EntryTimestamp, 'UTC') as fault_occurred_at,
      cast({{ adapter.quote('EntryTimestamp') }} as date) as fault_date,
      trim(AdditionalComment) as fault_description,
      case
          when LogicallyDeleted = 'Y' then true
          else false
      end as logically_deleted

  from source

)

select * from renamed
