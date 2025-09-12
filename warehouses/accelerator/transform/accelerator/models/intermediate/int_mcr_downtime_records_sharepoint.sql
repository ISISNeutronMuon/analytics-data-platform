with

stg_accelerator_sharepoint__equipment_downtime as (

  select * from {{ ref('stg_accelerator_sharepoint__equipment_downtime') }}

),

downtime_records_sharepoint as (

  select

    equipment,
    fault_date,
    cycle_name,
    cast(NULL as varchar) as cycle_phase,
    downtime_mins,
    fault_occurred_at,
    {{ adapter.quote('group') }},
    fault_description,
    managers_comments

  from

    stg_accelerator_sharepoint__equipment_downtime

)

select * from downtime_records_sharepoint
