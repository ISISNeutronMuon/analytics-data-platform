{{
  config(
    properties={
      "partitioning": "ARRAY['cycle_name']",
    },
    on_table_exists = 'drop'
)
}}

with

records_sharepoint as ( select * from {{ ref('stg_accelerator_sharepoint__mcr_equipment_downtime') }} ),
records_opralogweb as ( select * from {{ ref('stg_opralogweb__mcr_equipment_downtime') }} ),

records_sharepoint_with_cycle_phase_col as (

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

    records_sharepoint

),

records_opralogweb_after_sharepoint_joined_with_cycles as (

  select

    r.equipment,
    r.fault_date,
    c.name as cycle_name,
    c.phase as cycle_phase,
    r.downtime_mins,
    r.fault_occurred_at,
    r.{{ adapter.quote('group') }},
    r.fault_description,
    r.managers_comments

  from records_opralogweb r
  left join {{ ref("int_cycle_phases_without_target") }} c on r.fault_occurred_at between c.started_at and c.ended_at
  where fault_occurred_at > (select max(fault_occurred_at) from records_sharepoint_with_cycle_phase_col)
),

unioned as (

  select * from  records_sharepoint_with_cycle_phase_col
  union
  select * from records_opralogweb_after_sharepoint_joined_with_cycles

)

-- add order by clause for iceberg table sorting crterion
select * from unioned order by fault_occurred_at asc
