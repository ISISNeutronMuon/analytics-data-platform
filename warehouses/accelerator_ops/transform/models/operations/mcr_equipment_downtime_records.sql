{{
  config(
    properties={
      "partitioning": "ARRAY['cycle_name']",
    },
    on_table_exists = 'drop'
)
}}

with

records_sharepoint as ( select * from {{ ref('stg_accelerator_sharepoint__equipment_downtime_data_11_08_24') }} ),

records_opralogweb as ( select * from {{ ref('stg_opralogweb__mcr_equipment_downtime') }} ),

equipment_name_mappings as ( select * from {{ ref('stg_accelerator_sharepoint__edr_equipment_mapping') }} ),

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
  left join {{ ref("cycles") }} c on r.fault_occurred_at between c.started_at and c.ended_at
  where fault_occurred_at > (select max(fault_occurred_at) from records_sharepoint_with_cycle_phase_col)
),

all_records as (

  select * from  records_sharepoint_with_cycle_phase_col
  union
  select * from records_opralogweb_after_sharepoint_joined_with_cycles

),

equipment_up_at_col as (

  select

    equipment,
    fault_date,
    cycle_name,
    cycle_phase,
    downtime_mins,
    fault_occurred_at,
    fault_occurred_at + (interval '1' minute * downtime_mins) as equipment_up_at,
    {{ adapter.quote('group') }},
    fault_description,
    managers_comments

  from

    all_records d
),

uptime_col as (

  select

    equipment,
    fault_date,
    cycle_name,
    cycle_phase,
    downtime_mins,
    fault_occurred_at,
    equipment_up_at,
    date_diff('minute',
      lag(equipment_up_at, 1, null) over
          (partition by cycle_name, equipment order by fault_occurred_at), fault_occurred_at
    ) as uptime_before_fault_mins,
    {{ adapter.quote('group') }},
    fault_description,
    managers_comments

  from equipment_up_at_col
),

equipment_category_col as (

  select

    {{ normalize_whitespace('u.equipment') }} as equipment,
    m.equipment_category as equipment_category,
    fault_date,
    cycle_name,
    cycle_phase,
    downtime_mins,
    fault_occurred_at,
    equipment_up_at,
    uptime_before_fault_mins,
    {{ adapter.quote('group') }},
    fault_description,
    managers_comments

  from uptime_col u
  left join equipment_name_mappings m on {{ create_equipment_category_key('u.equipment') }} = m.equipment

)

-- add order by clause for iceberg table sorting crterion
select * from equipment_category_col order by fault_occurred_at asc
