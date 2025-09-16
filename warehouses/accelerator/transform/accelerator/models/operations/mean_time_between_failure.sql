{{
  config(
    properties={
      "partitioning": "ARRAY['cycle_name']",
    },
    on_table_exists = 'drop',
)
}}

with

cycles_start_end as ( select * from {{ ref('int_cycles_start_end') }} ),
downtime_records as ( select * from {{ ref('mcr_equipment_downtime_records') }} ),

equipment_up_at_col as (

  select

    cycle_name,
    equipment,
    fault_occurred_at,
    downtime_mins,
    fault_occurred_at + (interval '1' minute * downtime_mins) as equipment_up_at

  from

    downtime_records d

  left join cycles_start_end c on d.cycle_name = c.name
),

uptime_col as (

  select

    *,
    date_diff('minute',
      lag(equipment_up_at, 1,
          (select started_at from cycles_start_end where {{ adapter.quote('name') }} = cycle_name)
         )
         over
         (partition by cycle_name, equipment order by fault_occurred_at), fault_occurred_at) as uptime_mins

  from equipment_up_at_col
),

mean_time_between_failures as (

  select

    any_value(cycle_name) as cycle_name,
    equipment,
    sum(uptime_mins)/count(fault_occurred_at) as mtbf_mins

  from uptime_col
  where cycle_name is not null
  group by cycle_name, equipment

)

select * from mean_time_between_failures
