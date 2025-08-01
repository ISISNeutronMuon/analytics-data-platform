{{
  config(
    on_table_exists = 'drop'
)
}}

with

staged as (

  select * from {{ ref('stg_statusdisplay__cycle') }}

),

staged_discard_target as (

  select
    name,
    started_at,
    ended_at,
    phase

  from staged
  group by name, started_at, ended_at, phase

)

select * from staged_discard_target
