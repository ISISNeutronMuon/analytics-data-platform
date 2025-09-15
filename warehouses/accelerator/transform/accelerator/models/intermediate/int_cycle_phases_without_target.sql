{{
  config(
    on_table_exists = 'drop'
)
}}

with

staged as (

  select * from {{ ref('stg_statusdisplay__cycles') }}

),

staged_discard_target as (

  select

    {{ adapter.quote('name') }},
    started_at,
    ended_at,
    phase

  from staged
  group by {{ adapter.quote('name') }}, started_at, ended_at, phase

)

select * from staged_discard_target
