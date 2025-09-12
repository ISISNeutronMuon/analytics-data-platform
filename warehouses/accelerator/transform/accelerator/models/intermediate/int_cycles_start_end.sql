with cycle_start_end as (

  select

    {{ adapter.quote('name') }},
    min(started_at) as started_at,
    max(ended_at) as ended_at

  from {{ ref('int_cycles') }}
  group by {{ adapter.quote('name') }}

)

select * from cycle_start_end
