{{ config(
  on_table_exists = 'drop'
) }}

select
  year(started_at) as year,
  sum(date_diff('hour', started_at, ended_at)) / 24.0 as duration_days
from
  {{ ref('cycles') }}
where
  phase = 'user-time'
group by
  year(started_at)
