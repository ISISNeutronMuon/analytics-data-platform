{{ config(
  on_table_exists = 'drop'
) }}

select

  year(start_time) as "year",
  avg(planned_hours) / 24. as "avg_scheduled_days",
  avg(delivered_hours) / 24. as "avg_delivered_days"

from
  {{ ref('scheduled_experiments') }}
group by
  year(start_time)
