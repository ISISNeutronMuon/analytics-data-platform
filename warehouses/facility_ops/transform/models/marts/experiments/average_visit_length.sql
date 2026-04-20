{{ config(
  on_table_exists = 'drop'
) }}

select

  year(started_at) as "year",
  avg(planned_hours) / 24. as "avg_scheduled_days",
  avg(delivered_hours) / 24. as "avg_delivered_days"

from
  {{ ref('scheduled_experiment_parts') }}
group by
  year(started_at)
