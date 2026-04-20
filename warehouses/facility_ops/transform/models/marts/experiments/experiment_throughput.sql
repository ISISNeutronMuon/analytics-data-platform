{{ config(
  on_table_exists = 'drop'
) }}

select
  sb.year as year,
  count(distinct se.rb_number) / sb.duration_days as expts_per_day
from
  {{ ref('total_scheduled_user_beam_days_by_year') }} sb
  join {{ ref('scheduled_experiment_parts') }} se
  on sb.year = year(
    se.started_at
  )
group by
  sb.year,
  sb.duration_days
