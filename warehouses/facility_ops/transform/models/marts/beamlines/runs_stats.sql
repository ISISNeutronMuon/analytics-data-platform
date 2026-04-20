{{ config(
  on_table_exists = 'drop'
) }}

select

  instrument,
  count(run_number) as total_runs,
  count(case when event_mode = true then run_number end) as total_event_mode_runs,
  count(case when event_mode = true then run_number end) * 100.0 / count(run_number) as percentage_event_runs,
  avg(duration_mins) as average_duration_mins

from
  {{ ref('runs') }}

group by
  instrument
