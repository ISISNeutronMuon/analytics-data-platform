{{ config(
  on_table_exists = 'drop'
) }}

select

  instrument,
  count(run_number) as count_event_mode_runs

from
  {{ ref('journal_nxentries') }}

where
  event_mode
group by
  instrument
