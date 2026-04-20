{{ config(
  on_table_exists = 'drop'
) }}

select
  *
from
  {{ ref('stg_scheduler__scheduled_experiments') }}
