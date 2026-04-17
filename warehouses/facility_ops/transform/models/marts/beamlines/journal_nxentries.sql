{{ config(
  on_table_exists = 'drop'
) }}

select

  *

from

  {{ ref('stg_journals__journal_nxentries') }}
