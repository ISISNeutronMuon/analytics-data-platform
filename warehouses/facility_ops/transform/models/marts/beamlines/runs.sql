{{ config(
    properties={
      "partitioning": "ARRAY['year(started_at)']",
    },
  on_table_exists = 'drop'
) }}

select

  *

from

  {{ ref('stg_journals__journal_nxentries') }}
