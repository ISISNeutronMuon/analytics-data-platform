{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='started_at',
    begin='2010-01-01',
    batch_size='year',
    properties={
      "partitioning": "ARRAY['instrument', 'year(started_at)']",
    }
) }}


select
  *
from
  {{ ref('stg_scheduler__scheduled_experiment_parts') }}
