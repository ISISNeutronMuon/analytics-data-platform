{{
  config(
    properties={
      "partitioning": "ARRAY['beamline']",
    },
    on_table_exists = 'drop'
)
}}

with

monitor_peaks as (

    select * from {{ ref('stg_moderator_performance__monitor_peaks') }}

)

select

    beamline,
    run_start,
    peak_centre

from monitor_peaks
