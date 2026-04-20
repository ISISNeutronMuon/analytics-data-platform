{{
  config(
    properties={
      "partitioning": "ARRAY['beamline']",
    },
    on_table_exists = 'replace'
)
}}

with

monitor_peaks as (

    select * from {{ ref('stg_beamlines__monitor_peaks') }}

)

select

    beamline,
    run_number,
    cycle_name,
    run_start,
    peak_centre

from monitor_peaks
