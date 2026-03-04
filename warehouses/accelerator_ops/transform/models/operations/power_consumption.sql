{{
  config(
    properties={
      "partitioning": "ARRAY['year(power_measured_at)']",
    },
    on_table_exists = 'drop'
)
}}

with

staged as (

  select

    power_measured_at,
    total_isis_power_mw

  from {{ ref('stg_electricity_sharepoint_rdm_data') }}

)

select * from staged
