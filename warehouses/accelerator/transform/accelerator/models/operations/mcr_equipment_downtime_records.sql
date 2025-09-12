{{
  config(
    properties={
      "partitioning": "ARRAY['cycle_name']",
    },
    on_table_exists = 'drop'
)
}}

with

mcr_downtime_records_sharepoint as ( select * from {{ ref('int_mcr_downtime_records_sharepoint') }} ),
mcr_downtime_records_opralogweb as ( select * from {{ ref('int_mcr_downtime_records_opralogweb') }} ),

mcr_downtime_records_opralogweb_after_sharepoint as (

  select * from mcr_downtime_records_opralogweb
  where fault_occurred_at > (select max(fault_occurred_at) from mcr_downtime_records_sharepoint)

),

unioned as (

  select * from  mcr_downtime_records_sharepoint

  union

  select * from mcr_downtime_records_opralogweb_after_sharepoint

)


select * from unioned order by fault_occurred_at asc
