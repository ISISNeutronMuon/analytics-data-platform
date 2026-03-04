{{
  config(
    on_table_exists = 'drop',
    materialized = 'view'
)
}}

select

  distinct(equipment) as uncategorized_equipment

from

  {{ ref('mcr_equipment_downtime_records') }}

where equipment_category is null
