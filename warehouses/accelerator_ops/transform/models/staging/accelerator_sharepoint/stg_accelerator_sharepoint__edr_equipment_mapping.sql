with source as (

    select * from {{ source('src_accelerator_sharepoint', 'edr_equipment_mapping') }}

),

equipment_lowercased_and_deduped as (

    select

      -- After normalize there may be duplicate entries. Deduplicate by lower case equipment
      {{ create_equipment_category_key('equipment_name') }} as equipment,
      any_value(equipment_category) as equipment_category

    from source
    group by {{ create_equipment_category_key('equipment_name') }}

)

select * from equipment_lowercased_and_deduped
