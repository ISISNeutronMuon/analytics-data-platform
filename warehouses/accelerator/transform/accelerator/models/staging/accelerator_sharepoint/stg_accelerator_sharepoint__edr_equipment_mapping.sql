with source as (

    select * from {{ source('src_accelerator_sharepoint', 'edr_equipment_mapping') }}

),

renamed_and_deduped as (

    select

      -- After normalize there may be duplicate entries. Deduplicate by equipment_name
      any_value({{ normalize_whitespace('equipment_name') }}) as equipment_name,
      any_value({{ normalize_whitespace('equipment_category') }}) as equipment_category

    from source
    group by equipment_name

)

select * from renamed_and_deduped
