with source as (

    select * from {{ source('src_accelerator_sharepoint', 'edr_equipment_mapping') }}

),

renamed as (

    select

      {{ normalize_whitespace('equipment_name') }} as equipment_name,
      {{ normalize_whitespace('equipment_category') }} as equipment_category

    from source

)

select * from renamed
