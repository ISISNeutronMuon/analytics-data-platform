with source as (

    select * from {{ source('src_accelerator_sharepoint', 'edr_equipment_mapping') }}

),

renamed as (

    select

        trim(equipment_name_src) as equipment_name_src,
        trim(equipment_name_clean) as equipment_name_clean

    from source

)

select * from renamed
