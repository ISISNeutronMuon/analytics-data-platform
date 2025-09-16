with source as (

    select * from {{ source('src_accelerator_sharepoint', 'edr_equipment_mapping') }}

),

renamed as (

    select

        equipment_name_src,
        equipment_name_clean

    from source

)

select * from renamed
