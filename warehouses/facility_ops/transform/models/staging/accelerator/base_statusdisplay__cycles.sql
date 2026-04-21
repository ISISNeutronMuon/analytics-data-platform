with source as (

    select * from {{ source('accelerator_statusdisplay', 'cycles') }}

),

renamed as (

    select

        _dlt_id as dlt_id,
        label as name

    from source

)

select * from renamed
