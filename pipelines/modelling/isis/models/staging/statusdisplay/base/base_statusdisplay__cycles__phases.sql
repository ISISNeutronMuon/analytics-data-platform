--  See base_statusdisplay__cycles.sql for a description of why this table exists.

with source as (

    select * from {{ source('src_statusdisplay', 'cycles__phases') }}

),

renamed as (

    select
        type as phase,
        target,
        start as started_at,
        {{ identifier("end") }} as ended_at,
        _dlt_parent_id as dlt_cycles_id

    from source

)

select * from renamed
