-- The 'phases' field in the statusdisplay cycles resource is a list of values. The ingestion
-- framework creates 2 tables from the single statusdisplay cycles resource linked by a key.
-- The cycles tables contains only the cycle id & label along with a primary key referenced
-- in the secondary cycles__phases table where information describing a particular phase of a
-- cycle is stored. Each row in the cycles__phases table corrseponds to a section of time within
-- a given cycle

with source as (

    select * from {{ source('src_statusdisplay', 'cycles') }}

),

renamed as (

    select
        _dlt_id as dlt_id,
        label as name

    from source

)

select * from renamed
