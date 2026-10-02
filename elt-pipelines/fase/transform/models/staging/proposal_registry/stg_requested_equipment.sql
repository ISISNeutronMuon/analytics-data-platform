{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        request.summary_proposal_id as proposal_id,
        equipment.name as name,
        request.quantity as quantity,
        request.units as units
    from {{ source('fase_proposal_registry', 'resource_request') }} request
    join {{ ref('stg_experiment_equipment') }} equipment
        on request.resource_resource_id = equipment.id

),

cleaned as (

    select
        proposal_id,
        name,
        quantity,
        units
    from source

)

select * from cleaned
