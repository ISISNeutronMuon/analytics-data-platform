{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        review.summary_proposal_id as proposal_id,
        equipment.name as name,
        allocation.quantity as quantity,
        allocation.units as units
    from {{ source('fase_proposal_registry', 'resource_review') }} review
    join {{ source('fase_proposal_registry', 'resource_allocation') }} allocation
        on review.resource_review_id = allocation.review_resource_review_id
    join {{ ref('stg_experiment_equipment') }} equipment
        on allocation.resource_resource_id = equipment.id

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
