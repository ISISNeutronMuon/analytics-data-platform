{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        prop.proposal_id as proposal_id,
        prop.reference_number as reference_number,
        prop.title as title,
        prop.withdrawn as withdrawn,
        round.display_name as round,
        round.round_id as round_id,
        route.display_name as route,
        route.access_route_id as route_id,
        route.facility as facility,
        pi.un as pi_un,
        request.name as requested_equipment,
        request.quantity as requested_time,
        request.units as requested_time_units,
        allocation.name as allocated_equipment,
        allocation.quantity as allocated_time,
        allocation.units as allocated_time_units,
        prop.submission_date as submission_date
    from {{ source('fase_proposal_registry', 'proposal_summary') }} prop
    join {{ source('fase_proposal_registry', 'round') }} round
        on prop.round_round_id = round.round_id
    join {{ source('fase_proposal_registry', 'access_route') }} route
        on round.access_route_id = route.access_route_id
    left join {{ ref('stg_principal_investigator') }}   pi
        on prop.proposal_id = pi.proposal_id
    left join {{ ref('stg_requested_equipment') }}   request
        on prop.proposal_id = request.proposal_id
    left join {{ ref('stg_allocated_equipment') }}  allocation
        on prop.proposal_id = allocation.proposal_id

),

cleaned as (

    select
        proposal_id,
        reference_number,
        title,
        withdrawn,
        round,
        round_id,
        route,
        route_id,
        facility,
        pi_un,
        requested_equipment,
        requested_time,
        requested_time_units,
        allocated_equipment,
        allocated_time,
        allocated_time_units,
        submission_date
    from source

)

select * from cleaned
