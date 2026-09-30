{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        proposer.summary_proposal_id as proposal_id,
        proposer.un as un
    from {{ source('fase_proposal_registry', 'proposer') }} proposer
    join {{ source('fase_proposal_registry', 'unique_role_assignment') }} role
        on proposer.proposer_id = role.proposer_id
    where role.role = 'principal-investigator'

),

cleaned as (

    select
        proposal_id,
        un
    from source

)

select * from cleaned
