{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select
        resources.resource_id as id,
        name.name as name,
        regexp_extract(labels.labels, '^\s*') as facility
    from {{ source('fase_proposal_registry', 'assignable_resource') }} resources
    join {{ source('fase_proposal_registry', 'resource_name') }} name
        on resources.resource_id = name.resource_resource_id
       and name.is_primary = 1
    join {{ source('fase_proposal_registry', 'resource_labels') }} labels
        on resources.resource_id = labels.resource_resource_id
    where labels.labels in ('isis instrument', 'hpl laser', 'lsf laser', 'artemis laser')

),

cleaned as (

    select
        id,
        name,
        facility
    from source

)

select * from cleaned
