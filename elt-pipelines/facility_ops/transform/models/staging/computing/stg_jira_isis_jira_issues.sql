with source as (

    select * from {{ source('computing_jira', 'isis_jira_issues') }}

),

cleaned as (

    select

      issue_key, issue_type, project_name, "status", REPLACE(priority, 'Highest (migrated)', 'Highest') as priority, created as created_at, updated as updated_at, teams


    from source

)

select * from cleaned
