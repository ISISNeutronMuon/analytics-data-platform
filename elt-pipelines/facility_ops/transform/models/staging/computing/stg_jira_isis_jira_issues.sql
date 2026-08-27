with source as (

    select * from {{ source('computing_jira', 'isis_jira_issues') }}

),

cleaned as (

    select

      issue_key as issue_key,
      issue_type as issue_type,
      project_name as project_name,
      TRIM(REPLACE("status", '(migrated)', '')) as "status",
      TRIM(REPLACE(priority, '(migrated)', '')) as priority,
      created as created_at,
      updated as updated_at,
      teams


    from source

)

select * from cleaned
