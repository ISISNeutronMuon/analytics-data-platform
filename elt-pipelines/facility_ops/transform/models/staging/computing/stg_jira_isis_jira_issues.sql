with source as (

    select * from {{ source('computing_jira', 'isis_jira_issues') }}

),

cleaned as (

    select

      TRIM(issue_key) as issue_key,
      TRIM(issue_type) as issue_type,
      TRIM(project_name) as project_name,
      TRIM(REPLACE("status", '(migrated)', '') as "status",
      TRIM(REPLACE(priority, '(migrated)', '')) as priority,
      TRIM(created) as created_at,
      TRIM(updated) as updated_at,
      teams


    from source

)

select * from cleaned
