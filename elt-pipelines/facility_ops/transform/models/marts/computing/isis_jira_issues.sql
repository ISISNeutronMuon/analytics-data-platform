with staged as (
    select
        issue_key,
        issue_type,
        project_name,
        {{ adapter.quote('status') }},
        priority,
        created_at,
        updated_at,
        teams
    from {{ref('stg_jira_isis_jira_issues')}}
)

select * from staged
