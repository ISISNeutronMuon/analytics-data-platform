
-- 3. Get the differences between the status. No need for final status, so this query is perfectly suitable
with status_to_from as (
  select
    issue_key,
    from_status as status,
    lag(changed_at) over (partition by issue_key order by changed_at) as status_from,
    changed_at as status_to
    from {{ ref('stg_jira_issue_status_changelogs') }} as changelogs
)

select * from status_to_from
