-- Get the differences between the status. No need for final status, so this query is perfectly suitable
with status_to_from as (
  select
    issue_key,
    from_status as status,
    lag(changed_at) over (partition by issue_key order by changed_at) as status_from,
    changed_at as status_to
    from {{ ref('stg_jira_issue_status_changelogs') }} as changelogs
),

-- Populate null values of status from with the issue creation date. Join required
nn_status_to_from as (
  select
    status_to_from.issue_key,
    status_to_from.status,
    COALESCE(status_to_from.status_from, issues.created) as status_from,
    status_to_from.status_to
    from status_to_from
    inner join facility_ops_landing.computing_jira.isis_jira_issues as issues
    on status_to_from.issue_key = issues.issue_key
),

-- Subtract to and from date
status_durations as (
  select
    issue_key,
    status,
    date_diff('second', status_from, status_to) as status_duration
    from nn_status_to_from
),

-- Aggregate similar statuses and add their durations
times_in_status as (
  select
    issue_key,
    status,
    sum(status_duration) as time_in_status
    from status_durations
    group by issue_key,
    status
)

select * from times_in_status
