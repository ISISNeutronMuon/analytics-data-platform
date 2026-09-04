
-- 3. Get the differences between the status. No need for final status, so this query is perfectly suitable
with status_to_from as (
  select
    issue_key,
    from_status as status,
    lag(changed_at) over (partition by issue_key order by changed_at) as status_from,
    changed_at as status_to
    from {{ ref('stg_jira_issue_status_changelogs') }} as changelogs
),

-- 4. Populate null values of status from with the issue creation date. Join required
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

-- 5. Subtract to and from date
status_durations as (
  select
    issue_key,
    status,
    date_diff('second', status_from, status_to) as status_duration
    from nn_status_to_from
),

-- 6. Aggregate similar statuses and add their durations
times_in_status as (
  select
    issue_key,
    status,
    sum(status_duration) as time_in_status
    from status_durations
    group by issue_key,
    status
),

-- 7. Store these in time_in fields. THIS SHOULD BE THE ONLY THING IN THIS PARTICULAR FILE. THE ABOVE SHOULD BE EXTRACTED TO ANOTHER FILE
time_in_status_scientific_software as (
  select
  issue_key,
  MAX(case
    when status = 'funnel' then time_in_status
    else null
  end) as time_in_funnel_secs,
  MAX(case
    when status = 'reviewing' then time_in_status
    else null
  end) as time_in_reviewing_secs,
  MAX(case
    when status = 'analysing' then time_in_status
    else null
  end) as time_in_analysing_secs,
  MAX(case
    when status = 'ready' then time_in_status
    else null
  end) as time_in_ready_secs,
  MAX(case
    when status = 'blocked' then time_in_status
    else null
  end) as time_in_blocked_secs,
  MAX(case
    when status = 'implementing' then time_in_status
    else null
  end) as time_in_implementing_secs,
  MAX(case
    when status = 'implementing (mvp)' then time_in_status
    else null
  end) as time_in_implementing_mvp_secs,
  MAX(case
    when status = 'implementing (persevere)' then time_in_status
    else null
  end) as time_in_implementing_persevere_secs
  from times_in_status
  group by issue_key
)

select * from time_in_status_scientific_software
