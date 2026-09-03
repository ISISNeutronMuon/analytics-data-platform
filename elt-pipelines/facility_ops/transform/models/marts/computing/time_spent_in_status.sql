with

staged as (

  select

    issue_key,
    time_in_funnel_secs,
    time_in_reviewing_secs,
    time_in_analysing_secs,
    time_in_ready_secs,
    time_in_blocked_secs,
    time_in_implementing_secs,
    time_in_implementing_mvp_secs,
    time_in_implementing_persevere_secs

  from {{ ref('stg_jira_issue_status_changelogs') }} as changelogs
  inner join {{ ref('stg_jira_isis_jira_issues')}} as issues on changelogs.issue_key = issues.issue_key

)

select * from staged
