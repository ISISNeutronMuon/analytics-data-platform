with times_in_status as (
  select * from {{ ref('int_times_in_status') }}
),

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
