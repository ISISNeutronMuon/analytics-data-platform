with times_in_status as (
  select * from {{ ref('int_times_in_status') }}
),

time_in_status_data_driven_facility as (
    select
    issue_key,
    MAX(
        case
        when status = 'analyzing' then time_in_status
        else null
        end
    ) as time_in_analysing_secs,
    MAX(
        case
        when status = 'backlog' then time_in_status
        else null
        end
    ) as time_in_backlog_secs,
    MAX(
        case
        when status = 'done' then time_in_status
        else null
        end
    ) as time_in_done_secs,
    MAX(
        case
        when status = 'funnel' then time_in_status
        else null
        end
    ) as time_in_funnel_secs,
    MAX(
        case
        when status = 'implementing' then time_in_status
        else null
        end
    ) as time_in_implementing_secs,
    MAX(
        case
        when status = 'implementing (mvp)' then time_in_status
        else null
        end
    ) as time_in_implementing_mvp_secs,
    MAX(
        case
        when status = 'implementing (persevere)' then time_in_status
        else null
        end
    ) as time_in_implementing_persevere_secs,
    MAX(
        case
        when status = 'in progress' then time_in_status
        else null
        end
    ) as time_in_in_progress_secs,
    MAX(
        case
        when status = 'portfolio backlog' then time_in_status
        else null
        end
    ) as time_in_portfolio_backlog_secs,
    MAX(
        case
        when status = 'ready' then time_in_status
        else null
        end
    ) as time_in_ready_secs,
    MAX(
        case
        when status = 'reviewing' then time_in_status
        else null
        end
    ) as time_in_reviewing_secs,
    MAX(
        case
        when status = 'selected for development' then time_in_status
        else null
        end
    ) as time_in_selected_for_development_secs
    from times_in_status
    group by issue_key

)
select * from time_in_status_data_driven_facility
