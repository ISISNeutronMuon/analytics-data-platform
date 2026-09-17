with source as (

    select * from {{ source('accelerator_sharepoint', 'equipment_downtime_data_11_08_24') }}

),

cropped as (

    select
        equipment,
        "user run" as user_run,
        "downtime (minutes)" as downtime_minutesx,
        -- fault_date column has a full timestamp after the Opralog epoch a mix of timestamp
        -- with date and 00:00:00 portion or just the date. Chop the date out
        substring(faultdate, 1, 10) as fault_date_str,
        -- keep the time portion of any possible string
        substring(faulttime, -8) as fault_time_str,
        {{ adapter.quote('group') }},
        faultdescription,
        managerscomments

    from source

),

renamed as (

    select
        equipment,

        -- Reformat into four-digit year
        case
            when user_run like '.%' then replace(user_run, '.', '19')
            else concat('20', user_run)
        end as cycle_name,

        downtime_minutesx as downtime_mins,
        date(fault_date_str) as fault_date,

        -- Desktop Opralog used local time rather than UTC. Convert to UTC here.
        {{ parse_utc_timestamp('fault_date_str', 'yyyy-MM-dd', 'fault_time_str', src_timezone='Europe/London') }} as fault_occurred_at,

        {{ adapter.quote('group') }},
        faultdescription as fault_description,
        managerscomments as managers_comments

    from cropped

)

select
equipment,
cycle_name,
downtime_mins,
fault_date,
cast(fault_occurred_at as timestamp(6)) as fault_occurred_at,
fault_description,
managers_comments
from renamed
