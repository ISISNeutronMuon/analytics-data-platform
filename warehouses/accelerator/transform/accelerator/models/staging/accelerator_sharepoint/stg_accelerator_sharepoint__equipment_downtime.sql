-- This dataset contains equipment downtime records from 1996 until 2024 and includes records
-- also captured by Opralog. The Opralog epoch is defined as a project variable.
with source as (

    select * from {{ source('src_accelerator_sharepoint', 'equipment_downtime_data_11_08_24') }}

),

cropped as (

    select
        equipment,
        user_run,
        downtime_minutesx,
        -- fault_date column has a full timestamp after the Opralog epoch a mix of timestamp
        -- with date and 00:00:00 portion or just the date. Chop the date out
        substring(fault_date, 1, 10) as fault_date_str,
        -- keep the time portion of any possible string
        substring(fault_time, -8) as fault_time_str,
        {{ adapter.quote('group') }},
        fault_description,
        managerscomments

    from source

),

renamed as (

    select
        equipment,

        -- Reformat into cycle name including four-digit year
        case
            when user_run like '.%' then replace(user_run, '.', '19')
            else concat('20', user_run)
        end as cycle_name,

        downtime_minutesx as downtime_mins,
        date(fault_date_str) as fault_date,
        -- fault_time is just a time before the opralog_epoch but a full timestamp after
        {{ parse_utc_timestamp('fault_date_str', 'yyyy-MM-dd', 'fault_time_str') }} as fault_occurred_at,
        {{ adapter.quote('group') }},
        fault_description,
        managerscomments as managers_comments

    from cropped

)

select * from renamed
