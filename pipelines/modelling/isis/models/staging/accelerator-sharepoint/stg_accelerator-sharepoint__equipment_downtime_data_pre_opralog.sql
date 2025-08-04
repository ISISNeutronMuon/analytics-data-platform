-- This dataset contains equipment downtime records from 1996 until 2024 and includes records
-- also captured by Opralog. The Opralog epoch is defined as a project variable.
with source as (

    select * from {{ source('src_accelerator_sharepoint', 'equipment_downtime_data_11_08_24') }}

),

cropped as (

    select
        equipment,
        downtime_minutesx,
        -- fault_date column has a full timestamp after the Opralog epoch a mix of timestamp
        -- with date and 00:00:00 portion or just the date. Chop the date out
        substring(fault_date, 1, 10) as fault_date_str,
        -- keep the time portion of any possible string
        substring(fault_time, -8) as fault_time_str,
        {{ identifier('group') }},
        fault_description,
        managerscomments

    from source
    where date(substring(fault_date, 1, 10)) < date('{{ var("opralog_epoch_date") }}')

),

renamed as (

    select
        equipment,
        downtime_minutesx as downtime_mins,
        date(fault_date_str) as fault_date,
        -- fault_time is just a time before the opralog_epoch but a full timestamp after
        cast(parse_datetime(fault_date_str || ' ' || fault_time_str, 'yyyy-MM-dd HH:mm:ss') as timestamp(6)) at time zone 'UTC' as fault_occurred_at,
        {{ identifier('group') }},
        fault_description,
        managerscomments as managers_comments

    from cropped

)

select * from renamed
