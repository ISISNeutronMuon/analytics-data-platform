-- This dataset contains equipment downtime records from 1996 until 2024 and includes records
-- also captured by Opralog. The Opralog epoch is defined as a project variable.
with source as (

    select * from {{ source('src_accelerator_sharepoint', 'equipment_downtime_data_11_08_24') }}

),

renamed as (

    select
        equipment,
        downtime_minutesx as downtime_mins,
        -- fault_date column has a full timestamp after the Opralog epoch but only the date
        -- before the epoch.
        case
          when fault_date like '%T%' then from_iso8601_timestamp(fault_date)
          else with_timezone(to_timestamp(fault_date, 'yyyy-mm-dd hh24:mi:ss'), 'Europe/London')
        end as fault_date,
        -- fault_time is just a time before the opralog_epoch but a full timestamp after
        fault_time,
        {{ identifier('group') }},
        fault_description,
        managerscomments as managers_comments

    from source

),

cropped as (

    select
        equipment,
        downtime_mins,
        date(fault_date) as fault_date,
        cast(parse_datetime(cast(date(fault_date) as varchar) || ' ' || fault_time, 'yyyy-MM-dd HH:mm:ss') as timestamp(6)) as fault_occurred_at,
        "group",
        fault_description,
        managers_comments

    from renamed
    where fault_date < date('{{ var("opralog_epoch") }}')

)

select * from cropped
