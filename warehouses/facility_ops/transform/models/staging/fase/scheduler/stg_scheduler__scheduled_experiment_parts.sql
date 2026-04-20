{{ config(
    event_time='started_at'
) }}

with source as (

    select * from {{ source('fase_scheduler', 'scheduled_experiment_parts') }}

),

renamed as (

    select

        instrument,
        ref_number as rb_number,
        part as part_number,
        "start_date" as started_at,
        end_date as ended_at,
        planned_hours,
        delivered_hours

    from source

)

select * from renamed
