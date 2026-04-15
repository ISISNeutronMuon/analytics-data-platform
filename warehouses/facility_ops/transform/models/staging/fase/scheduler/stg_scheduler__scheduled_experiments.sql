with source as (

    select * from {{ source('fase_scheduler', 'scheduled_experiments') }}

),

renamed as (

    select

        instrument,
        ref_number as rb_number,
        part as part_number,
        start_date as start_time,
        end_date as end_time,
        planned_hours,
        delivered_hours,
        variance_against_plan_hoursx as variance_against_plan_hours

    from source

)

select * from renamed
