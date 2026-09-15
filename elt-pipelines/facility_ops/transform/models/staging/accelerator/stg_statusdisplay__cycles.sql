with
source as (
    select * from {{source('accelerator_statusdisplay', 'elt_cycles')}}
),

status_display as (
    select
        source.label as name,
        json_extract_scalar(phase, '$.start') as started_at,
        json_extract_scalar(phase, '$.end') as ended_at,
        json_extract_scalar(phase, '$.type') as phase,
        json_extract_scalar(phase, '$.target') as target
        from source
        cross join unnest(cast(source.phases as array(json))) as u(phase)
)

select * from status_display
