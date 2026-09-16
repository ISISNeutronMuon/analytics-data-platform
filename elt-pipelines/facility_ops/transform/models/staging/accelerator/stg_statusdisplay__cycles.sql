with
source as (
    select * from {{source('accelerator_statusdisplay', 'elt_cycles')}}
),

status_display as (
    select
        source.label as {{ adapter.quote("name") }},
        cast(json_extract_scalar(phase, '$.start') as timestamp) as started_at,
        cast(json_extract_scalar(phase, '$.end') as timestamp) as ended_at,
        json_extract_scalar(phase, '$.type') as phase,
        cast(json_extract_scalar(phase, '$.target') as int) as {{ adapter.quote("target") }}
        from source
        cross join unnest(cast(source.phases as array(json))) as u(phase)
)

select * from status_display
