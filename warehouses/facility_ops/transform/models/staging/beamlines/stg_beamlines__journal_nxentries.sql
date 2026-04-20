with source as (

    select * from {{ source('beamlines_journals', 'journal_nxentries') }}

),

renamed as (

    select

        instrument_name as instrument,
        experiment_identifier as rb_number,
        run_number,
        cast(from_iso8601_timestamp(start_time) as timestamp(6)) as started_at,
        cast(from_iso8601_timestamp(end_time) as timestamp(6)) as ended_at,
        duration as duration_mins,
        case
          when event_mode > 0 then true
          else false
        end as event_mode,
        total_mevents * 1000000 as total_events,
        proton_charge,
        raw_frames,
        good_frames

    from source

)

select * from renamed
