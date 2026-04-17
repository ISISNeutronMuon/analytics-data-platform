with source as (

    select * from {{ source('beamlines_journals', 'journal_nxentries') }}

),

renamed as (

    select

        instrument_name as instrument,
        experiment_identifier as rb_number,
        run_number,
        start_time,
        end_time,
        duration as duration_mins,
        case
          when event_mode > 0 then true
          else false
        end as event_mode,
        total_mevents * 1000000 as total_events,
        proton_charge,
        raw_frames,
        good_frames,
        monitor_sum,
        number_detectors,
        number_periods,
        number_spectra,
        number_time_regimes

    from source

)

select * from renamed
