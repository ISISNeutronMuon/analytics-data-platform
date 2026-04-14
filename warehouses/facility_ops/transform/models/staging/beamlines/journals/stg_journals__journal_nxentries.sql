with source as (

    select * from {{ source('beamlines_journals', 'journal_nxentries') }}

),

renamed as (

    select

        duration,
        end_time,
        case
          when event_mode > 0.0 then true
          else false
        end as event_mode,
        experiment_identifier,
        good_frames,
        instrument_name,
        isis_cycle,
        monitor_sum,
        npratio_average,
        number_detectors,
        number_periods,
        number_spectra,
        number_time_regimes,
        proton_charge,
        raw_frames,
        run_number,
        start_time,
        total_mevents * 1000000 as total_events,

    from source

)

select * from renamed
