with source as (

    select * from {{ source('source_beamlines_moderator_performance', 'monitor_peaks') }}

),

renamed as (

    select

        beamline,
        run_number,
        cycle_name,
        run_start,
        proton_charge,
        peak_centre,
        peak_centre_error,
        peak_amplitude,
        peak_amplitude_error,
        peak_sigma,
        peak_sigma_error

    from source

)

select * from renamed
