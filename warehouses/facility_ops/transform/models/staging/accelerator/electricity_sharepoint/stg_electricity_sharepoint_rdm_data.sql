with source as (

    select * from {{ source('source_accelerator_electricity_sharepoint', 'rdm_data') }}

),

renamed as (

    select

      date_time as power_measured_at,
      isis_elec_total_power_mwx as total_isis_power_mw

    from source

)

select * from renamed
