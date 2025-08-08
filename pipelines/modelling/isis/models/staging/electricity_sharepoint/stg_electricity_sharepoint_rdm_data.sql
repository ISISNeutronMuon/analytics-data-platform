with source as (

    select * from {{ source('src_electricity_sharepoint', 'rdm_data') }}

),

renamed as (

    select
      {{ parse_utc_timestamp(date, 'dd/MM/yy', time) }} as power_consumption_at,
      isis_elec_total_power_mwx as total_isis_power_mw

    from source

)

select * from renamed
