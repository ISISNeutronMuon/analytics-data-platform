with source as (

    select * from {{ source('src_electricity_sharepoint', 'rdm_data') }}

),

renamed as (

    select
      cast(parse_datetime(date || ' ' || time, 'dd/MM/yy HH:mm:ss') as timestamp(6)) at time zone 'UTC' as power_consumption_at,
      isis_elec_total_power_mwx as total_isis_power_mw

    from source

)

select * from renamed
