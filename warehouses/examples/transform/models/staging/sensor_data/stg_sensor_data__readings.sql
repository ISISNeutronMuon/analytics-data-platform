select
    sensor_id,
    timestamp,
    cast(temperature as double) as temperature_celsius,
    cast(humidity as double) as relative_humidity_pct
from {{ source('examples_sensor_data', 'readings') }}
