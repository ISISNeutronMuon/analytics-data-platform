select
    event_id,
    event_type,
    occurred_at,
    description
from {{ source('examples_sensor_data', 'events') }}
