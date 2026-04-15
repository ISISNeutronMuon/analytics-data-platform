[0m07:40:49  Running with dbt=1.11.2
[0m07:40:50  Registered adapter: trino=1.10.1
[0m07:40:50  Unable to do partial parsing because saved manifest not found. Starting full parse.
[0m07:40:51  Found 20 models, 28 data tests, 14 sources, 614 macros


with source as (

    select * from {{ source('fase_scheduler', 'scheduled_experiments') }}

),

renamed as (

    select

    from source

)

select * from renamed
