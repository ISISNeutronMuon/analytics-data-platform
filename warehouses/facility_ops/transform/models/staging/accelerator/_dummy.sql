-- For testing we need the schema to exist to populate tables with fake data.
-- This model references nothing so will not cause an error when running dbt.
-- It's a view so doesn't take up any space.
{{ config(
    tags=["unit_test_setup"]
) }}

select 1 as _id
