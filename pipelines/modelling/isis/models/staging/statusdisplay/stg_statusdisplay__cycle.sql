-- Simple join to denormalize the cycles and cycles__phases tables
-- that are not much use as separated table.

{# Denormalize cycles & cycles__phases to include the cyce label for each phase #}

with

base_cycles as (

  select * from {{ ref('base_statusdisplay__cycles') }}

),

base_cycles__phases as (

  select * from {{ ref('base_statusdisplay__cycles__phases') }}

),

join_cycle_labels_and_phases as (

  select
    cycle_name,
    started_at,
    ended_at,
    type,
    target

  from base_cycles
  join base_cycles__phases on base_cycles.dlt_id = base_cycles__phases.dlt_cycles_id

)

select * from join_cycle_labels_and_phases
order by started_at asc
