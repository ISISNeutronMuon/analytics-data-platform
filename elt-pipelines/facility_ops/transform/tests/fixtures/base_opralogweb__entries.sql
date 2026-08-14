select

  cast(100 as integer) as entry_id,
  with_timezone(timestamp '2017-04-25 23:59:59', 'UTC') as fault_occurred_at,
  date '2017-04-24' as fault_date,
  cast('Comment 100' as varchar) as fault_description,
  false as logically_deleted

union all

select

  cast(101 as integer) as entry_id,
  with_timezone(timestamp '2024-01-01 00:00:01', 'UTC') as fault_occurred_at,
  date '2024-01-01' as fault_date,
  cast('Comment 101' as varchar) as fault_description,
  false as logically_deleted

union all

select

  cast(102 as integer) as entry_id,
  with_timezone(timestamp '2024-01-01 00:01:03', 'UTC') as fault_occurred_at,
  date '2024-01-01' as fault_date,
  cast('Deleted 102' as varchar) as fault_description,
  true as logically_deleted
