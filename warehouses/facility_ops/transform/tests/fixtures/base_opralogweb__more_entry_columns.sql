select

  cast(100 as integer) as entry_id,
  cast('Equipment 100' as varchar) as string_data,
  cast(null as double) as number_data,
  cast(1 as integer) as additional_column_id

union all

select

  cast(100 as integer) as entry_id,
  cast('Group 100' as varchar) as string_data,
  cast(null as double) as number_data,
  cast(2 as integer) as additional_column_id

union all

select

  cast(100 as integer) as entry_id,
  cast(null as varchar) as string_data,
  cast(4.2 as double) as number_data,
  cast(3 as integer) as additional_column_id

union all

select

  cast(101 as integer) as entry_id,
  cast('Equipment 101' as varchar) as string_data,
  cast(null as double) as number_data,
  cast(1 as integer) as additional_column_id

union all

select

  cast(101 as integer) as entry_id,
  cast('Group 101' as varchar) as string_data,
  cast(null as double) as number_data,
  cast(2 as integer) as additional_column_id

union all

select

  cast(101 as integer) as entry_id,
  cast(null as varchar) as string_data,
  cast(5.3 as double) as number_data,
  cast(3 as integer) as additional_column_id

union all

select

  cast(102 as integer) as entry_id,
  cast('Equipment 102' as varchar) as string_data,
  cast(null as double) as number_data,
  cast(1 as integer) as additional_column_id

union all

select

  cast(102 as integer) as entry_id,
  cast('Group 102' as varchar) as string_data,
  cast(null as double) as number_data,
  cast(2 as integer) as additional_column_id

union all

select

  cast(102 as integer) as entry_id,
  cast(null as varchar) as string_data,
  cast(4.3 as double) as number_data,
  cast(3 as integer) as additional_column_id
