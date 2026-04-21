select
  cast(1 as integer) as additional_column_id,
  cast('Equipment' as varchar) as column_title

union all

select
  cast(2 as integer) as additional_column_id,
  cast('Group' as varchar) as column_title

union all

select
  cast(3 as integer) as additional_column_id,
  cast('Lost Time' as varchar) as column_title

union all

select
  cast(4 as integer) as additional_column_id,
  cast('Group Leader comments' as varchar) as column_title
