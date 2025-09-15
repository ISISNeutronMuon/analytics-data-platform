{%- set OPRALOG_EPOCH = dbt.string_literal("2017-04-25") -%}
{%- set MCR_LOGBOOK = dbt.string_literal("MCR Running Log") -%}

with

staging_entries as ( select * from {{ ref('base_opralogweb__user_entries') }} ),

staging_chapter_entry as ( select * from {{ ref('base_opralogweb__chapter_entry') }} ),

staging_logbook_chapter as ( select * from {{ ref('base_opralogweb__logbook_chapter') }} ),

staging_logbooks as ( select * from {{ ref('base_opralogweb__logbooks') }} ),

staging_more_entry_columns as ( select * from {{ ref('base_opralogweb__more_entry_columns') }} ),

staging_additional_columns as ( select * from {{ ref('base_opralogweb__additional_columns') }} ),

denormalized as (
  select

    staging_entries.entry_id,
    staging_entries.fault_occurred_at,
    staging_entries.fault_date,
    staging_additional_columns.column_title,
    staging_more_entry_columns.string_data,
    staging_more_entry_columns.number_data,
    staging_entries.fault_description

  from
    staging_entries
    join staging_chapter_entry on staging_chapter_entry.entry_id = staging_entries.entry_id
    join staging_logbook_chapter on staging_logbook_chapter.logbook_chapter_no = staging_chapter_entry.logbook_chapter_no
    join staging_logbooks on staging_logbooks.logbook_id = staging_chapter_entry.logbook_id
    left outer join staging_more_entry_columns on staging_more_entry_columns.entry_id = staging_entries.entry_id
    left outer join staging_additional_columns on staging_additional_columns.additional_column_id = staging_more_entry_columns.additional_column_id

  where

    staging_logbooks.logbook_name = {{ MCR_LOGBOOK }}
    and staging_chapter_entry.logbook_id = staging_chapter_entry.principal_logbook
    and staging_additional_columns.column_title in ('Equipment', 'Group', 'Lost Time', 'Group Leader comments')
    and (
      staging_more_entry_columns.string_data is not null
      or staging_more_entry_columns.number_data is not null
    )
    and staging_entries.fault_date >= from_iso8601_date({{ OPRALOG_EPOCH }})
),

mcr_equipment_downtime as (
  select
    *
  from
    (
      select

        min(
          case
            column_title
            when 'Equipment' then string_data
          end
        ) as equipment,
        min(
          case
            column_title
            when 'Lost Time' then number_data
          end
        ) as downtime_mins,
        fault_date,
        fault_occurred_at,
        min(
          case
            column_title
            when 'Group' then string_data
          end
        ) as {{ adapter.quote('group') }},
        fault_description,
        min(
          case
            column_title
            when 'Group Leader comments' then string_data
          end
        ) as managers_comments
      from
        denormalized
      group by
        fault_occurred_at,
        fault_date,
        fault_description
    )
  where
    equipment is not null
    and downtime_mins is not null
    and {{ adapter.quote('group') }} is not null
)

select * from mcr_equipment_downtime
