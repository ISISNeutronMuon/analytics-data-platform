-- Parse separate date and time strings into a single UTC timestamp
{%- macro parse_utc_timestamp(date_col, date_format, time_col, time_format='HH:mm:ss', src_timezone='UTC') -%}
  {{ return(adapter.dispatch('parse_utc_timestamp')(date_col, date_format, time_col, time_format, src_timezone)) }}
{% endmacro %}


{%- macro trino__parse_utc_timestamp(date_col, date_format, time_col, time_format, src_timezone) -%}
  with_timezone(
    cast(
      parse_datetime({{ adapter.quote(date_col) }} || ' ' || {{ adapter.quote(time_col) }},
                     '{{ date_format ~ ' ' ~ time_format }}')
                     as timestamp(6)
        ),
    '{{ src_timezone }}'
  ) at time zone 'UTC'
{%- endmacro -%}
