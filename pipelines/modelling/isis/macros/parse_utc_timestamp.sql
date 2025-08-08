-- Parse separate date and time strings into a single UTC timestamp
{%- macro parse_utc_timestamp(date, date_format, time, time_format='HH:mm:ss') -%}
  {{ return(adapter.dispatch('parse_utc_timestamp')(date, date_format, time, time_format)) }}
{% endmacro %}


{%- macro trino__parse_utc_timestamp(date, date_format, time, time_format) -%}
  cast(parse_datetime({{ adapter.quote('date') }} || ' ' || {{ adapter.quote('time') }}, '{{ date_format ~ ' ' ~ time_format }}') as timestamp(6)) at time zone 'UTC'
{%- endmacro -%}
