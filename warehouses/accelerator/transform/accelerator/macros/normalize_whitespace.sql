-- Removes leading and trailing whitespace and replaces any multiple-spaces with a single space
{%- macro normalize_whitespace(text_col) -%}
  {{ return(adapter.dispatch('normalize_whitespace')(text_col)) }}
{% endmacro %}

{%- macro default__normalize_whitespace(text_col) -%}
  regexp_replace(trim({{ adapter.quote(text_col) }}), '\s+', ' ')
{%- endmacro -%}
