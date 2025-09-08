{%- macro identifier(name) -%}
    {{ return(adapter.dispatch('identifier')(name)) }}
{% endmacro %}

{%- macro default__identifier(name) -%}
  "{{ name }}"
{%- endmacro -%}

{%- macro spark__identifier(name) -%}
  `{{ name }}`
{%- endmacro -%}
