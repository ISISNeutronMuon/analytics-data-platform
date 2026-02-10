-- Creates a key for the equipment -> category mapping table
{%- macro create_equipment_category_key(text_col) -%}
  lower( {{ normalize_whitespace(text_col) }} )
{%- endmacro -%}
