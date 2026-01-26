{#--
  Simple schema introspection helpers for Athena via dbt adapter.

  These macros require an active dbt connection (they query the catalog).
--#}

{% macro athena_get_column_names(relation) %}
  {%- set cols = adapter.get_columns_in_relation(relation) -%}
  {%- set names = [] -%}
  {%- for c in cols -%}
    {%- do names.append(c.name) -%}
  {%- endfor -%}
  {{ return(names) }}
{% endmacro %}


{% macro athena_star(relation, except=[]) %}
  {%- set except_lower = [] -%}
  {%- for e in except -%}
    {%- do except_lower.append(e | lower) -%}
  {%- endfor -%}

  {%- set names = athena_get_column_names(relation) -%}
  {%- set projections = [] -%}
  {%- for n in names -%}
    {%- if (n | lower) not in except_lower -%}
      {# Quote with double-quotes (Athena/Trino-compatible). Escape internal quotes. #}
      {%- set escaped = (n | replace('"', '""')) -%}
      {%- do projections.append('"' ~ escaped ~ '"') -%}
    {%- endif -%}
  {%- endfor -%}

  {{ return(projections | join(', ')) }}
{% endmacro %}


{% macro print_source_schema(source_name, table_name) %}
  {%- set rel = source(source_name, table_name) -%}
  {%- set cols = adapter.get_columns_in_relation(rel) -%}

  {% do log('Relation: ' ~ rel, info=true) %}
  {% for c in cols %}
    {% do log(' - ' ~ c.name ~ ' (' ~ c.data_type ~ ')', info=true) %}
  {% endfor %}
{% endmacro %}

