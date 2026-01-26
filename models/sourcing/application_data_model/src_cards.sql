{{ config(materialized='view') }}

{% set operation_timestamp_start = var('operation_timestamp_start', none) %}
{% set operation_timestamp_end = var('operation_timestamp_end', none) %}

with cards as (
    select *
    from {{ source('sourcing', 'segments__public__cards') }}
    where 1=1
      {# Default to last 7 days if no explicit range is provided #}
      {% if operation_timestamp_start %}
        and operation_timestamp >= timestamp '{{ operation_timestamp_start }}'
      {% else %}
        and operation_timestamp >= date_add('day', -7, current_timestamp)
      {% endif %}

      {% if operation_timestamp_end %}
        and operation_timestamp < timestamp '{{ operation_timestamp_end }}'
      {% else %}
        and operation_timestamp < current_timestamp
      {% endif %}
)

select * from cards

