{{ config(materialized='view') }}

{# Dynamically select all columns from the Athena table #}
{% set rel = source('sourcing', 'test_dbt_source') %}

select
  {{ athena_star(rel) }}
from {{ rel }}

