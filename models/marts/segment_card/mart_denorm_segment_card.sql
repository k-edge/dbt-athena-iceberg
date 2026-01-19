{{
  config(
    materialized='view'
  )
}}

select *
from {{ ref('int_segment_card') }}