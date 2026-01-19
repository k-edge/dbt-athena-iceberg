{{ config(materialized='view') }}

-- Passthrough staging view:
-- - keeps staging lightweight (no duplicate data in S3)
-- - enables tests/docs at a stable `stg_*` layer
with card_segment as (
    select *
    from {{ source('sourcing', 'segments__public__card_segment') }}
)

select * from card_segment

