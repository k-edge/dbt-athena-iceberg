{{ config(materialized='view') }}

-- Passthrough staging view:
-- - keeps staging lightweight (no duplicate data in S3)
-- - enables tests/docs at a stable `stg_*` layer
with segments as (
    select *
    from {{ source('sourcing', 'segments__public__segments') }}
)

select * from segments

