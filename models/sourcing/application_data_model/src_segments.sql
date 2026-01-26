{{ config(materialized='view') }}

with segments as (
    select *
    from {{ source('sourcing', 'segments__public__segments') }}
)

select * from segments

