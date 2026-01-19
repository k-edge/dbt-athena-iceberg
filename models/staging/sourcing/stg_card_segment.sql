{{ config(materialized='view') }}

with card_segment as (
    select *
    from {{ source('sourcing', 'segments__public__card_segment') }}
)

select * from card_segment

