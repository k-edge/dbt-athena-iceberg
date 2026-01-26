{% set merge_lookback_days = var('incremental_merge_lookback_days', 7) %}

{{
  config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['map_segment_id', 'map_card_id'],
    on_schema_change='append_new_columns',
    partitioned_by=['segment_line_id', 'day(last_updated_at)'],
    s3_data_naming='table',
    persist_docs={'relation': true, 'columns': true},
    incremental_predicates=[
      "target.last_updated_at >= date_add('day', -" ~ merge_lookback_days ~ ", current_timestamp)"
    ]
  )
}}

with segments as (
    select *
    from {{ ref('src_segments') }}
),
cards as (
    select *
    from {{ ref('src_cards') }}
),
card_segment as (
    select *
    from {{ ref('src_card_segment') }}
),
joined as (
    select
        -- segments__public__segments
        s.database_name as segment_database_name,
        s.schema_name as segment_schema_name,
        s.table_name as segment_table_name,
        s.operation_timestamp as segment_operation_timestamp,
        s.operation as segment_operation,
        s.id as segment_id,
        s.line_id as segment_line_id,
        s.level as segment_level,
        s.start_time as segment_start_time,
        s.end_time as segment_end_time,
        s.data as segment_data,
        s.created_at as segment_created_at,
        s.updated_at as segment_updated_at,
        s.average_speed as segment_average_speed,
        s.loss as segment_loss,
        s.anomaly as segment_anomaly,
        s.marked as segment_marked,
        s.marked_timestamp as segment_marked_timestamp,
        s.recalculating as segment_recalculating,
        s.machine_id as segment_machine_id,
        s.artificial as segment_artificial,
        s.auto_documented as segment_auto_documented,
        s.section_id as segment_section_id,
        s.is_manual_split as segment_is_manual_split,

        -- segments__public__card_segment
        cs.database_name as map_database_name,
        cs.schema_name as map_schema_name,
        cs.table_name as map_table_name,
        cs.operation_timestamp as map_operation_timestamp,
        cs.operation as map_operation,
        cs.segment_id as map_segment_id,
        cs.card_id as map_card_id,

        -- segments__public__cards
        c.database_name as card_database_name,
        c.schema_name as card_schema_name,
        c.table_name as card_table_name,
        c.operation_timestamp as card_operation_timestamp,
        c.operation as card_operation,
        c.id as card_id_from_cards,
        c.details as card_details,
        c.machine_type as card_machine_type,
        c.created_at as card_created_at,
        c.updated_at as card_updated_at,
        c.complete as card_complete,
        c.root_cause as card_root_cause,
        c.machine as card_machine,
        c.data as card_data,
        c.machine_group as card_machine_group,
        c.start_time as card_start_time,
        c.end_time as card_end_time,
        c.marked_timestamp as card_marked_timestamp,
        c.reported_time as card_reported_time,
        c.status as card_status,
        c.maintenance as card_maintenance,
        c.planned as card_planned,
        c.line_id as card_line_id,
        c.section_id as card_section_id,

        greatest(
            coalesce(s.updated_at, timestamp '1970-01-01 00:00:00'),
            coalesce(c.updated_at, timestamp '1970-01-01 00:00:00')
        ) as last_updated_at

    from segments s
    join card_segment cs
        on cs.segment_id = s.id
    left join cards c
        on c.id = cs.card_id
        and c.line_id = s.line_id
)

select *
from joined

{% if is_incremental() %}
where last_updated_at >= (
  select coalesce(max(last_updated_at), timestamp '1970-01-01 00:00:00')
  from {{ this }}
)
{% endif %}

{% if var('row_limit', 500) is not none %}
order by last_updated_at desc, map_segment_id, map_card_id
limit {{ var('row_limit', 500) }}
{% endif %}

