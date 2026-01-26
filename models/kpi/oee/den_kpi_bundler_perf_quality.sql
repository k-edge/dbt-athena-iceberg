{% set previous_run = var('previous_run', none) %}
{% set current_run = var('current_run', none) %}
{% set customer_id_filter = var('customer_id', none) %}
{% set line_id_filter = var('line_id', none) %}

{{
  config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['line_uuid', 'shift_uuid', 'shift_start'],
    on_schema_change='append_new_columns',
    partitioned_by=['line_uuid', 'day(shift_start)'],
    s3_data_naming='table',
    persist_docs={'relation': true, 'columns': true},
    incremental_predicates=[
      "target.shift_start >= date_add('hour', -" ~ var('incremental_merge_lookback_hours', 8) ~ ", current_timestamp)"
    ]
  )
}}

{% if not previous_run or not current_run %}
  {% set current_run = "current_timestamp" %}
  {% set previous_run = "date_add('hour', -8, current_timestamp)" %}
  {% set use_dynamic_ts = true %}
{% else %}
  {% set use_dynamic_ts = false %}
{% endif %}

with cfg as (
  select max(case when type = 'target_speed_default_value' then default_value end) as default_target_speed
  from {{ source('kpi_oee', 'kpi_machine_config') }}
  where asset_type = 'BU'
),

shifts as (
  select distinct
    shift_uuid,
    customer_uuid,
    line_uuid,
    start_at as shift_start,
    end_at as shift_end_original,
    case 
      when end_at is null then {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
      when end_at > {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %} then {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
      else end_at
    end as shift_end
  from {{ source('kpi_oee', 'vw_smoothoperator_operations_shift_history') }}
  where 1=1
    {% if customer_id_filter %}
      and customer_uuid = '{{ customer_id_filter }}'
    {% endif %}
    {% if line_id_filter %}
      and line_uuid = '{{ line_id_filter }}'
    {% endif %}
    and start_at < {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and (end_at is null or end_at > {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %})
),

shift_start_produced as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.shift_start,
    coalesce(amp.minimum, 0) as produced_not_reset_bu_start
  from shifts s
  inner join {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }} amp 
    on s.line_uuid = amp.line_uuid 
    and s.shift_start = amp.window_start 
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and amp.oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset' 
    and amp.fp_process_timestamp = (
      select min(fp_process_timestamp)
      from {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }}
      where line_uuid = amp.line_uuid 
        and window_start = amp.window_start 
        and oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset'
        and window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    )
),

shift_end_produced as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.shift_start,
    coalesce(amp.maximum, 0) as produced_not_reset_bu_end
  from shifts s
  inner join {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }} amp 
    on s.line_uuid = amp.line_uuid 
    and s.shift_end = amp.window_start 
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and amp.oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset' 
    and amp.fp_process_timestamp = (
      select max(fp_process_timestamp)
      from {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }}
      where line_uuid = amp.line_uuid 
        and window_start = amp.window_start 
        and oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset' 
        and window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    )
),

base as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.customer_uuid,
    s.shift_start,
    s.shift_end,
    avg(case when p.oem_key = 'PACK.BU1.Status.State.PresetSpeed' then p.average end) as target_speed_bu
  from shifts s
  inner join {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }} p 
    on s.line_uuid = p.line_uuid 
    and p.window_start >= s.shift_start 
    and p.window_start < s.shift_end 
    and p.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and p.oem_key in ('PACK.BU1.Status.State.PresetSpeed')
  group by s.shift_uuid, s.line_uuid, s.customer_uuid, s.shift_start, s.shift_end
),

deltas as (
  select
    b.shift_uuid,
    b.line_uuid,
    b.customer_uuid,
    b.shift_start,
    b.shift_end,
    b.target_speed_bu,
    sep.produced_not_reset_bu_end,
    ssp.produced_not_reset_bu_start,
    greatest((sep.produced_not_reset_bu_end - ssp.produced_not_reset_bu_start), 0) as delta_produced_bu
  from base b
  inner join shift_start_produced ssp 
    on b.shift_uuid = ssp.shift_uuid 
    and b.line_uuid = ssp.line_uuid 
    and b.shift_start = ssp.shift_start
  inner join shift_end_produced sep 
    on b.shift_uuid = sep.shift_uuid 
    and b.line_uuid = sep.line_uuid 
    and b.shift_start = sep.shift_start
),

calc as (
  select
    d.shift_uuid,
    d.line_uuid,
    d.customer_uuid,
    d.shift_start,
    d.shift_end,
    d.target_speed_bu,
    cfg.default_target_speed,
    coalesce(d.target_speed_bu, cfg.default_target_speed) as target_speed_used,
    d.delta_produced_bu,
    (date_diff('second', d.shift_start, d.shift_end) / 60.0) as runtime_min
  from deltas d
  cross join cfg
)

select
  c.line_uuid,
  c.shift_uuid,
  c.customer_uuid,
  c.shift_start,
  c.shift_end,
  c.target_speed_bu,
  c.default_target_speed,
  c.target_speed_used,
  c.delta_produced_bu,
  c.runtime_min,
  (c.delta_produced_bu / nullif((c.target_speed_used * c.runtime_min), 0)) as performance,
  1.0 as quality
from calc c
