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
  where asset_type = 'REW'
),

shifts as (
  select distinct
    shift_uuid,
    customer_uuid as shift_customer_uuid,
    line_uuid as shift_line_uuid,
    start_at as shift_start_at,
    end_at as shift_end_at_original,
    case 
      when end_at is null then {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
      when end_at > {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %} then {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
      else end_at
    end as shift_end_at_effective
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

aggregated_data_clipped as (
  select
    amp.line_uuid,
    amp.customer_uuid,
    amp.oem_key,
    amp.average,
    s.shift_uuid,
    s.shift_start_at,
    s.shift_end_at_effective
  from {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }} amp
  inner join shifts s 
    on amp.line_uuid = s.shift_line_uuid 
    and amp.customer_uuid = s.shift_customer_uuid
    and (
      (amp.window_start >= s.shift_start_at and amp.window_start < s.shift_end_at_effective)
      or (amp.window_end > s.shift_start_at and amp.window_end <= s.shift_end_at_effective)
      or (amp.window_start <= s.shift_start_at and amp.window_end >= s.shift_end_at_effective)
    )
  where 1=1
    {% if customer_id_filter %}
      and amp.customer_uuid = '{{ customer_id_filter }}'
    {% endif %}
    {% if line_id_filter %}
      and amp.line_uuid = '{{ line_id_filter }}'
    {% endif %}
    and amp.oem_key in (
      'speed',
      'CONV.LINE.Statistics.Production.Log.ProducedNotReset',
      'CONV.LINE.Statistics.Production.Log.RejectedNotReset',
      'CONV.REW.Status.Product.Log.SheetsNumber',
      'CONV.REW.Status.Product.Log.PerforationLenght'
    )
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
),

base as (
  select
    shift_uuid,
    shift_start_at as shift_start,
    shift_end_at_effective as shift_end,
    line_uuid,
    avg(case when oem_key = 'speed' then average end) as target_speed_mpm,
    avg(case when oem_key = 'CONV.LINE.Statistics.Production.Log.ProducedNotReset' then average end) as produced_not_reset,
    max(case when oem_key = 'CONV.LINE.Statistics.Production.Log.RejectedNotReset' then average end) as rejected_not_reset,
    max(case when oem_key = 'CONV.REW.Status.Product.Log.SheetsNumber' then average end) as sheet_number,
    coalesce(max(case when oem_key = 'CONV.REW.Status.Product.Log.PerforationLenght' then average end), 0) as perf_len_raw
  from aggregated_data_clipped
  group by shift_uuid, shift_start_at, shift_end_at_effective, line_uuid
),

deltas as (
  select
    *,
    -- Handle counter rollover (uint32 max = 4294967295)
    case 
      when ((produced_not_reset - lag(produced_not_reset) over (partition by line_uuid order by shift_start asc)) < 0)
        and ((produced_not_reset - lag(produced_not_reset) over (partition by line_uuid order by shift_start asc)) < -5000)
      then ((4294967295 - lag(produced_not_reset) over (partition by line_uuid order by shift_start asc)) + produced_not_reset)
      else greatest((produced_not_reset - lag(produced_not_reset) over (partition by line_uuid order by shift_start asc)), 0)
    end as delta_produced,
    case 
      when ((coalesce(rejected_not_reset, 0) - lag(coalesce(rejected_not_reset, 0)) over (partition by line_uuid order by shift_start asc)) < 0)
        and ((coalesce(rejected_not_reset, 0) - lag(coalesce(rejected_not_reset, 0)) over (partition by line_uuid order by shift_start asc)) < -5000)
      then ((4294967295 - lag(coalesce(rejected_not_reset, 0)) over (partition by line_uuid order by shift_start asc)) + coalesce(rejected_not_reset, 0))
      else greatest((coalesce(rejected_not_reset, 0) - lag(coalesce(rejected_not_reset, 0)) over (partition by line_uuid order by shift_start asc)), 0)
    end as delta_rejected
  from base
),

calc as (
  select
    d.*,
    c.default_target_speed,
    coalesce(d.target_speed_mpm, c.default_target_speed) as target_speed_used,
    coalesce((d.perf_len_raw / 100.0), 2.1) as perf_len_m,
    (date_diff('second', shift_start, shift_end) / 60.0) as runtime_min
  from deltas d
  cross join cfg c
)

select
  line_uuid,
  shift_uuid,
  shift_start,
  shift_end,
  target_speed_mpm,
  default_target_speed,
  target_speed_used,
  delta_produced,
  delta_rejected,
  sheet_number,
  perf_len_m,
  ((delta_produced * coalesce(sheet_number, 450)) * perf_len_m) as paper_unwound,
  ((delta_rejected * coalesce(sheet_number, 450)) * perf_len_m) as paper_unwound_rejected,
  (((delta_produced * coalesce(sheet_number, 450)) * perf_len_m) / nullif((target_speed_used * runtime_min), 0)) as performance,
  ((((delta_produced * coalesce(sheet_number, 450)) * perf_len_m) - ((delta_rejected * coalesce(sheet_number, 450)) * perf_len_m)) / nullif(((delta_produced * coalesce(sheet_number, 450)) * perf_len_m), 0)) as quality,
  runtime_min
from calc
