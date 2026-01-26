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
  where asset_type = 'WR'
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

-- Wrapper production at shift start
shift_start_produced_wr as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.shift_start,
    coalesce(amp.average, 0) as produced_not_reset_wr_start
  from shifts s
  inner join {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }} amp 
    on s.line_uuid = amp.line_uuid 
    and s.shift_start = amp.window_start 
    and amp.oem_key = 'PACK.WR1.Statistics.Production.Pack.ProducedNotReset' 
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and amp.fp_process_timestamp = (
      select min(fp_process_timestamp)
      from {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }}
      where line_uuid = amp.line_uuid 
        and window_start = amp.window_start 
        and oem_key = 'PACK.WR1.Statistics.Production.Pack.ProducedNotReset'
        and window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    )
),

-- Wrapper production at shift end
shift_end_produced_wr as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.shift_start,
    coalesce(amp.average, 0) as produced_not_reset_wr_end
  from shifts s
  inner join {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }} amp 
    on s.line_uuid = amp.line_uuid 
    and s.shift_end = amp.window_start 
    and amp.oem_key = 'PACK.WR1.Statistics.Production.Pack.ProducedNotReset' 
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and amp.fp_process_timestamp = (
      select max(fp_process_timestamp)
      from {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }}
      where line_uuid = amp.line_uuid 
        and window_start = amp.window_start 
        and oem_key = 'PACK.WR1.Statistics.Production.Pack.ProducedNotReset'
        and window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    )
),

-- Bundler production at shift start
shift_start_produced_bu as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.shift_start,
    coalesce(amp.average, 0) as produced_not_reset_bu_start
  from shifts s
  inner join {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }} amp 
    on s.line_uuid = amp.line_uuid 
    and s.shift_start = amp.window_start 
    and amp.oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset' 
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and amp.fp_process_timestamp = (
      select min(fp_process_timestamp)
      from {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }}
      where line_uuid = amp.line_uuid 
        and window_start = amp.window_start 
        and oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset'
        and window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    )
),

-- Bundler production at shift end
shift_end_produced_bu as (
  select
    s.shift_uuid,
    s.line_uuid,
    s.shift_start,
    coalesce(amp.average, 0) as produced_not_reset_bu_end
  from shifts s
  inner join {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }} amp 
    on s.line_uuid = amp.line_uuid 
    and s.shift_end = amp.window_start 
    and amp.oem_key = 'PACK.BU1.Statistics.Production.Bag.ProducedNotReset' 
    and amp.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
    and amp.fp_process_timestamp = (
      select max(fp_process_timestamp)
      from {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }}
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
    avg(case when p.oem_key = 'PACK.WR1.Status.State.PresetSpeed' then coalesce(p.average, 0) end) as target_speed_wr,
    avg(case when p.oem_key = 'PACK.BU1.Status.Product.Bag.RollIntoProduct' then coalesce(p.average, 0) end) as i16kv2m
  from shifts s
  left join {{ source('kpi_oee_production', 'production_ingestion_aggregated_machine_parameter') }} p 
    on s.line_uuid = p.line_uuid 
    and p.window_start >= s.shift_start 
    and p.window_start < s.shift_end 
    and p.oem_key in ('PACK.WR1.Status.State.PresetSpeed', 'PACK.BU1.Status.Product.Bag.RollIntoProduct')
    and p.window_start between {% if use_dynamic_ts %}{{ previous_run }}{% else %}timestamp '{{ previous_run }}'{% endif %} and {% if use_dynamic_ts %}{{ current_run }}{% else %}timestamp '{{ current_run }}'{% endif %}
  group by s.shift_uuid, s.line_uuid, s.customer_uuid, s.shift_start, s.shift_end
),

deltas as (
  select
    b.shift_uuid,
    b.line_uuid,
    b.customer_uuid,
    b.shift_start,
    b.shift_end,
    b.target_speed_wr,
    b.i16kv2m,
    sswr.produced_not_reset_wr_start,
    sewr.produced_not_reset_wr_end,
    ssbu.produced_not_reset_bu_start,
    sebu.produced_not_reset_bu_end,
    greatest((sewr.produced_not_reset_wr_end - sswr.produced_not_reset_wr_start), 0) as delta_produced_wr,
    greatest((sebu.produced_not_reset_bu_end - ssbu.produced_not_reset_bu_start), 0) as delta_produced_bundler,
    greatest(((sebu.produced_not_reset_bu_end * coalesce(b.i16kv2m, 0)) - (ssbu.produced_not_reset_bu_start * coalesce(b.i16kv2m, 0))), 0) as delta_produced_bundler_weighted
  from base b
  inner join shift_start_produced_wr sswr 
    on b.shift_uuid = sswr.shift_uuid 
    and b.line_uuid = sswr.line_uuid 
    and b.shift_start = sswr.shift_start
  inner join shift_end_produced_wr sewr 
    on b.shift_uuid = sewr.shift_uuid 
    and b.line_uuid = sewr.line_uuid 
    and b.shift_start = sewr.shift_start
  inner join shift_start_produced_bu ssbu 
    on b.shift_uuid = ssbu.shift_uuid 
    and b.line_uuid = ssbu.line_uuid 
    and b.shift_start = ssbu.shift_start
  inner join shift_end_produced_bu sebu 
    on b.shift_uuid = sebu.shift_uuid 
    and b.line_uuid = sebu.line_uuid 
    and b.shift_start = sebu.shift_start
),

calc as (
  select
    d.shift_uuid,
    d.line_uuid,
    d.customer_uuid,
    d.shift_start,
    d.shift_end,
    d.target_speed_wr,
    cfg.default_target_speed,
    coalesce(d.target_speed_wr, cfg.default_target_speed) as target_speed_used,
    d.delta_produced_wr,
    d.delta_produced_bundler,
    d.i16kv2m,
    (date_diff('second', d.shift_start, d.shift_end) / 60.0) as runtime_min,
    d.delta_produced_bundler_weighted as bundler_produced_weighted,
    d.delta_produced_wr as total_produced_shift,
    case 
      when (d.delta_produced_bundler is null) or (d.i16kv2m is null) then 1.0 
      else (d.delta_produced_bundler_weighted / nullif(d.delta_produced_wr, 0)) 
    end as quality,
    (d.delta_produced_wr / nullif((coalesce(d.target_speed_wr, cfg.default_target_speed) * (date_diff('second', d.shift_start, d.shift_end) / 60.0)), 0)) as performance
  from deltas d
  cross join cfg
)

select
  line_uuid,
  shift_uuid,
  customer_uuid,
  shift_start,
  shift_end,
  target_speed_wr,
  default_target_speed,
  target_speed_used,
  delta_produced_wr,
  delta_produced_bundler,
  i16kv2m,
  runtime_min,
  bundler_produced_weighted,
  total_produced_shift,
  quality,
  performance
from calc
