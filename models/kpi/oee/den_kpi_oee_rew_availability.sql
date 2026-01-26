{% set previous_run = var('previous_run', none) %}
{% set current_run = var('current_run', none) %}
{% set filter_customer_id = var('customer_id', none) %}
{% set filter_line_id = var('line_id', none) %}
{% set merge_lookback_hours = var('incremental_merge_lookback_hours', 8) %}

{{
  config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['customer_id', 'line_id', 'shift_uuid', 'section_uuid', 'level', 'shift_start_at'],
    on_schema_change='append_new_columns',
    partitioned_by=['line_id', 'day(shift_start_at)'],
    s3_data_naming='table',
    persist_docs={'relation': true, 'columns': true},
    incremental_predicates=[
      "target.shift_start_at >= date_add('hour', -" ~ merge_lookback_hours ~ ", current_timestamp)"
    ]
  )
}}

with config_lines as (
    select
        id as line_id,
        customer_id
    from {{ source('kpi_oee', 'vw_smoothoperator_config_lines') }}
    where 1=1
      {% if filter_customer_id %}
        and customer_id = '{{ filter_customer_id }}'
      {% endif %}
      {% if filter_line_id %}
        and id = '{{ filter_line_id }}'
      {% endif %}
),

threshold as (
    select
        max(case when type = 'downtime_threshold_value' then default_value end) as downtime_threshold_value,
        asset_type
    from {{ source('kpi_oee', 'kpi_machine_config') }}
    group by asset_type
),

segment_shift_splits as (
    select distinct
        sg.id as segment_id,
        sg.loss,
        sg.line_id,
        sg.section_id,
        sg.level,
        sg.operation,
        sg.created_at,
        sg.updated_at,
        sg.start_time,
        sg.end_time,
        sg.average_speed,
        sh.id as shift_id,
        sh.shift_uuid,
        sh.start_at as shift_start_at,
        sh.end_at as shift_end_at,
        sh.source as shift_source,
        sh.version as shift_version,
        ((sg.end_time - sg.start_time) / 1E3) as original_segment_duration_seconds,
        case
            when from_unixtime(sg.start_time / 1000) >= sh.start_at
                 and from_unixtime(sg.start_time / 1000) < sh.end_at
            then from_unixtime(sg.start_time / 1000)
            else sh.start_at
        end as calculated_segment_start_time,
        case
            when from_unixtime(sg.end_time / 1000) >= sh.start_at
                 and from_unixtime(sg.end_time / 1000) < sh.end_at
            then from_unixtime(sg.end_time / 1000)
            else sh.end_at
        end as calculated_segment_end_time
    from {{ source('kpi_oee', 'vw_segments_public_segments') }} sg
    inner join {{ source('kpi_oee', 'vw_smoothoperator_operations_shift_history') }} sh
        on sg.line_id = sh.line_uuid
        and (
            (from_unixtime(sg.start_time / 1000) >= sh.start_at and from_unixtime(sg.start_time / 1000) < sh.end_at)
            or (from_unixtime(sg.end_time / 1000) >= sh.start_at and from_unixtime(sg.end_time / 1000) < sh.end_at)
            or (from_unixtime(sg.start_time / 1000) <= sh.start_at and from_unixtime(sg.end_time / 1000) >= sh.end_at)
        )
    where sg.line_id in (select line_id from config_lines)
      and sh.line_uuid in (select line_id from config_lines)
      {% if is_incremental() %}
        {% if previous_run and current_run %}
          and sg.updated_at between timestamp '{{ previous_run }}' and timestamp '{{ current_run }}'
        {% else %}
          and sg.updated_at >= date_add('hour', -8, current_timestamp)
          and sg.updated_at < current_timestamp
        {% endif %}
      {% endif %}
),

all_segments_including_splitted as (
    select
        segment_id,
        line_id,
        section_id,
        level,
        operation,
        created_at,
        updated_at,
        (loss * (to_unixtime(calculated_segment_end_time) - to_unixtime(calculated_segment_start_time))) 
            / original_segment_duration_seconds as loss,
        shift_id,
        shift_uuid,
        shift_start_at,
        shift_end_at,
        shift_source,
        shift_version,
        calculated_segment_start_time,
        calculated_segment_end_time,
        average_speed
    from segment_shift_splits
),

all_closed_documented_downtime_segments as (
    select
        s1.segment_id,
        s1.line_id as line_uuid,
        s1.section_id as section_uuid,
        s1.shift_uuid,
        s1.shift_start_at,
        s1.shift_end_at,
        s1.created_at as segment_created_at,
        c.created_at as card_created_at,
        concat(date_format(s1.calculated_segment_start_time, '%Y-%m-%d %H:%i'), ':00') as start_timestamp,
        date(s1.calculated_segment_start_time) as prod_date,
        case
            when s1.level = 0
            then cast((to_unixtime(s1.calculated_segment_end_time) - to_unixtime(s1.calculated_segment_start_time)) / 60 as smallint)
            else cast(round(s1.loss, 0) as smallint)
        end as down_mins,
        s1.calculated_segment_end_time as end_timestamp,
        case when s1.operation = '-D' then 'DEL' else 'OK' end as stat,
        case when s1.updated_at > c.updated_at then s1.updated_at else c.updated_at end as last_upd,
        c.root_cause as root_cause_uuid,
        c.details as remarks,
        s1.level,
        case when s1.level = 0 then 'DOWNTIME' else 'SPEED_LOSS' end as loss_type,
        s1.average_speed
    from all_segments_including_splitted s1
    left join all_segments_including_splitted s2
        on s1.line_id = s2.line_id
        and s1.section_id = s2.section_id
        and s1.calculated_segment_end_time = s2.calculated_segment_start_time
    inner join {{ source('kpi_oee', 'vw_segments_public_card_segment') }} csg
        on s1.segment_id = csg.segment_id
    inner join {{ source('kpi_oee', 'vw_segments_public_cards') }} c
        on csg.card_id = c.id
    where c.line_id in (select line_id from config_lines)
      and s1.level in (0, 1)
),

rew_stop_flags as (
    select
        seg.line_uuid,
        seg.shift_uuid,
        seg.shift_start_at,
        seg.shift_end_at,
        case when s.oem_key = 'CONV.LINE.Status.State.Process.AutomaticStopByFault' then s.bool_value end as automatic_stop_by_fault,
        case when s.oem_key = 'CONV.LINE.Status.State.Process.AutomaticStopByEntry' then s.bool_value end as automatic_stop_by_entry,
        case when s.oem_key = 'CONV.LINE.Status.State.Process.AutomaticStopByExit' then s.bool_value end as automatic_stop_by_exit,
        case when s.oem_key = 'CONV.LINE.Status.State.Process.AutomaticStopByMaterial' then s.bool_value end as automatic_stop_by_material
    from (
        select
            amp.line_uuid,
            amp.window_start,
            amp.window_end,
            amp.oem_key,
            case when lower(sd.value) = 'true' then 1 else 0 end as bool_value
        from {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }} amp
        cross join unnest(samples_distinct) as t(sd)
        where amp.oem_key in (
            'CONV.LINE.Status.State.Process.AutomaticStopByFault',
            'CONV.LINE.Status.State.Process.AutomaticStopByEntry',
            'CONV.LINE.Status.State.Process.AutomaticStopByExit',
            'CONV.LINE.Status.State.Process.AutomaticStopByMaterial'
        )
        {% if is_incremental() %}
          {% if previous_run and current_run %}
            and amp.window_start between timestamp '{{ previous_run }}' and timestamp '{{ current_run }}'
          {% else %}
            and amp.window_start >= date_add('hour', -8, current_timestamp)
            and amp.window_start < current_timestamp
          {% endif %}
        {% endif %}
        and amp.fp_process_timestamp = (
            select max(fp_process_timestamp)
            from {{ source('kpi_oee', 'production_ingestion_aggregated_machine_parameter') }}
            where line_uuid = amp.line_uuid
              and oem_key in (
                  'CONV.LINE.Status.State.Process.AutomaticStopByFault',
                  'CONV.LINE.Status.State.Process.AutomaticStopByEntry',
                  'CONV.LINE.Status.State.Process.AutomaticStopByExit',
                  'CONV.LINE.Status.State.Process.AutomaticStopByMaterial'
              )
              {% if is_incremental() %}
                {% if previous_run and current_run %}
                  and window_start between timestamp '{{ previous_run }}' and timestamp '{{ current_run }}'
                {% else %}
                  and window_start >= date_add('hour', -8, current_timestamp)
                  and window_start < current_timestamp
                {% endif %}
              {% endif %}
        )
    ) s
    inner join all_closed_documented_downtime_segments seg
        on s.line_uuid = seg.line_uuid
        and s.window_start >= seg.shift_start_at
        and s.window_start < seg.shift_end_at
),

no_data_values as (
    select
        no_data.shift_uuid,
        no_data.customer_uuid,
        no_data.line_uuid,
        no_data.shift_start_at,
        no_data.shift_end_at,
        no_data.fp_key,
        no_data.total_minutes,
        no_data.minutes_with_data,
        no_data.missing_minutes
    from {{ source('kpi_oee', 'vw_kpi_shift_no_data_verification') }} no_data
    where fp_key = 'speed'
      and no_data.line_uuid in (select line_id from config_lines)
),

final as (
    select
        cfg.customer_id,
        cfg.line_id,
        adcs.section_uuid,
        adcs.shift_uuid,
        adcs.level,
        adcs.loss_type,
        adcs.shift_start_at,
        adcs.shift_end_at,
        cast((to_unixtime(adcs.shift_end_at) - to_unixtime(adcs.shift_start_at)) / 60 as integer) as shift_duration_mins,
        sum(adcs.down_mins) as down_mins,
        sum(case
            when adcs.average_speed < threshold.downtime_threshold_value
                 and coalesce(rew_flags.automatic_stop_by_fault, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_entry, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_exit, 0) = 1
                 and coalesce(rew_flags.automatic_stop_by_material, 0) = 0
            then adcs.down_mins else 0
        end) as downstream_mins,
        sum(case
            when adcs.average_speed < threshold.downtime_threshold_value
                 and coalesce(rew_flags.automatic_stop_by_fault, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_entry, 0) = 1
                 and coalesce(rew_flags.automatic_stop_by_exit, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_material, 0) = 0
            then adcs.down_mins else 0
        end) as upstream_mins,
        sum(case
            when adcs.average_speed < threshold.downtime_threshold_value
                 and coalesce(rew_flags.automatic_stop_by_fault, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_entry, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_exit, 0) = 0
                 and coalesce(rew_flags.automatic_stop_by_material, 0) = 1
            then adcs.down_mins else 0
        end) as material_mins,
        no_data.fp_key,
        no_data.total_minutes,
        no_data.minutes_with_data,
        coalesce(no_data.missing_minutes, 0) as missing_minutes,
        threshold.downtime_threshold_value,
        adcs.stat,
        case
            when cast((to_unixtime(adcs.shift_end_at) - to_unixtime(adcs.shift_start_at)) / 60 as double) = 0 then 0E0
            else round(
                (
                    (
                        cast((to_unixtime(adcs.shift_end_at) - to_unixtime(adcs.shift_start_at)) / 60 as double)
                        - coalesce(sum(adcs.down_mins), 0)
                        - coalesce(no_data.missing_minutes, 0)
                    )
                    / nullif(
                        (
                            cast((to_unixtime(adcs.shift_end_at) - to_unixtime(adcs.shift_start_at)) / 60 as double)
                            - coalesce(sum(case
                                when adcs.average_speed < threshold.downtime_threshold_value
                                     and coalesce(rew_flags.automatic_stop_by_fault, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_entry, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_exit, 0) = 1
                                     and coalesce(rew_flags.automatic_stop_by_material, 0) = 0
                                then adcs.down_mins else 0
                            end), 0)
                            - coalesce(sum(case
                                when adcs.average_speed < threshold.downtime_threshold_value
                                     and coalesce(rew_flags.automatic_stop_by_fault, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_entry, 0) = 1
                                     and coalesce(rew_flags.automatic_stop_by_exit, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_material, 0) = 0
                                then adcs.down_mins else 0
                            end), 0)
                            - coalesce(sum(case
                                when adcs.average_speed < threshold.downtime_threshold_value
                                     and coalesce(rew_flags.automatic_stop_by_fault, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_entry, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_exit, 0) = 0
                                     and coalesce(rew_flags.automatic_stop_by_material, 0) = 1
                                then adcs.down_mins else 0
                            end), 0)
                        ),
                        0E0
                    )
                ) * 1E2,
                1
            )
        end as availability,
        current_timestamp as dbt_updated_at
    from all_closed_documented_downtime_segments adcs
    inner join config_lines cfg
        on adcs.line_uuid = cfg.line_id
    inner join threshold
        on threshold.asset_type = 'REW'
    left join rew_stop_flags rew_flags
        on rew_flags.line_uuid = adcs.line_uuid
        and rew_flags.shift_uuid = adcs.shift_uuid
    left join no_data_values no_data
        on adcs.shift_uuid = no_data.shift_uuid
        and no_data.shift_start_at = adcs.shift_start_at
        and no_data.customer_uuid = cfg.customer_id
    group by
        cfg.customer_id,
        cfg.line_id,
        adcs.section_uuid,
        adcs.shift_uuid,
        adcs.level,
        adcs.loss_type,
        adcs.shift_start_at,
        adcs.shift_end_at,
        no_data.fp_key,
        no_data.total_minutes,
        no_data.minutes_with_data,
        no_data.missing_minutes,
        adcs.stat,
        threshold.downtime_threshold_value
)

select * from final
