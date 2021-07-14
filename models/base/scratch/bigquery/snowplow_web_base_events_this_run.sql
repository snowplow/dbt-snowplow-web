{{ 
  config(
    partition_by = {
      "field": "collector_tstamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["event_name","page_view_id"],
    tags=["this_run"]
  ) 
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_base_session_limits(ref('snowplow_web_base_sessions_this_run')) %}

-- without downstream joins, it's safe to dedupe by picking the first event_id found.
select
  array_agg(e order by e.collector_tstamp limit 1)[offset(0)].*

from (

  select
    a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[safe_offset(0)].id as page_view_id,
    a.* except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0)

  from {{ var('snowplow__events') }} as a
  inner join {{ ref('snowplow_web_base_sessions_this_run') }} as b
  on a.domain_sessionid = b.session_id

  where timestamp_diff(a.collector_tstamp, b.start_tstamp, day) <= {{ var("snowplow__max_session_days", 3) }}
  and timestamp_diff(a.dvce_sent_tstamp,  a.dvce_created_tstamp, day) <= {{ var("snowplow__days_late_allowed", 3) }}
  and a.collector_tstamp >= {{ lower_limit }}
  and a.collector_tstamp <= {{ upper_limit }}
  {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %}
    and a.derived_tstamp >= {{ lower_limit }}
    and a.derived_tstamp <= {{ upper_limit }}
  {% endif %}
  and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}

) e
group by e.event_id

