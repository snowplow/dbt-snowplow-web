{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='session_id',
    upsert_date_key='start_tstamp',
    full_refresh=false,
    schema=var("snowplow__manifest_custom_schema"),
    sort='start_tstamp',
    dist='session_id',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["session_id"]
  ) 
}}

{% set lower_limit, upper_limit, session_lookback_limit = snowplow_utils.return_base_new_event_limits(ref('snowplow_web_base_new_event_limits')) %}
{% set is_run_with_new_events = snowplow_utils.is_run_with_new_events('snowplow_web') %}

with new_events_session_ids as (
  select
    e.domain_sessionid as session_id,
    min(e.collector_tstamp) as start_tstamp,
    max(e.collector_tstamp) as end_tstamp

  from {{ var('snowplow__events') }} e

  where
    e.domain_sessionid is not null
    and {{ dbt_utils.datediff('dvce_created_tstamp', 'dvce_sent_tstamp', 'day') }} <= {{ var("snowplow__days_late_allowed", 3) }} -- don't process data that's too late
    and e.collector_tstamp >= {{ lower_limit }}
    and e.collector_tstamp <= {{ upper_limit }}
    and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
    and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
    {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %} -- BQ only
      and e.derived_tstamp >= {{ lower_limit }}
      and e.derived_tstamp <= {{ upper_limit }}
    {% endif %}

  group by 1
  )

{% if snowplow_utils.snowplow_is_incremental() %} 

, previous_sessions as (
  select *

  from {{ this }}

  where start_tstamp >= {{ session_lookback_limit }}
  and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
)

  select
    ns.session_id,
    least(ns.start_tstamp, coalesce(self.start_tstamp, ns.start_tstamp)) as start_tstamp,
    greatest(ns.end_tstamp, coalesce(self.end_tstamp, ns.end_tstamp)) as end_tstamp -- BQ 1 NULL will return null hence coalesce
    
  from new_events_session_ids ns
  left join previous_sessions as self
    on ns.session_id = self.session_id

  where
    self.session_id is null -- process all new sessions
    or {{ dbt_utils.datediff('self.start_tstamp', 'self.end_tstamp', 'day') }} <= {{ var("snowplow__max_session_days", 3) }} --stop updating sessions exceeding 3 days

{% else %}

  select * from new_events_session_ids

{% endif %}
