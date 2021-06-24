{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='session_id',
    upsert_date_key='start_tstamp',
    full_refresh=false,
    schema=var("snowplow__manifest_custom_schema"),
    sort='start_tstamp',
    dist='session_id'
  ) 
}}

with sessions_this_run as (
  select
    e.domain_sessionid as session_id,
    min(e.collector_tstamp) as start_tstamp,
    max(e.collector_tstamp) as end_tstamp

  from {{ var('snowplow__events') }} e

  where
    e.domain_sessionid is not null
    and {{ dbt_utils.datediff('dvce_created_tstamp', 'dvce_sent_tstamp', 'day') }} <= {{ var("snowplow__days_late_allowed", 3) }} -- don't process data that's too late
    and e.collector_tstamp >= (select lower_limit from {{ ref('snowplow_web_base_new_event_limits') }})
    and e.collector_tstamp <= (select upper_limit from {{ ref('snowplow_web_base_new_event_limits') }})
    and {{ snowplow_utils.app_id_filter() }}
    and {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --don't reprocess sessions that have already been processed.

  group by 1
  )

{% if snowplow_utils.snowplow_is_incremental() %} 

, previous_sessions as (
  select *

  from {{ this }}

  where start_tstamp >= (select {{ dbt_utils.dateadd('day', -var("snowplow__session_lookback_days", 365), 'lower_limit') }} AS session_limit from {{ ref('snowplow_web_base_new_event_limits') }})
  and {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --don't reprocess sessions that have already been processed.
)

  select
    str.session_id,
    least(str.start_tstamp, self.start_tstamp) as start_tstamp,
    greatest(str.end_tstamp, self.end_tstamp) as end_tstamp
    
  from sessions_this_run str
  left join previous_sessions as self
    on str.session_id = self.session_id

  where
    self.session_id is null -- process all new sessions
    or {{ dbt_utils.datediff('self.start_tstamp', 'self.end_tstamp', 'day') }} <= {{ var("snowplow__max_session_days", 3) }} --stop updating sessions exceeding 3 days

{% else %}

  select * from sessions_this_run

{% endif %}
