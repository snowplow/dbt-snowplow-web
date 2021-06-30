{{ 
  config(
    materialized='incremental',
    unique_key='session_id',
    sort='start_tstamp',
    dist='session_id',
    full_refresh=false
  ) 
}}
--TODO: Consider using 'snowplow_incremental' materialization
{% set has_new_events = snowplow_utils.is_run_with_new_events('snowplow_web') %}

with sessions_this_run as (
  select
    e.domain_sessionid as session_id,
    min(e.collector_tstamp) as start_tstamp,
    max(e.collector_tstamp) as end_tstamp

  from {{ var('snowplow__events') }} e

  where
    {{ dbt_utils.datediff('dvce_created_tstamp', 'dvce_sent_tstamp', 'day') }} <= {{ var("snowplow__days_late_allowed", 3) }} -- don't process data that's too late
    and e.collector_tstamp >= (select lower_limit from {{ ref('snowplow_web_current_incremental_tstamp') }})
    and e.collector_tstamp <= (select upper_limit from {{ ref('snowplow_web_current_incremental_tstamp') }})
    and {{ snowplow_utils.app_id_filter() }}
    and {{ has_new_events }} --don't reprocess sessions that have already been processed.

  group by 1
  )

{% if is_incremental() %} 

, previous_sessions as (
  select *

  from {{ this }}

  where start_tstamp >= (select {{ dbt_utils.dateadd('hour', -var("snowplow__session_lookback_days", 365), 'lower_limit') }} AS session_limit from {{ ref('snowplow_web_current_incremental_tstamp') }})
  and {{ has_new_events }} --don't reprocess sessions that have already been processed.
)

  select
    str.session_id,
    least(str.start_tstamp, self.start_tstamp) as start_tstamp,
    greatest(str.end_tstamp, self.end_tstamp) as end_tstamp --probably dont need greatest. being safe
    
  from sessions_this_run str
  left join previous_sessions as self
    on str.session_id = self.session_id

  where
    self.session_id is null -- process all new sessions
    or {{ dbt_utils.datediff('self.start_tstamp', 'self.end_tstamp', 'day') }} <= {{ var("snowplow__days_late_allowed", 3) }} --stop updating sessions exceeding 3 days

{% else %}

  select * from sessions_this_run

{% endif %}


