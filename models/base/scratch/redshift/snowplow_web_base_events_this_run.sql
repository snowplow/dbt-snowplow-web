{{ 
  config(
    materialized='table',
    sort='collector_tstamp',
    dist='event_id'
  ) 
}}


with sessions_this_run as (
  select
    s.session_id,
    s.start_tstamp,
    s.end_tstamp

  from {{ ref('snowplow_web_base_sessions_lifecycle')}} s

  where 
     (s.end_tstamp between 
          (select lower_limit from {{ ref('snowplow_web_current_incremental_tstamp') }})
      and (select upper_limit from {{ ref('snowplow_web_current_incremental_tstamp') }}) )
  or (s.start_tstamp between 
          (select lower_limit from {{ ref('snowplow_web_current_incremental_tstamp') }})
      and (select upper_limit from {{ ref('snowplow_web_current_incremental_tstamp') }}) )
)

, session_limits as (
  select
    min(start_tstamp) as lower_limit,
    max(end_tstamp) as upper_limit

  from sessions_this_run
)

, events_this_run AS (
  select
    a.*,
    dense_rank() over (partition by a.event_id order by a.collector_tstamp) as event_id_dedupe_index --dense_rank to catch event_ids with dupe tstamps later

  from {{ source(var('snowplow_atomic_schema'), var('atomic_events_table')) }} as a
  inner join sessions_this_run as b
  on a.domain_sessionid = b.session_id

  where datediff(day, b.start_tstamp, a.collector_tstamp) <= {{ var("days_late_allowed", 3) }}
  and datediff(day, a.dvce_created_tstamp, a.dvce_sent_tstamp) <= {{ var("days_late_allowed", 3) }}
  and a.collector_tstamp >= (select lower_limit from session_limits)
  and a.collector_tstamp <= (select upper_limit from session_limits)
  and {{ snowplow_dbt_utils.app_id_filter(var("app_id")) }}
)

, events_deduped as (
  select
    *,
    count(*) over(partition by e.event_id) as row_count

  from events_this_run e
  
  where 
    e.event_id_dedupe_index = 1
)

, page_context as (
select
  root_id,
  root_tstamp,
  id as page_view_id

from {{ source(var('snowplow_atomic_schema'), var('page_view_context_table')) }} 
where 
  root_tstamp >= (select lower_limit from session_limits)
  and root_tstamp <= (select upper_limit from session_limits)
)

select
  ed.*,
  pc.page_view_id

from 
  events_deduped as ed
left join 
  page_context as pc
on ed.event_id = pc.root_id
and ed.collector_tstamp = pc.root_tstamp

where row_count = 1
