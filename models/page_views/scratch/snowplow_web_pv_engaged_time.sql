{{ 
  config(
    materialized='table',
    sort='page_view_id',
    dist='page_view_id'
  ) 
}}

select
  ev.page_view_id,
  max(ev.derived_tstamp) as end_tstamp,

  -- aggregate pings:
    -- divides epoch tstamps by heartbeat to get distinct intervals
    -- floor rounds to nearest integer - duplicates all evaluate to the same number
    -- count(distinct) counts duplicates only once
    -- adding minimumvisitlength accounts for the page view event itself.

  {{ var("heartbeat", 10) }} * (count(distinct(floor({{ dbt_date.to_unixtimestamp('ev.derived_tstamp') }}/{{ var("heartbeat", 10) }}))) - 1) + {{ var("minimumVisitLength", 5) }} as engaged_time_in_s

from {{ ref('snowplow_web_base_events_this_run') }} as ev

where ev.event_name = 'page_ping'

group by 1
