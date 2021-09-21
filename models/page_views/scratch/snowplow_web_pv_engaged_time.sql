{{ 
  config(
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["page_view_id"]),
    sort='page_view_id',
    dist='page_view_id'
  ) 
}}

select
  ev.page_view_id,
  max(ev.derived_tstamp) as end_tstamp,

  -- aggregate pings:
    -- divides epoch tstamps by snowplow__heartbeat to get distinct intervals
    -- floor rounds to nearest integer - duplicates all evaluate to the same number
    -- count(distinct) counts duplicates only once
    -- adding snowplow__min_visit_length accounts for the page view event itself.

  {{ var("snowplow__heartbeat", 10) }} * (count(distinct(floor({{ snowplow_utils.to_unixtstamp('ev.derived_tstamp') }}/{{ var("snowplow__heartbeat", 10) }}))) - 1) + {{ var("snowplow__min_visit_length", 5) }} as engaged_time_in_s

from {{ ref('snowplow_web_base_events_this_run') }} as ev

where ev.event_name = 'page_ping'
and ev.page_view_id is not null

group by 1
