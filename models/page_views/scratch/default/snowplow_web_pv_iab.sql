{{ 
  config(
    sort='page_view_id',
    dist='page_view_id',
    enabled=(var('snowplow__enable_iab', false) and target.type in ['redshift', 'postgres'] | as_bool())
  ) 
}}

select
  pv.page_view_id,

  iab.category,
  iab.primary_impact,
  iab.reason,
  iab.spider_or_robot

from {{ var('snowplow__iab_context') }} iab

inner join {{ ref('snowplow_web_page_view_events') }} pv
on iab.root_id = pv.event_id
and iab.root_tstamp = pv.collector_tstamp

where iab.root_tstamp >= (select lower_limit from {{ ref('snowplow_web_pv_limits') }})
  and iab.root_tstamp <= (select upper_limit from {{ ref('snowplow_web_pv_limits') }})
