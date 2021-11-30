{{ 
  config(
    sort='page_view_id',
    dist='page_view_id',
    enabled=(var('snowplow__enable_ua', false) and target.type in ['redshift', 'postgres'] | as_bool())
  ) 
}}

select
  pv.page_view_id,

  ua.useragent_family,
  ua.useragent_major,
  ua.useragent_minor,
  ua.useragent_patch,
  ua.useragent_version,
  ua.os_family,
  ua.os_major,
  ua.os_minor,
  ua.os_patch,
  ua.os_patch_minor,
  ua.os_version,
  ua.device_family

from {{ var('snowplow__ua_parser_context') }} as ua

inner join {{ ref('snowplow_web_page_view_events') }} pv
on ua.root_id = pv.event_id
and ua.root_tstamp = pv.collector_tstamp

where ua.root_tstamp >= (select lower_limit from {{ ref('snowplow_web_pv_limits') }})
  and ua.root_tstamp <= (select upper_limit from {{ ref('snowplow_web_pv_limits') }})
