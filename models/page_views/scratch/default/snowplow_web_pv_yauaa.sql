{{ 
  config(
    sort='page_view_id',
    dist='page_view_id',
    enabled=(var('snowplow__enable_yauaa', false) and target.type in ['redshift', 'postgres'] | as_bool())
  ) 
}}


select
  pv.page_view_id,

  ya.device_class,
  ya.agent_class,
  ya.agent_name,
  ya.agent_name_version,
  ya.agent_name_version_major,
  ya.agent_version,
  ya.agent_version_major,
  ya.device_brand,
  ya.device_name,
  ya.device_version,
  ya.layout_engine_class,
  ya.layout_engine_name,
  ya.layout_engine_name_version,
  ya.layout_engine_name_version_major,
  ya.layout_engine_version,
  ya.layout_engine_version_major,
  ya.operating_system_class,
  ya.operating_system_name,
  ya.operating_system_name_version,
  ya.operating_system_version

from {{ var('snowplow__yauaa_context') }} ya

inner join {{ ref('snowplow_web_page_view_events') }} pv
on ya.root_id = pv.event_id
and ya.root_tstamp = pv.collector_tstamp

where ya.root_tstamp >= (select lower_limit from {{ ref('snowplow_web_pv_limits') }})
  and ya.root_tstamp <= (select upper_limit from {{ ref('snowplow_web_pv_limits') }})
