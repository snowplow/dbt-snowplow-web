{{
  config(
    tags=["this_run"],
    enabled=var("snowplow__enable_cwv", false) and target.type in ('redshift', 'postgres') | as_bool()
  )
}}

with {{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__cwv_context'), lower_limit, upper_limit, 'cwv') }}

, prep as (

  select
    e.event_id,
    e.event_name,
    e.app_id,
    e.platform,
    e.domain_userid,
    e.user_id,
    e.page_view_id,
    e.domain_sessionid,
    e.collector_tstamp,
    e.derived_tstamp,
    e.dvce_created_tstamp,
    e.load_tstamp,
    e.geo_country,
    e.page_url,
    e.page_title,
    e.useragent,

    {{snowplow_web.get_yauaa_context_fields()}},

    ceil(cast(cwv.cwv_lcp/1000 as decimal(14,4))*1000) /1000 as lcp,
    ceil(cast(cwv.cwv_fcp as decimal(14,4))*1000) /1000 as fcp,
    ceil(cast(cwv.cwv_fid as decimal(14,4))*1000) /1000 as fid,
    ceil(cast(cwv.cwv_cls as decimal(14,4))*1000) /1000 as cls,
    ceil(cast(cwv.cwv_inp as decimal(14,4))*1000) /1000 as inp,
    ceil(cast(cwv.cwv_ttfb as decimal(14,4))*1000) /1000 as ttfb,
    cast(cwv.cwv_navigation_type as {{ dbt.type_string() }}) as navigation_type

  from {{ ref("snowplow_web_base_events_this_run") }} as e

  inner join snowplow_web_cwv_context_stg cwv on e.event_id = cwv.cwv__id and e.collector_tstamp = cwv.cwv__tstamp

  where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.

  and event_name = 'web_vitals'

  and page_view_id is not null

  -- exclude bot traffic

  {% if var('snowplow__enable_iab', false) %}
    and not e.iab_spider_or_robot = True
  {% endif %}

  {{ filter_bots() }}

)

select
  event_id,
  event_name,
  app_id,
  platform,
  domain_userid,
  user_id,
  page_view_id,
  domain_sessionid,
  collector_tstamp,
  derived_tstamp,
  dvce_created_tstamp,
  load_tstamp,
  geo_country,
  page_url,
  page_title,
  useragent,
  lower(yauaa_device_class) as device_class,
  yauaa_agent_class as agent_class,
  yauaa_agent_name as agent_name,
  yauaa_agent_name_version as agent_name_version,
  yauaa_agent_name_version_major as agent_name_version_major,
  yauaa_agent_version as agent_version,
  yauaa_agent_version_major as agent_version_major,
  yauaa_device_brand as device_brand,
  yauaa_device_name as device_name,
  yauaa_device_version as device_version,
  yauaa_layout_engine_class as layout_engine_class,
  yauaa_layout_engine_name as layout_engine_name,
  yauaa_layout_engine_name_version as layout_engine_name_version,
  yauaa_layout_engine_name_version_major as layout_engine_name_version_major,
  yauaa_layout_engine_version as layout_engine_version,
  yauaa_layout_engine_version_major as layout_engine_version_major,
  yauaa_operating_system_class as operating_system_class,
  yauaa_operating_system_name as operating_system_name,
  yauaa_operating_system_name_version as operating_system_name_version,
  yauaa_operating_system_version as operating_system_version,
  lcp,
  fcp,
  fid,
  cls,
  inp,
  ttfb,
  navigation_type

from prep p
