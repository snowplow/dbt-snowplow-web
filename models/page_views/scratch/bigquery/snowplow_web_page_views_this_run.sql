{{
  config(
    tags=["this_run"]
  )
}}

with page_view_events as (
select
  ev.page_view_id,
  ev.event_id,

  ev.app_id,

  -- user fields
  ev.user_id,
  ev.domain_userid,
  ev.network_userid,

  -- session fields
  ev.domain_sessionid,
  ev.domain_sessionidx,

  -- timestamp fields
  ev.dvce_created_tstamp,
  ev.collector_tstamp,
  ev.derived_tstamp,
  ev.derived_tstamp as start_tstamp,

  ev.doc_width,
  ev.doc_height,

  ev.page_title,
  ev.page_url,
  ev.page_urlscheme,
  ev.page_urlhost,
  ev.page_urlpath,
  ev.page_urlquery,
  ev.page_urlfragment,

  ev.mkt_medium,
  ev.mkt_source,
  ev.mkt_term,
  ev.mkt_content,
  ev.mkt_campaign,
  ev.mkt_clickid,
  ev.mkt_network,

  ev.page_referrer,
  ev.refr_urlscheme,
  ev.refr_urlhost,
  ev.refr_urlpath,
  ev.refr_urlquery,
  ev.refr_urlfragment,
  ev.refr_medium,
  ev.refr_source,
  ev.refr_term,

  ev.geo_country,
  ev.geo_region,
  ev.geo_region_name,
  ev.geo_city,
  ev.geo_zipcode,
  ev.geo_latitude,
  ev.geo_longitude,
  ev.geo_timezone ,

  ev.user_ipaddress,

  ev.useragent,

  ev.br_lang,
  ev.br_viewwidth,
  ev.br_viewheight,
  ev.br_colordepth,
  ev.br_renderengine,
  ev.os_timezone,

  row_number() over (partition by ev.domain_sessionid order by ev.derived_tstamp) AS page_view_in_session_index,

  -- optional fields, only populated if enabled.

  -- iab enrichment fields: set iab variable to true to enable
  {{ snowplow_utils.get_optional_fields(
        enabled=var('snowplow__enable_iab', false),
        fields=iab_fields(),
        col_prefix='contexts_com_iab_snowplow_spiders_and_robots_1',
        relation=ref('snowplow_web_base_events_this_run'),
        relation_alias='ev') }},

  -- ua parser enrichment fields: set ua_parser variable to true to enable
  {{ snowplow_utils.get_optional_fields(
        enabled=var('snowplow__enable_ua', false),
        fields=ua_fields(),
        col_prefix='contexts_com_snowplowanalytics_snowplow_ua_parser_context_1',
        relation=ref('snowplow_web_base_events_this_run'),
        relation_alias='ev') }},

  -- yauaa enrichment fields: set yauaa variable to true to enable
  {{ snowplow_utils.get_optional_fields(
        enabled=var('snowplow__enable_yauaa', false),
        fields=yauaa_fields(),
        col_prefix='contexts_nl_basjes_yauaa_context_1',
        relation=ref('snowplow_web_base_events_this_run'),
        relation_alias='ev') }}

from (
  select
    array_agg(e order by e.derived_tstamp limit 1)[offset(0)] as ev
    -- order by matters here since derived_tstamp determines parts of model logic

  from {{ ref('snowplow_web_base_events_this_run') }} as e
  where e.event_name = 'page_view'
  and e.page_view_id is not null

  group by e.page_view_id
)
where 1 = 1

{% if var("snowplow__ua_bot_filter", true) %}
 {{ filter_bots() }}
{% endif %}
)


select
  ev.page_view_id,
  ev.event_id,

  ev.app_id,

  -- user fields
  ev.user_id,
  ev.domain_userid,
  ev.network_userid,

  -- session fields
  ev.domain_sessionid,
  ev.domain_sessionidx,

  ev.page_view_in_session_index,
  max(ev.page_view_in_session_index) over (partition by ev.domain_sessionid) as page_views_in_session,

  -- timestamp fields
  ev.dvce_created_tstamp,
  ev.collector_tstamp,
  ev.derived_tstamp,
  ev.start_tstamp,
  coalesce(t.end_tstamp, ev.derived_tstamp) as end_tstamp, -- only page views with pings will have a row in table t
  {{ dbt_utils.current_timestamp_in_utc() }} as model_tstamp,

  coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s, -- where there are no pings, engaged time is 0.
  timestamp_diff(coalesce(t.end_tstamp, ev.derived_tstamp), ev.derived_tstamp, second)  as absolute_time_in_s,

  sd.hmax as horizontal_pixels_scrolled,
  sd.vmax as vertical_pixels_scrolled,

  sd.relative_hmax as horizontal_percentage_scrolled,
  sd.relative_vmax as vertical_percentage_scrolled,

  ev.doc_width,
  ev.doc_height,

  ev.page_title,
  ev.page_url,
  ev.page_urlscheme,
  ev.page_urlhost,
  ev.page_urlpath,
  ev.page_urlquery,
  ev.page_urlfragment,

  ev.mkt_medium,
  ev.mkt_source,
  ev.mkt_term,
  ev.mkt_content,
  ev.mkt_campaign,
  ev.mkt_clickid,
  ev.mkt_network,

  ev.page_referrer,
  ev.refr_urlscheme,
  ev.refr_urlhost,
  ev.refr_urlpath,
  ev.refr_urlquery,
  ev.refr_urlfragment,
  ev.refr_medium,
  ev.refr_source,
  ev.refr_term,

  ev.geo_country,
  ev.geo_region,
  ev.geo_region_name,
  ev.geo_city,
  ev.geo_zipcode,
  ev.geo_latitude,
  ev.geo_longitude,
  ev.geo_timezone,

  ev.user_ipaddress,

  ev.useragent,

  ev.br_lang,
  ev.br_viewwidth,
  ev.br_viewheight,
  ev.br_colordepth,
  ev.br_renderengine,

  ev.os_timezone,

  ev.category,
  ev.primary_impact,
  ev.reason,
  ev.spider_or_robot,

  ev.useragent_family,
  ev.useragent_major,
  ev.useragent_minor,
  ev.useragent_patch,
  ev.useragent_version,
  ev.os_family,
  ev.os_major,
  ev.os_minor,
  ev.os_patch,
  ev.os_patch_minor,
  ev.os_version,
  ev.device_family,

  ev.device_class,
  ev.agent_class,
  ev.agent_name,
  ev.agent_name_version,
  ev.agent_name_version_major,
  ev.agent_version,
  ev.agent_version_major,
  ev.device_brand,
  ev.device_name,
  ev.device_version,
  ev.layout_engine_class,
  ev.layout_engine_name,
  ev.layout_engine_name_version,
  ev.layout_engine_name_version_major,
  ev.layout_engine_version,
  ev.layout_engine_version_major,
  ev.operating_system_class,
  ev.operating_system_name,
  ev.operating_system_name_version,
  ev.operating_system_version

from page_view_events ev

left join {{ ref('snowplow_web_pv_engaged_time') }} t
on ev.page_view_id = t.page_view_id

left join {{ ref('snowplow_web_pv_scroll_depth') }} sd
on ev.page_view_id = sd.page_view_id
