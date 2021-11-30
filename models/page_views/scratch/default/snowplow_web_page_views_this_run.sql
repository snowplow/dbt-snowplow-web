{{ 
  config(
    materialized='table',
    sort='start_tstamp',
    dist='page_view_id',
    tags=["this_run"]
  ) 
}}


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
  {{ dbt_utils.datediff('ev.derived_tstamp', 'coalesce(t.end_tstamp, ev.derived_tstamp)', 'second') }} as absolute_time_in_s,

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

  -- optional fields, only populated if enabled.

  -- iab enrichment fields: set iab variable to true to enable
  {% if var('snowplow__enable_iab', false) %}
    iab.category,
    iab.primary_impact,
    iab.reason,
    iab.spider_or_robot,
  {% else %}
    cast(null as varchar) as category,
    cast(null as varchar) as primary_impact,
    cast(null as varchar) as reason,
    cast(null as boolean) as spider_or_robot,
  {% endif %}

  -- ua parser enrichment fields: set ua_parser variable to true to enable
  {% if var('snowplow__enable_ua', false) %}
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
    ua.device_family,
  {% else %}
    cast(null as varchar) as useragent_family,
    cast(null as varchar) as useragent_major,
    cast(null as varchar) as useragent_minor,
    cast(null as varchar) as useragent_patch,
    cast(null as varchar) as useragent_version,
    cast(null as varchar) as os_family,
    cast(null as varchar) as os_major,
    cast(null as varchar) as os_minor,
    cast(null as varchar) as os_patch,
    cast(null as varchar) as os_patch_minor,
    cast(null as varchar) as os_version,
    cast(null as varchar) as device_family,
  {% endif %}

  -- yauaa enrichment fields: set yauaa variable to true to enable
  {% if var('snowplow__enable_yauaa', false) %}
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
  {% else %}
    cast(null as varchar) as device_class,
    cast(null as varchar) as agent_class,
    cast(null as varchar) as agent_name,
    cast(null as varchar) as agent_name_version,
    cast(null as varchar) as agent_name_version_major,
    cast(null as varchar) as agent_version,
    cast(null as varchar) as agent_version_major,
    cast(null as varchar) as device_brand,
    cast(null as varchar) as device_name,
    cast(null as varchar) as device_version,
    cast(null as varchar) as layout_engine_class,
    cast(null as varchar) as layout_engine_name,
    cast(null as varchar) as layout_engine_name_version,
    cast(null as varchar) as layout_engine_name_version_major,
    cast(null as varchar) as layout_engine_version,
    cast(null as varchar) as layout_engine_version_major,
    cast(null as varchar) as operating_system_class,
    cast(null as varchar) as operating_system_name,
    cast(null as varchar) as operating_system_name_version,
    cast(null as varchar) as operating_system_version
  {% endif %}

from {{ ref('snowplow_web_page_view_events') }} ev

left join {{ ref('snowplow_web_pv_engaged_time') }} t
on ev.page_view_id = t.page_view_id

left join {{ ref('snowplow_web_pv_scroll_depth') }} sd
on ev.page_view_id = sd.page_view_id

{% if var('snowplow__enable_iab', false) -%}

  left join {{ ref('snowplow_web_pv_iab') }} iab
  on ev.page_view_id = iab.page_view_id

{% endif -%}

{% if var('snowplow__enable_ua', false) -%}

  left join {{ ref('snowplow_web_pv_ua_parser') }} ua
  on ev.page_view_id = ua.page_view_id

{% endif -%}

{% if var('snowplow__enable_yauaa', false) -%}

  left join {{ ref('snowplow_web_pv_yauaa') }} ya
  on ev.page_view_id = ya.page_view_id

{%- endif -%}
