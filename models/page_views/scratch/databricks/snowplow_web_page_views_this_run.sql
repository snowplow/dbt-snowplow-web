{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (
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

  -- optional fields, only populated if enabled.

  -- iab enrichment fields: set iab variable to true to enable
  {% if var('snowplow__enable_iab', false) %}

  ev.contexts_com_iab_snowplow_spiders_and_robots_1[0].category::STRING AS category,
  ev.contexts_com_iab_snowplow_spiders_and_robots_1[0].primary_impact::STRING AS primary_impact,
  ev.contexts_com_iab_snowplow_spiders_and_robots_1[0].reason::STRING AS reason,
  ev.contexts_com_iab_snowplow_spiders_and_robots_1[0].spider_or_robot::BOOLEAN AS spider_or_robot,

  {% else %}

  cast(null as {{ dbt_utils.type_string() }}) as category,
  cast(null as {{ dbt_utils.type_string() }}) as primary_impact,
  cast(null as {{ dbt_utils.type_string() }}) as reason,
  cast(null as boolean) as spider_or_robot,

  {% endif %}

  -- ua parser enrichment fields: set ua_parser variable to true to enable
  {% if var('snowplow__enable_ua', false) %}

  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].useragent_family::STRING AS useragent_family,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].useragent_major::STRING AS useragent_major,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].useragent_minor::STRING AS useragent_minor,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].useragent_patch::STRING AS useragent_patch,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].useragent_version::STRING AS useragent_version,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].os_family::STRING AS os_family,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].os_major::STRING AS os_major,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].os_minor::STRING AS os_minor,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].os_patch::STRING AS os_patch,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].os_patch_minor::STRING AS os_patch_minor,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].os_version::STRING AS os_version,
  ev.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0].device_family::STRING AS device_family,

  {% else %}

  cast(null as {{ dbt_utils.type_string() }}) as useragent_family,
  cast(null as {{ dbt_utils.type_string() }}) as useragent_major,
  cast(null as {{ dbt_utils.type_string() }}) as useragent_minor,
  cast(null as {{ dbt_utils.type_string() }}) as useragent_patch,
  cast(null as {{ dbt_utils.type_string() }}) as useragent_version,
  cast(null as {{ dbt_utils.type_string() }}) as os_family,
  cast(null as {{ dbt_utils.type_string() }}) as os_major,
  cast(null as {{ dbt_utils.type_string() }}) as os_minor,
  cast(null as {{ dbt_utils.type_string() }}) as os_patch,
  cast(null as {{ dbt_utils.type_string() }}) as os_patch_minor,
  cast(null as {{ dbt_utils.type_string() }}) as os_version,
  cast(null as {{ dbt_utils.type_string() }}) as device_family,

  {% endif %}

  -- yauaa enrichment fields: set yauaa variable to true to enable
  {% if var('snowplow__enable_yauaa', false) %}

  ev.contexts_nl_basjes_yauaa_context_1[0].device_class::STRING AS device_class,
  ev.contexts_nl_basjes_yauaa_context_1[0].agent_class::STRING AS agent_class,
  ev.contexts_nl_basjes_yauaa_context_1[0].agent_name::STRING AS agent_name,
  ev.contexts_nl_basjes_yauaa_context_1[0].agent_name_version::STRING AS agent_name_version,
  ev.contexts_nl_basjes_yauaa_context_1[0].agent_name_version_major::STRING AS agent_name_version_major,
  ev.contexts_nl_basjes_yauaa_context_1[0].agent_version::STRING AS agent_version,
  ev.contexts_nl_basjes_yauaa_context_1[0].agent_version_major::STRING AS agent_version_major,
  ev.contexts_nl_basjes_yauaa_context_1[0].device_brand::STRING AS device_brand,
  ev.contexts_nl_basjes_yauaa_context_1[0].device_name::STRING AS device_name,
  ev.contexts_nl_basjes_yauaa_context_1[0].device_version::STRING AS device_version,
  ev.contexts_nl_basjes_yauaa_context_1[0].layout_engine_class::STRING AS layout_engine_class,
  ev.contexts_nl_basjes_yauaa_context_1[0].layout_engine_name::STRING AS layout_engine_name,
  ev.contexts_nl_basjes_yauaa_context_1[0].layout_engine_name_version::STRING AS layout_engine_name_version,
  ev.contexts_nl_basjes_yauaa_context_1[0].layout_engine_name_version_major::STRING AS layout_engine_name_version_major,
  ev.contexts_nl_basjes_yauaa_context_1[0].layout_engine_version::STRING AS layout_engine_version,
  ev.contexts_nl_basjes_yauaa_context_1[0].layout_engine_version_major::STRING AS layout_engine_version_major,
  ev.contexts_nl_basjes_yauaa_context_1[0].operating_system_class::STRING AS operating_system_class,
  ev.contexts_nl_basjes_yauaa_context_1[0].operating_system_name::STRING AS operating_system_name,
  ev.contexts_nl_basjes_yauaa_context_1[0].operating_system_name_version::STRING AS operating_system_name_version,
  ev.contexts_nl_basjes_yauaa_context_1[0].operating_system_version::STRING AS operating_system_version

  {% else %}

  cast(null as {{ dbt_utils.type_string() }}) as device_class,
  cast(null as {{ dbt_utils.type_string() }}) as agent_class,
  cast(null as {{ dbt_utils.type_string() }}) as agent_name,
  cast(null as {{ dbt_utils.type_string() }}) as agent_name_version,
  cast(null as {{ dbt_utils.type_string() }}) as agent_name_version_major,
  cast(null as {{ dbt_utils.type_string() }}) as agent_version,
  cast(null as {{ dbt_utils.type_string() }}) as agent_version_major,
  cast(null as {{ dbt_utils.type_string() }}) as device_brand,
  cast(null as {{ dbt_utils.type_string() }}) as device_name,
  cast(null as {{ dbt_utils.type_string() }}) as device_version,
  cast(null as {{ dbt_utils.type_string() }}) as layout_engine_class,
  cast(null as {{ dbt_utils.type_string() }}) as layout_engine_name,
  cast(null as {{ dbt_utils.type_string() }}) as layout_engine_name_version,
  cast(null as {{ dbt_utils.type_string() }}) as layout_engine_name_version_major,
  cast(null as {{ dbt_utils.type_string() }}) as layout_engine_version,
  cast(null as {{ dbt_utils.type_string() }}) as layout_engine_version_major,
  cast(null as {{ dbt_utils.type_string() }}) as operating_system_class,
  cast(null as {{ dbt_utils.type_string() }}) as operating_system_name,
  cast(null as {{ dbt_utils.type_string() }}) as operating_system_name_version,
  cast(null as {{ dbt_utils.type_string() }}) as operating_system_version

  {% endif %}

  from {{ ref('snowplow_web_base_events_this_run') }} as ev

  where ev.event_name = 'page_view'
  and ev.page_view_id is not null

  {% if var("snowplow__ua_bot_filter", true) %}
     {{ filter_bots() }}
  {% endif %}

  qualify row_number() over (partition by ev.page_view_id order by ev.derived_tstamp) = 1
)

, page_view_events as (
  select
    p.page_view_id,
    p.event_id,

    p.app_id,

    -- user fields
    p.user_id,
    p.domain_userid,
    p.network_userid,

    -- session fields
    p.domain_sessionid,
    p.domain_sessionidx,

    row_number() over (partition by p.domain_sessionid order by p.derived_tstamp) AS page_view_in_session_index,

    -- timestamp fields
    p.dvce_created_tstamp,
    p.collector_tstamp,
    p.derived_tstamp,
    p.start_tstamp,
    coalesce(t.end_tstamp, p.derived_tstamp) as end_tstamp, -- only page views with pings will have a row in table t
    {{ dbt_utils.current_timestamp_in_utc() }} as model_tstamp,

    coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s, -- where there are no pings, engaged time is 0.
    datediff(second, p.derived_tstamp, coalesce(t.end_tstamp, p.derived_tstamp))  as absolute_time_in_s,

    sd.hmax as horizontal_pixels_scrolled,
    sd.vmax as vertical_pixels_scrolled,

    sd.relative_hmax as horizontal_percentage_scrolled,
    sd.relative_vmax as vertical_percentage_scrolled,

    p.doc_width,
    p.doc_height,

    p.page_title,
    p.page_url,
    p.page_urlscheme,
    p.page_urlhost,
    p.page_urlpath,
    p.page_urlquery,
    p.page_urlfragment,

    p.mkt_medium,
    p.mkt_source,
    p.mkt_term,
    p.mkt_content,
    p.mkt_campaign,
    p.mkt_clickid,
    p.mkt_network,

    p.page_referrer,
    p.refr_urlscheme,
    p.refr_urlhost,
    p.refr_urlpath,
    p.refr_urlquery,
    p.refr_urlfragment,
    p.refr_medium,
    p.refr_source,
    p.refr_term,

    p.geo_country,
    p.geo_region,
    p.geo_region_name,
    p.geo_city,
    p.geo_zipcode,
    p.geo_latitude,
    p.geo_longitude,
    p.geo_timezone,

    p.user_ipaddress,

    p.useragent,

    p.br_lang,
    p.br_viewwidth,
    p.br_viewheight,
    p.br_colordepth,
    p.br_renderengine,

    p.os_timezone,

    p.category,
    p.primary_impact,
    p.reason,
    p.spider_or_robot,

    p.useragent_family,
    p.useragent_major,
    p.useragent_minor,
    p.useragent_patch,
    p.useragent_version,
    p.os_family,
    p.os_major,
    p.os_minor,
    p.os_patch,
    p.os_patch_minor,
    p.os_version,
    p.device_family,

    p.device_class,
    p.agent_class,
    p.agent_name,
    p.agent_name_version,
    p.agent_name_version_major,
    p.agent_version,
    p.agent_version_major,
    p.device_brand,
    p.device_name,
    p.device_version,
    p.layout_engine_class,
    p.layout_engine_name,
    p.layout_engine_name_version,
    p.layout_engine_name_version_major,
    p.layout_engine_version,
    p.layout_engine_version_major,
    p.operating_system_class,
    p.operating_system_name,
    p.operating_system_name_version,
    p.operating_system_version

  from prep p

  left join {{ ref('snowplow_web_pv_engaged_time') }} t
  on p.page_view_id = t.page_view_id

  left join {{ ref('snowplow_web_pv_scroll_depth') }} sd
  on p.page_view_id = sd.page_view_id
)

select
  pve.page_view_id,
  pve.event_id,

  pve.app_id,

  -- user fields
  pve.user_id,
  pve.domain_userid,
  pve.network_userid,

  -- session fields
  pve.domain_sessionid,
  pve.domain_sessionidx,

  pve.page_view_in_session_index,
  max(pve.page_view_in_session_index) over (partition by pve.domain_sessionid) as page_views_in_session,

  -- timestamp fields
  pve.dvce_created_tstamp,
  pve.collector_tstamp,
  pve.derived_tstamp,
  pve.start_tstamp,
  pve.end_tstamp,
  pve.model_tstamp,

  pve.engaged_time_in_s,
  pve.absolute_time_in_s,

  pve.horizontal_pixels_scrolled,
  pve.vertical_pixels_scrolled,

  pve.horizontal_percentage_scrolled,
  pve.vertical_percentage_scrolled,

  pve.doc_width,
  pve.doc_height,

  pve.page_title,
  pve.page_url,
  pve.page_urlscheme,
  pve.page_urlhost,
  pve.page_urlpath,
  pve.page_urlquery,
  pve.page_urlfragment,

  pve.mkt_medium,
  pve.mkt_source,
  pve.mkt_term,
  pve.mkt_content,
  pve.mkt_campaign,
  pve.mkt_clickid,
  pve.mkt_network,

  pve.page_referrer,
  pve.refr_urlscheme,
  pve.refr_urlhost,
  pve.refr_urlpath,
  pve.refr_urlquery,
  pve.refr_urlfragment,
  pve.refr_medium,
  pve.refr_source,
  pve.refr_term,

  pve.geo_country,
  pve.geo_region,
  pve.geo_region_name,
  pve.geo_city,
  pve.geo_zipcode,
  pve.geo_latitude,
  pve.geo_longitude,
  pve.geo_timezone,

  pve.user_ipaddress,

  pve.useragent,

  pve.br_lang,
  pve.br_viewwidth,
  pve.br_viewheight,
  pve.br_colordepth,
  pve.br_renderengine,

  pve.os_timezone,

  pve.category,
  pve.primary_impact,
  pve.reason,
  pve.spider_or_robot,

  pve.useragent_family,
  pve.useragent_major,
  pve.useragent_minor,
  pve.useragent_patch,
  pve.useragent_version,
  pve.os_family,
  pve.os_major,
  pve.os_minor,
  pve.os_patch,
  pve.os_patch_minor,
  pve.os_version,
  pve.device_family,

  pve.device_class,
  pve.agent_class,
  pve.agent_name,
  pve.agent_name_version,
  pve.agent_name_version_major,
  pve.agent_version,
  pve.agent_version_major,
  pve.device_brand,
  pve.device_name,
  pve.device_version,
  pve.layout_engine_class,
  pve.layout_engine_name,
  pve.layout_engine_name_version,
  pve.layout_engine_name_version_major,
  pve.layout_engine_version,
  pve.layout_engine_version_major,
  pve.operating_system_class,
  pve.operating_system_name,
  pve.operating_system_name_version,
  pve.operating_system_version

from page_view_events pve
